// Package hoptool holds the pure-function translation layer of the "Hoplite
// bridge": it converts the MCP tool calls that a Hoplite cloud agent sends into
// the Anthropic-protocol tool_use blocks that a local Claude Code client can
// execute, and converts Claude Code's tool_result content back into MCP tool
// results (text *and* images).
//
// Everything here is a pure function of its inputs (the one exception is the
// in-memory AttachmentStore in attachments.go, which is plain state, no I/O).
// No networking, no filesystem.
//
// The three MCP tools bridged today (from live capture):
//
//	local_fs_list  arguments: {"dir":  "."}
//	local_fs_read  arguments: {"file": "README.md"}
//	local_fs_write arguments: {"file": "x.go", "content": "..."}
package hoptool

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/tidwall/gjson"
)

// MCP tool names sent by Hoplite.
const (
	ToolLocalFsList  = "local_fs_list"
	ToolLocalFsRead  = "local_fs_read"
	ToolLocalFsWrite = "local_fs_write"
)

// Claude Code (Anthropic protocol) built-in tool names we translate into.
const (
	ClaudeToolRead  = "Read"
	ClaudeToolWrite = "Write"
	ClaudeToolBash  = "Bash"
)

// Sentinel errors so callers can branch with errors.Is.
var (
	// ErrUnknownTool is returned when the MCP tool name has no mapping.
	ErrUnknownTool = errors.New("hoptool: unknown MCP tool")
	// ErrMissingArgument is returned when a required MCP argument is absent/empty.
	ErrMissingArgument = errors.New("hoptool: missing required argument")
	// ErrInvalidRequest is returned when the JSON-RPC body or arguments are malformed.
	ErrInvalidRequest = errors.New("hoptool: invalid JSON-RPC request")
)

// McpToolCall is a tool invocation parsed out of an MCP tools/call request.
type McpToolCall struct {
	// Name is params.name, e.g. "local_fs_write".
	Name string
	// Arguments is the raw params.arguments object (may be nil if absent).
	Arguments json.RawMessage
}

// ClaudeToolUse is a tool_use to hand to Claude Code for execution: the tool
// name plus its input object encoded as JSON.
type ClaudeToolUse struct {
	Name  string
	Input json.RawMessage
}

// ImageContent is one image to hand back to the Hoplite model as an MCP
// ImageContent block. Base64Data carries the raw base64 payload without any
// "data:" URL prefix, matching the MCP wire shape.
type ImageContent struct {
	MediaType  string
	Base64Data string
}

// --- input/output shapes (internal, kept small & explicit for deterministic
//     field ordering in the emitted JSON) ---

type readInput struct {
	FilePath string `json:"file_path"`
}

type writeInput struct {
	FilePath string `json:"file_path"`
	Content  string `json:"content"`
}

type bashInput struct {
	Command string `json:"command"`
}

type mcpTextContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type mcpImageContent struct {
	Type     string `json:"type"`     // always "image"
	Data     string `json:"data"`     // base64, no data: prefix
	MimeType string `json:"mimeType"` // image/png, image/jpeg, ...
}

// mcpToolResult carries mixed content blocks: MCP allows text and image blocks
// in one result, which is what lets a PNG reach the model at all.
type mcpToolResult struct {
	Content []any `json:"content"`
	IsError bool  `json:"isError"`
}

// ParseMcpToolCall extracts a McpToolCall (params.name + params.arguments) from
// a full JSON-RPC tools/call request body. It validates that the body is JSON
// and that method == "tools/call" with a non-empty params.name.
func ParseMcpToolCall(body []byte) (McpToolCall, error) {
	if !gjson.ValidBytes(body) {
		return McpToolCall{}, fmt.Errorf("%w: body is not valid JSON", ErrInvalidRequest)
	}
	if method := gjson.GetBytes(body, "method").String(); method != "tools/call" {
		return McpToolCall{}, fmt.Errorf("%w: method %q is not %q", ErrInvalidRequest, method, "tools/call")
	}
	name := gjson.GetBytes(body, "params.name").String()
	if name == "" {
		return McpToolCall{}, fmt.Errorf("%w: missing params.name", ErrInvalidRequest)
	}
	var args json.RawMessage
	if r := gjson.GetBytes(body, "params.arguments"); r.Exists() {
		// r.Raw is the exact JSON substring of the arguments value.
		args = json.RawMessage(r.Raw)
	}
	return McpToolCall{Name: name, Arguments: args}, nil
}

// FromClaudeToolResult wraps Claude Code's tool_result text into the MCP tool
// result shape: {"content":[{"type":"text","text":<resultText>}],"isError":<isError>}.
func FromClaudeToolResult(resultText string, isError bool) (json.RawMessage, error) {
	return FromClaudeToolResultWithImages(resultText, nil, isError)
}

// FromClaudeToolResultWithImages is FromClaudeToolResult plus MCP image blocks.
// Images with an empty payload are skipped; an empty media type falls back to
// image/png (Claude Code's Read returns PNG most often and MCP requires the
// field). When there is no text and no image the result still carries one empty
// text block, because MCP requires a non-empty content array.
func FromClaudeToolResultWithImages(resultText string, images []ImageContent, isError bool) (json.RawMessage, error) {
	res := mcpToolResult{IsError: isError}
	if resultText != "" || len(images) == 0 {
		res.Content = append(res.Content, mcpTextContent{Type: "text", Text: resultText})
	}
	for _, img := range images {
		data := strings.TrimSpace(img.Base64Data)
		if data == "" {
			continue
		}
		mt := strings.TrimSpace(img.MediaType)
		if mt == "" {
			mt = "image/png"
		}
		res.Content = append(res.Content, mcpImageContent{Type: "image", Data: data, MimeType: mt})
	}
	if len(res.Content) == 0 {
		res.Content = append(res.Content, mcpTextContent{Type: "text", Text: resultText})
	}
	raw, err := marshalNoHTMLEscape(res)
	if err != nil {
		return nil, fmt.Errorf("hoptool: marshal tool result: %w", err)
	}
	return raw, nil
}

// --- helpers ---

// marshalNoHTMLEscape marshals v to JSON without Go's default HTML escaping, so
// that <, > and & in file content or result text survive verbatim (still valid
// JSON, just more faithful for code/HTML payloads).
func marshalNoHTMLEscape(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	// Encoder.Encode appends a trailing newline; drop it.
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

// shellQuoteSingle wraps s in POSIX single quotes, escaping embedded single
// quotes with the close-escape-reopen idiom. This keeps a directory argument from breaking out
// of the `ls -la <dir>` command (command-injection safe).
func shellQuoteSingle(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// isLetter reports whether b is an ASCII letter.
func isLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

// isAbsPath reports whether p looks absolute on either unix or Windows, so an
// already-absolute path is never re-prefixed with workspaceRoot.
func isAbsPath(p string) bool {
	if p == "" {
		return false
	}
	if p[0] == '/' || p[0] == '\\' { // unix absolute, or UNC/rooted "\..."
		return true
	}
	// Windows drive-absolute: "C:\..." or "C:/...".
	if len(p) >= 3 && isLetter(p[0]) && p[1] == ':' && (p[2] == '\\' || p[2] == '/') {
		return true
	}
	return false
}

// resolveFilePath applies the optional workspaceRoot prefix to a path taken
// from an MCP argument.
//
// LIVE CALIBRATION POINT: the default is passthrough — when workspaceRoot is ""
// the (relative) path is handed to Claude Code unchanged and Claude Code
// resolves it against its own cwd.
func resolveFilePath(workspaceRoot, p string) string {
	if workspaceRoot == "" || p == "" {
		return p
	}
	if isAbsPath(p) {
		return p
	}
	return path.Join(workspaceRoot, p)
}

// ToClaudeToolUse translates one local_fs_* MCP call into a Claude Code
// tool_use (name + input JSON). An unknown tool name returns ErrUnknownTool; a
// missing required argument returns ErrMissingArgument; malformed arguments
// JSON returns ErrInvalidRequest.
//
// Mapping:
//
//	local_fs_read {file}          -> Read  {file_path}
//	local_fs_write {file,content} -> Write {file_path, content}
//	local_fs_list {dir}           -> Bash  {command: "ls -la <dir>"}
//
// list maps to Bash rather than the built-in LS tool on purpose: LS requires an
// absolute path, but the default (passthrough) mode ships relative paths, so LS
// would reject the common case.
func ToClaudeToolUse(call McpToolCall, workspaceRoot string) (ClaudeToolUse, error) {
	// Validate arguments JSON up front (empty/nil is allowed here; per-tool
	// checks below decide whether a required field is missing).
	if len(call.Arguments) > 0 && !gjson.ValidBytes(call.Arguments) {
		return ClaudeToolUse{}, fmt.Errorf("%w: params.arguments for %q is not valid JSON", ErrInvalidRequest, call.Name)
	}

	switch call.Name {
	case ToolLocalFsRead:
		file := gjson.GetBytes(call.Arguments, "file").String()
		if file == "" {
			return ClaudeToolUse{}, fmt.Errorf("%w: %q needs \"file\"", ErrMissingArgument, call.Name)
		}
		input, err := marshalNoHTMLEscape(readInput{FilePath: resolveFilePath(workspaceRoot, file)})
		if err != nil {
			return ClaudeToolUse{}, fmt.Errorf("hoptool: marshal %s input: %w", ClaudeToolRead, err)
		}
		return ClaudeToolUse{Name: ClaudeToolRead, Input: input}, nil

	case ToolLocalFsWrite:
		file := gjson.GetBytes(call.Arguments, "file").String()
		if file == "" {
			return ClaudeToolUse{}, fmt.Errorf("%w: %q needs \"file\"", ErrMissingArgument, call.Name)
		}
		// content may legitimately be empty (writing an empty file).
		content := gjson.GetBytes(call.Arguments, "content").String()
		input, err := marshalNoHTMLEscape(writeInput{
			FilePath: resolveFilePath(workspaceRoot, file),
			Content:  content,
		})
		if err != nil {
			return ClaudeToolUse{}, fmt.Errorf("hoptool: marshal %s input: %w", ClaudeToolWrite, err)
		}
		return ClaudeToolUse{Name: ClaudeToolWrite, Input: input}, nil

	case ToolLocalFsList:
		// dir defaults to "." when absent/empty — a bare listing of cwd.
		dir := gjson.GetBytes(call.Arguments, "dir").String()
		if dir == "" {
			dir = "."
		}
		dir = resolveFilePath(workspaceRoot, dir)
		cmd := "ls -la " + shellQuoteSingle(dir)
		input, err := marshalNoHTMLEscape(bashInput{Command: cmd})
		if err != nil {
			return ClaudeToolUse{}, fmt.Errorf("hoptool: marshal %s input: %w", ClaudeToolBash, err)
		}
		return ClaudeToolUse{Name: ClaudeToolBash, Input: input}, nil

	default:
		return ClaudeToolUse{}, fmt.Errorf("%w: %q", ErrUnknownTool, call.Name)
	}
}
