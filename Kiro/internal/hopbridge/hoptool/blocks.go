package hoptool

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

// resultEnvelopePrefix marks a rendezvous ToolResult.Content string that is not
// plain text but a JSON envelope carrying text + image blocks. The rendezvous
// channel between the parked provider and the MCP relay only carries a string,
// and widening that struct would touch the concurrency core; smuggling a tagged
// envelope through it keeps the change inside this package. The NUL bytes make
// the tag impossible to collide with real tool output.
const resultEnvelopePrefix = "\x00hopbridge/blocks-v1\x00"

type resultEnvelope struct {
	Text   string         `json:"text"`
	Images []ImageContent `json:"images,omitempty"`
}

// EncodeToolResult packs text + images into a single transport string. With no
// images it returns the text unchanged, so the common path stays plain text.
func EncodeToolResult(text string, images []ImageContent) string {
	if len(images) == 0 {
		return text
	}
	raw, err := json.Marshal(resultEnvelope{Text: text, Images: images})
	if err != nil {
		return text
	}
	return resultEnvelopePrefix + string(raw)
}

// DecodeToolResult is the inverse of EncodeToolResult. A string that is not an
// envelope is returned as plain text with no images.
func DecodeToolResult(s string) (string, []ImageContent) {
	if !strings.HasPrefix(s, resultEnvelopePrefix) {
		return s, nil
	}
	var env resultEnvelope
	if err := json.Unmarshal([]byte(strings.TrimPrefix(s, resultEnvelopePrefix)), &env); err != nil {
		return s, nil
	}
	return env.Text, env.Images
}

// ParseClaudeToolResultContent flattens an Anthropic tool_result `content` value
// into text plus image blocks.
//
// Claude Code answers a Read of a PNG with an image block
// ({"type":"image","source":{"type":"base64","media_type":...,"data":...}}) and
// no text field at all — dropping non-text blocks here is what made images
// silently vanish. Both the Anthropic `source` shape and the MCP-native
// {"data","mimeType"} shape are accepted.
func ParseClaudeToolResultContent(v gjson.Result) (string, []ImageContent) {
	if v.Type == gjson.String {
		return v.String(), nil
	}
	if !v.IsArray() {
		if img, ok := imageBlock(v); ok {
			return "", []ImageContent{img}
		}
		return v.String(), nil
	}
	var text []string
	var images []ImageContent
	v.ForEach(func(_, item gjson.Result) bool {
		if item.Type == gjson.String {
			if s := item.String(); s != "" {
				text = append(text, s)
			}
			return true
		}
		if t := item.Get("text").String(); t != "" {
			text = append(text, t)
			return true
		}
		if img, ok := imageBlock(item); ok {
			images = append(images, img)
		}
		return true
	})
	return strings.Join(text, "\n"), images
}

// imageBlock recognises one image block in either Anthropic or MCP shape.
func imageBlock(item gjson.Result) (ImageContent, bool) {
	if !item.IsObject() {
		return ImageContent{}, false
	}
	typ := strings.ToLower(strings.TrimSpace(item.Get("type").String()))
	// Anthropic: {"type":"image","source":{"type":"base64","media_type":..,"data":..}}
	if src := item.Get("source"); src.Exists() {
		if data := strings.TrimSpace(src.Get("data").String()); data != "" {
			return ImageContent{MediaType: mediaTypeOf(src.Get("media_type").String(), typ), Base64Data: data}, true
		}
		if u := strings.TrimSpace(src.Get("url").String()); strings.HasPrefix(u, "data:") {
			if mt, data, ok := splitDataURL(u); ok {
				return ImageContent{MediaType: mt, Base64Data: data}, true
			}
		}
	}
	// MCP native: {"type":"image","data":"...","mimeType":"image/png"}
	if data := strings.TrimSpace(item.Get("data").String()); data != "" && typ == "image" {
		mt := item.Get("mimeType").String()
		if mt == "" {
			mt = item.Get("media_type").String()
		}
		return ImageContent{MediaType: mediaTypeOf(mt, typ), Base64Data: data}, true
	}
	return ImageContent{}, false
}

func mediaTypeOf(mt, typ string) string {
	mt = strings.ToLower(strings.TrimSpace(mt))
	if mt != "" {
		return mt
	}
	if typ == "image" {
		return "image/png"
	}
	return "application/octet-stream"
}

func splitDataURL(u string) (mediaType, data string, ok bool) {
	rest := strings.TrimPrefix(u, "data:")
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return "", "", false
	}
	meta := rest[:comma]
	if !strings.Contains(meta, "base64") {
		return "", "", false
	}
	return strings.TrimSpace(strings.Split(meta, ";")[0]), strings.TrimSpace(rest[comma+1:]), true
}
