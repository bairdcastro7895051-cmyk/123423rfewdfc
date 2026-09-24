package hoptool

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/tidwall/gjson"

	hoplitert "kiro-proxy/internal/runtime/hoplite"
)

// AttachmentStore keeps the inline attachments (images/files the client pasted)
// of live bridge sessions, keyed by bridgeKey. They exist only in memory: the
// virtual .hoplite-attachments/ paths handed to the model are never on disk, so
// a local_fs_read of one must be answered here instead of being forwarded to
// Claude Code.
type AttachmentStore struct {
	mu    sync.RWMutex
	items map[string][]hoplitert.Attachment
}

func NewAttachmentStore() *AttachmentStore {
	return &AttachmentStore{items: map[string][]hoplitert.Attachment{}}
}

// Put registers this turn's attachments for a bridge session (no-op when empty).
func (s *AttachmentStore) Put(bridgeKey string, atts []hoplitert.Attachment) {
	if s == nil || bridgeKey == "" || len(atts) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.items == nil {
		s.items = map[string][]hoplitert.Attachment{}
	}
	seen := map[string]bool{}
	for _, a := range s.items[bridgeKey] {
		seen[a.ID] = true
	}
	for _, a := range atts {
		if !seen[a.ID] {
			seen[a.ID] = true
			s.items[bridgeKey] = append(s.items[bridgeKey], a)
		}
	}
}

// Drop releases a finished session's attachments.
func (s *AttachmentStore) Drop(bridgeKey string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	delete(s.items, bridgeKey)
	s.mu.Unlock()
}

// List returns the session's attachments.
func (s *AttachmentStore) List(bridgeKey string) []hoplitert.Attachment {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]hoplitert.Attachment(nil), s.items[bridgeKey]...)
}

// Find resolves a virtual path (with or without the directory prefix) to an
// attachment of that session.
func (s *AttachmentStore) Find(bridgeKey, file string) (hoplitert.Attachment, bool) {
	f := normalizeVirtualPath(file)
	bare := strings.TrimPrefix(f, hoplitert.AttachmentDir+"/")
	for _, a := range s.List(bridgeKey) {
		if f == a.Path || bare == strings.TrimPrefix(a.Path, hoplitert.AttachmentDir+"/") {
			return a, true
		}
	}
	return hoplitert.Attachment{}, false
}

// IsAttachmentPath reports whether an MCP path argument targets the virtual
// attachment directory.
func IsAttachmentPath(p string) bool {
	return strings.HasPrefix(normalizeVirtualPath(p), hoplitert.AttachmentDir)
}

func normalizeVirtualPath(p string) string {
	v := strings.TrimSpace(p)
	v = strings.ReplaceAll(v, "\\", "/")
	v = strings.TrimPrefix(v, "./")
	return strings.TrimPrefix(v, "/")
}

// Serve answers a local_fs_read / local_fs_list that targets the virtual
// attachment directory, returning the MCP result JSON. handled=false means the
// call is a normal filesystem call and must go to Claude Code as usual.
func (s *AttachmentStore) Serve(bridgeKey, tool string, args json.RawMessage) (json.RawMessage, bool) {
	switch tool {
	case ToolLocalFsRead:
		file := gjson.GetBytes(args, "file").String()
		if !IsAttachmentPath(file) {
			return nil, false
		}
		att, ok := s.Find(bridgeKey, file)
		if !ok {
			out, _ := FromClaudeToolResult("attachment not found: "+file, true)
			return out, true
		}
		if att.IsImage() {
			out, _ := FromClaudeToolResultWithImages("", []ImageContent{{
				MediaType: att.MediaType, Base64Data: att.Base64(),
			}}, false)
			return out, true
		}
		out, _ := FromClaudeToolResult(string(att.Data), false)
		return out, true

	case ToolLocalFsList:
		dir := gjson.GetBytes(args, "dir").String()
		if !IsAttachmentPath(dir) {
			return nil, false
		}
		var b strings.Builder
		for _, a := range s.List(bridgeKey) {
			fmt.Fprintf(&b, "%s\t%s\t%d bytes\n", a.Path, a.MediaType, len(a.Data))
		}
		out, _ := FromClaudeToolResult(b.String(), false)
		return out, true
	}
	return nil, false
}
