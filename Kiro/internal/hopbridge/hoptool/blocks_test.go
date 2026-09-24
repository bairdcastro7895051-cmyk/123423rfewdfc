package hoptool

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestParseClaudeToolResultContent_KeepsImage(t *testing.T) {
	v := gjson.Parse(`[{"type":"text","text":"read ok"},
		{"type":"image","source":{"type":"base64","media_type":"image/png","data":"QUJD"}}]`)
	text, imgs := ParseClaudeToolResultContent(v)
	if text != "read ok" {
		t.Fatalf("text=%q", text)
	}
	if len(imgs) != 1 || imgs[0].Base64Data != "QUJD" || imgs[0].MediaType != "image/png" {
		t.Fatalf("images=%+v", imgs)
	}
}

func TestParseClaudeToolResultContent_Shapes(t *testing.T) {
	if text, imgs := ParseClaudeToolResultContent(gjson.Parse(`"plain"`)); text != "plain" || imgs != nil {
		t.Fatalf("plain string broke: %q %+v", text, imgs)
	}
	v := gjson.Parse(`[{"type":"image","data":"QUJD","mimeType":"image/jpeg"}]`)
	text, imgs := ParseClaudeToolResultContent(v)
	if text != "" || len(imgs) != 1 || imgs[0].MediaType != "image/jpeg" {
		t.Fatalf("mcp-shaped image broke: %q %+v", text, imgs)
	}
}

func TestEncodeDecodeToolResult_RoundTrip(t *testing.T) {
	if got := EncodeToolResult("just text", nil); got != "just text" {
		t.Fatalf("no-image path must stay plain: %q", got)
	}
	enc := EncodeToolResult("t", []ImageContent{{MediaType: "image/png", Base64Data: "QUJD"}})
	text, imgs := DecodeToolResult(enc)
	if text != "t" || len(imgs) != 1 || imgs[0].Base64Data != "QUJD" {
		t.Fatalf("round trip lost data: %q %+v", text, imgs)
	}
	if text, imgs := DecodeToolResult("ordinary output"); text != "ordinary output" || imgs != nil {
		t.Fatalf("plain decode broke: %q %+v", text, imgs)
	}
}

func TestFromClaudeToolResultWithImages(t *testing.T) {
	raw, err := FromClaudeToolResultWithImages("", []ImageContent{{MediaType: "image/png", Base64Data: "QUJD"}}, false)
	if err != nil {
		t.Fatal(err)
	}
	s := string(raw)
	if !strings.Contains(s, `"type":"image"`) || !strings.Contains(s, `"mimeType":"image/png"`) || !strings.Contains(s, `"data":"QUJD"`) {
		t.Fatalf("mcp image block missing: %s", s)
	}
	if strings.Contains(s, `"type":"text"`) {
		t.Fatalf("image-only result should not pad a text block: %s", s)
	}
	plain, _ := FromClaudeToolResult("hello", false)
	if !strings.Contains(string(plain), `"content":[{"type":"text","text":"hello"}]`) {
		t.Fatalf("text-only shape changed: %s", plain)
	}
}
