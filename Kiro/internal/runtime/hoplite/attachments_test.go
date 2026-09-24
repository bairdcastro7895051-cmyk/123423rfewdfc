package hoplite

import (
	"strings"
	"testing"
)

// 1x1 PNG.
const pngB64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg=="

func TestFlattenAnthropic_ImageBecomesAttachmentPath(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[
		{"type":"text","text":"这个图片里面有什么"},
		{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + pngB64 + `"}}]}]}`)
	got := FlattenAnthropic(body)
	if strings.Contains(got, "unwired") {
		t.Fatalf("prompt still carries unwired placeholder: %q", got)
	}
	if !strings.Contains(got, AttachmentDir+"/") || !strings.Contains(got, ".png") {
		t.Fatalf("prompt lacks attachment path: %q", got)
	}
	if !strings.Contains(got, "local_fs_read") {
		t.Fatalf("prompt lacks read instruction: %q", got)
	}
}

func TestFlattenOpenAI_DataURLImage(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[
		{"type":"image_url","image_url":{"url":"data:image/png;base64,` + pngB64 + `"}}]}]}`)
	got := FlattenOpenAI(body)
	if !strings.Contains(got, AttachmentDir+"/") {
		t.Fatalf("openai data-url image not wired: %q", got)
	}
}

func TestFlattenOpenAI_RemoteURLKeepsURL(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[
		{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}]}]}`)
	got := FlattenOpenAI(body)
	if !strings.Contains(got, "https://example.com/a.png") {
		t.Fatalf("remote url lost: %q", got)
	}
}

// 命门：prompt 层与中继层是两次独立扫描，路径必须一致。
func TestExtractAttachments_SamePathAsPrompt(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[
		{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + pngB64 + `"}}]}]}`)
	atts := ExtractAttachments(body)
	if len(atts) != 1 {
		t.Fatalf("want 1 attachment, got %d", len(atts))
	}
	if !strings.Contains(FlattenAnthropic(body), atts[0].Path) {
		t.Fatalf("prompt path != extracted path %q", atts[0].Path)
	}
	if !atts[0].IsImage() || atts[0].MediaType != "image/png" {
		t.Fatalf("bad media type: %v", atts[0].MediaType)
	}
	if len(atts[0].Data) == 0 {
		t.Fatal("attachment data empty")
	}
}

func TestExtractAttachments_DedupAndSkipText(t *testing.T) {
	body := []byte(`{"messages":[
		{"role":"user","content":[{"type":"text","text":"hi"},
			{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + pngB64 + `"}}]},
		{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + pngB64 + `"}}]}]}`)
	if n := len(ExtractAttachments(body)); n != 1 {
		t.Fatalf("want dedup to 1, got %d", n)
	}
}

func TestFlatten_TextOnlyUnchanged(t *testing.T) {
	body := []byte(`{"system":"be brief","messages":[{"role":"user","content":"ping"}]}`)
	if got := FlattenAnthropic(body); got != "system: be brief\n\nuser: ping" {
		t.Fatalf("text path changed: %q", got)
	}
}
