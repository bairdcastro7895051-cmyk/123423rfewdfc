package hoplite

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"strings"

	"github.com/tidwall/gjson"
)

// AttachmentDir 是附件的**虚拟**目录：磁盘上不存在。桥中继在 local_fs_read /
// local_fs_list 命中该前缀时直接从内存作答，不下发给 Claude Code。
const AttachmentDir = ".hoplite-attachments"

// Attachment 是从客户端请求体里摘出的一张图 / 一个文件。
// ID 取内容 sha256 前 8 字节 hex：确定性 → prompt 层与中继层两次独立扫描得到同一路径，
// 不必跨层传参（RunPrompt / ThreadOpts 等签名不动）。
type Attachment struct {
	ID        string
	MediaType string
	Path      string // .hoplite-attachments/<id>.<ext>
	Data      []byte
}

// NewAttachment 按内容算出确定性 ID 与虚拟路径。
func NewAttachment(mediaType string, data []byte) Attachment {
	sum := sha256.Sum256(data)
	id := hex.EncodeToString(sum[:8])
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	if i := strings.IndexByte(mt, ';'); i >= 0 { // "image/png; charset=binary"
		mt = strings.TrimSpace(mt[:i])
	}
	if mt == "" {
		mt = "application/octet-stream"
	}
	return Attachment{ID: id, MediaType: mt, Data: data, Path: AttachmentDir + "/" + id + extForMedia(mt)}
}

func extForMedia(mt string) string {
	switch mt {
	case "image/png":
		return ".png"
	case "image/jpeg", "image/jpg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "application/pdf":
		return ".pdf"
	case "text/plain", "text/markdown":
		return ".txt"
	case "application/json":
		return ".json"
	}
	return ".bin"
}

// IsImage 判定该附件能否以 MCP image content 回给模型。
func (a Attachment) IsImage() bool { return strings.HasPrefix(a.MediaType, "image/") }

// Base64 是附件内容的标准 base64（回 MCP image content 用）。
func (a Attachment) Base64() string { return base64.StdEncoding.EncodeToString(a.Data) }

// DecodeInlineAttachment 认三条协议各自的内联附件形状：
//
//	Anthropic {"type":"image","source":{"type":"base64","media_type":"image/png","data":"..."}}
//	          {"type":"document","source":{...}}
//	OpenAI    {"type":"image_url","image_url":{"url":"data:image/png;base64,..."}}
//	Responses {"type":"input_image","image_url":"data:image/png;base64,..."}
//
// ok=false 表示这不是可内联的附件（例如 http(s) 远程 URL —— 那种直接把 URL 写进 prompt）。
func DecodeInlineAttachment(item gjson.Result) (Attachment, bool) {
	if !item.IsObject() {
		return Attachment{}, false
	}
	// 1) Anthropic source.{type=base64,media_type,data}
	if src := item.Get("source"); src.Exists() {
		if strings.EqualFold(strings.TrimSpace(src.Get("type").String()), "base64") {
			if raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(src.Get("data").String())); err == nil && len(raw) > 0 {
				return NewAttachment(src.Get("media_type").String(), raw), true
			}
		}
		if u := strings.TrimSpace(src.Get("url").String()); strings.HasPrefix(u, "data:") {
			return decodeDataURL(u)
		}
	}
	// 2) OpenAI image_url：可能是对象 {url:...}，也可能是裸串
	iu := item.Get("image_url")
	u := strings.TrimSpace(iu.Get("url").String())
	if u == "" && iu.Type == gjson.String {
		u = strings.TrimSpace(iu.String())
	}
	if strings.HasPrefix(u, "data:") {
		return decodeDataURL(u)
	}
	// 3) 裸 data 字段（部分客户端的 file / input_file 块）
	if d := strings.TrimSpace(item.Get("data").String()); d != "" {
		if raw, err := base64.StdEncoding.DecodeString(d); err == nil && len(raw) > 0 {
			mt := item.Get("mime_type").String()
			if mt == "" {
				mt = item.Get("media_type").String()
			}
			return NewAttachment(mt, raw), true
		}
	}
	return Attachment{}, false
}

// decodeDataURL 解 data:<mime>;base64,<payload>。非 base64 的 data URL 不接。
func decodeDataURL(u string) (Attachment, bool) {
	rest := strings.TrimPrefix(u, "data:")
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return Attachment{}, false
	}
	meta, payload := rest[:comma], rest[comma+1:]
	if !strings.Contains(meta, "base64") {
		return Attachment{}, false
	}
	mt := strings.TrimSpace(strings.Split(meta, ";")[0])
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(payload))
	if err != nil || len(raw) == 0 {
		return Attachment{}, false
	}
	return NewAttachment(mt, raw), true
}

// RemoteAttachmentURL 取 http(s) 远程附件 URL（没有就回 ""）。
func RemoteAttachmentURL(item gjson.Result) string {
	for _, p := range []string{"image_url.url", "image_url", "source.url", "url"} {
		u := strings.TrimSpace(item.Get(p).String())
		if strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://") {
			return u
		}
	}
	return ""
}

// AttachmentRef 是 prompt 里给模型看的一行：告诉它用 local_fs_read 去读这个虚拟路径。
func AttachmentRef(a Attachment) string {
	return "[attachment " + a.MediaType + " " + a.Path +
		" — read it with the local_fs_read MCP tool to see the actual content]"
}

// ExtractAttachments 扫整个请求体（system + messages[].content[]）收集内联附件，按 ID 去重。
// 与 flattenContent 是两次独立扫描，靠确定性 sha256 ID 保证路径一致。
func ExtractAttachments(body []byte) []Attachment {
	var out []Attachment
	seen := map[string]bool{}
	collect := func(content gjson.Result) {
		if !content.IsArray() {
			return
		}
		content.ForEach(func(_, item gjson.Result) bool {
			if att, ok := DecodeInlineAttachment(item); ok && !seen[att.ID] {
				seen[att.ID] = true
				out = append(out, att)
			}
			return true
		})
	}
	collect(gjson.GetBytes(body, "system"))
	if msgs := gjson.GetBytes(body, "messages"); msgs.IsArray() {
		msgs.ForEach(func(_, m gjson.Result) bool {
			collect(m.Get("content"))
			return true
		})
	}
	return out
}
