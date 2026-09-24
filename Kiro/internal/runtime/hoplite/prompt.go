package hoplite

import (
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

// LocalFSPreamble 是账号开了 localFs 时，建 thread 前置到 prompt 的强提示。
// 逼 Hoplite 云端模型只用项目里注册的 local-fs MCP 工具改「远端服务器本地文件」，
// 而不是改它沙箱里那份一次性副本（沙箱改动会被丢弃）。工具名与 MCP server 对齐。
// 实测（2026-09-17 gpt-5.6-terra）：加此前置，模型确实回调 local_fs_write 改服务器本地文件。
const LocalFSPreamble = "CRITICAL EXECUTION RULE: The real project files live on a REMOTE host reachable ONLY through the MCP tools local_fs_list, local_fs_read, local_fs_write. " +
	"The sandbox filesystem you normally see is NOT the target — any edits you make there are thrown away. " +
	"You MUST use local_fs_read to read and local_fs_write to write every file change. " +
	"Do NOT use your built-in Read/Write/Edit/file tools — they write to the wrong place and the task will silently FAIL. " +
	"Use local_fs_list to explore the tree.\n\n---\n\n"

// PrependLocalFS 在原 prompt 前拼上 LocalFSPreamble（仅账号 localFs 开启、新建 thread 时用）。
func PrependLocalFS(prompt string) string {
	return LocalFSPreamble + prompt
}

// FlattenOpenAI 把 /v1/chat/completions 的 messages 压成 Hoplite thread 单 prompt。
// 多轮对话按 "role: content" 拼接；system 置顶。
// 续跑 append 时官方 thread 自带历史；适配层仍把本次 messages 压成一条 user content。
func FlattenOpenAI(body []byte) string {
	arr := gjson.GetBytes(body, "messages")
	if !arr.IsArray() {
		if p := strings.TrimSpace(gjson.GetBytes(body, "prompt").String()); p != "" {
			return p
		}
		return strings.TrimSpace(gjson.GetBytes(body, "input").String())
	}
	var b strings.Builder
	arr.ForEach(func(_, msg gjson.Result) bool {
		role := strings.TrimSpace(msg.Get("role").String())
		if role == "" {
			role = "user"
		}
		text := flattenContent(msg.Get("content"))
		if text == "" {
			return true
		}
		if b.Len() > 0 {
			b.WriteString("\n\n")
		}
		b.WriteString(role)
		b.WriteString(": ")
		b.WriteString(text)
		return true
	})
	return strings.TrimSpace(b.String())
}

// FlattenAnthropic 把 /v1/messages 的 system + messages 压成单 prompt。
func FlattenAnthropic(body []byte) string {
	var b strings.Builder
	if sys := flattenContent(gjson.GetBytes(body, "system")); sys != "" {
		b.WriteString("system: ")
		b.WriteString(sys)
	}
	msgs := gjson.GetBytes(body, "messages")
	if msgs.IsArray() {
		msgs.ForEach(func(_, msg gjson.Result) bool {
			role := strings.TrimSpace(msg.Get("role").String())
			if role == "" {
				role = "user"
			}
			text := flattenContent(msg.Get("content"))
			if text == "" {
				return true
			}
			if b.Len() > 0 {
				b.WriteString("\n\n")
			}
			b.WriteString(role)
			b.WriteString(": ")
			b.WriteString(text)
			return true
		})
	}
	if b.Len() == 0 {
		return FlattenOpenAI(body)
	}
	return strings.TrimSpace(b.String())
}

func flattenContent(v gjson.Result) string {
	if !v.Exists() {
		return ""
	}
	if v.Type == gjson.String {
		return strings.TrimSpace(v.String())
	}
	if v.IsArray() {
		var parts []string
		v.ForEach(func(_, item gjson.Result) bool {
			switch {
			case item.Type == gjson.String:
				if s := strings.TrimSpace(item.String()); s != "" {
					parts = append(parts, s)
				}
			case item.Get("text").Exists():
				if s := strings.TrimSpace(item.Get("text").String()); s != "" {
					parts = append(parts, s)
				}
			case item.Get("content").Exists() && item.Get("content").Type == gjson.String:
				if s := strings.TrimSpace(item.Get("content").String()); s != "" {
					parts = append(parts, s)
				}
			default:
				t := strings.TrimSpace(item.Get("type").String())
				if t == "" || t == "text" {
					return true
				}
				// 图 / 文件真接：落成虚拟附件路径，让模型用 local_fs_read 去取真内容。
				// 官方 createThread 只吃文本 prompt，但桥模式下我们自己就是模型的 MCP 服务端，
				// 所以像素数据走工具结果进去，不再是 [unwired_image:…] 这种假占位。
				if att, ok := DecodeInlineAttachment(item); ok {
					parts = append(parts, AttachmentRef(att))
					return true
				}
				if u := RemoteAttachmentURL(item); u != "" {
					parts = append(parts, "[attachment url "+u+"]")
					return true
				}
				name := strings.TrimSpace(item.Get("name").String())
				if name == "" {
					name = t
				}
				parts = append(parts, "["+name+"]")
			}
			return true
		})
		return strings.TrimSpace(strings.Join(parts, "\n"))
	}
	if v.IsObject() {
		if s := strings.TrimSpace(v.Get("text").String()); s != "" {
			return s
		}
	}
	var raw any
	if err := json.Unmarshal([]byte(v.Raw), &raw); err == nil {
		if s, ok := raw.(string); ok {
			return strings.TrimSpace(s)
		}
	}
	return strings.TrimSpace(v.String())
}
