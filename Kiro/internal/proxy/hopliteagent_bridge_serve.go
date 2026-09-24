package proxy

// hopliteagent_bridge_serve.go —— 桥的 provider 侧 park/resume serve 路径。
//
// 一次「用 Hoplite 模型改本地文件」的完整多轮：
//   turn-1（新任务）：客户端 /v1/messages 带任务 prompt。
//     provider 生成 bridgeKey → bridgeHub.OpenSession(bridgeKey)（会合槽先就位，无竞态）
//     → bridgeBinder.enqueueWaiter(bridgeKey) → 后台 goroutine 跑整条 Hoplite thread（hopliteExec.Messages，
//       绑**会话 ctx** 不绑请求 ctx，请求断了 thread 不死）→ 等第一个工具请求 or thread 完成。
//     工具请求来 → 翻成 Claude Code tool_use（stop_reason=tool_use）回客户端。
//   turn-N（回 tool_result）：客户端把执行结果发回。
//     provider 按会话键找回 bridgeSession → SubmitToolResult → 等下一个工具请求 or thread 完成。
//   thread 完成 → 回最终文本（end_turn）+ 关会话。
//
// 关联：Claude Code 侧用 coreauth 会话键找 bridgeSession；MCP 侧用 Mcp-Session-Id→bridgeKey（binder）。
// 单会话直接成；多会话靠 provider 串行化「起 thread→首触绑定」这一小段（FIFO），见 bridgeBinder。

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"kiro-proxy/internal/hopbridge/hoptool"
	"kiro-proxy/internal/hopbridge/rendezvous"
	hoplitert "kiro-proxy/internal/runtime/hoplite"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// bridgeModelPrefix 是桥模式的模型前缀。与 hoplite/（云端 thread 直执行）、hopliteagent/（本地 agent）互斥。
const bridgeModelPrefix = "hoplitebridge/"

// isHopliteBridgeModel 判定客户端是否显式钉死桥模式（hoplitebridge/<model>，大小写不敏感）。
func isHopliteBridgeModel(model string) bool {
	m := strings.TrimSpace(model)
	return len(m) >= len(bridgeModelPrefix) && strings.EqualFold(m[:len(bridgeModelPrefix)], bridgeModelPrefix)
}

// stripBridgeModel 去掉 hoplitebridge/ 前缀，返回裸上游模型名（如 gpt-5.6-terra）。空则回落 Terra。
func stripBridgeModel(model string) string {
	m := strings.TrimSpace(model)
	if isHopliteBridgeModel(m) {
		m = strings.TrimSpace(m[len(bridgeModelPrefix):])
	}
	if m == "" {
		return "gpt-5.6-terra"
	}
	return m
}

// bridgeSession 是一条进行中的桥任务的服务端状态。
type bridgeSession struct {
	bridgeKey string
	claudeKey string
	rz        *rendezvous.Session
	toolReqCh chan rendezvous.ToolRequest // pump goroutine 把工具请求送这里
	finalCh   chan bridgeFinal            // thread goroutine 把终态送这里
	cancel    context.CancelFunc          // 取消会话 ctx（关 thread + pump）
	closeOnce sync.Once

	// 以下由 hopliteagent_bridge_recover.go（409 找回层）维护，统一走 mu。
	mu       sync.Mutex
	closed   bool                    // 已关：与「从没有过」分开，409 文案不同
	inflight *rendezvous.ToolRequest // 已发出未收回的那次调用，重发时原样再发
	callIDs  []string                // 本会话发出过的 tool_use id（反查索引的账）
	fp       string                  // 请求指纹：认「同一个任务」，防重发被当新任务
	idle     *time.Timer             // 兜底闹钟：单轮超时不杀会话，靠它收一去不返的
}

type bridgeFinal struct {
	text string
	err  error
}

func (h *Handler) storeBridgeSession(claudeKey string, s *bridgeSession) {
	h.bridgeSessMu.Lock()
	if h.bridgeSessions == nil {
		h.bridgeSessions = map[string]*bridgeSession{}
	}
	h.bridgeSessions[claudeKey] = s
	h.bridgeSessMu.Unlock()
}

func (h *Handler) lookupBridgeSession(claudeKey string) *bridgeSession {
	h.bridgeSessMu.Lock()
	defer h.bridgeSessMu.Unlock()
	return h.bridgeSessions[claudeKey]
}

func (h *Handler) closeBridgeSession(s *bridgeSession) {
	s.closeOnce.Do(func() {
		s.markClosed()
		s.cancel()
		s.rz.Close()
		h.bridgeBinder.cancelWaiter(s.bridgeKey)
		bridgeAttachments.Drop(s.bridgeKey)
		dropBridgeCalls(s)
		// 按**值**删：键可能被 rebind 改过，按旧键删会留悬空项。
		h.dropBridgeSessionEntries(s)
	})
}

// serveHopliteBridgeMessages 处理桥模式的 Anthropic /v1/messages（Claude Code 当客户端）。
func (h *Handler) serveHopliteBridgeMessages(c *gin.Context, body []byte, model string, stream bool) {
	allowed, _, authErr := h.authorizeClient(c, model)
	if authErr != nil {
		c.JSON(authErr.status, errorBody("authentication_error", authErr.msg))
		return
	}
	claudeKey := agentSessionKey(c, body)

	// 续跑：请求里带 tool_result → 找回会话、投递结果、等下一步。
	if results := extractBridgeToolResults(body); len(results) > 0 {
		sess, failMsg := h.resolveBridgeSessionForResults(claudeKey, results)
		if sess == nil {
			c.JSON(http.StatusConflict, errorBody("invalid_request_error", failMsg))
			return
		}
		for _, r := range results {
			sess.rz.SubmitToolResult(r)
		}
		sess.settleCalls(results) // 按 CallID 销账：旧结果不会误清新调用
		h.bridgeNextTurn(c, sess, model, stream)
		return
	}

	// 本轮超时(504)后客户端重发同一条 prompt：会话还活着就接着等，绝不再起第二条云端 thread
	// （再起一条 = 两个 waiter 抢同一个 MCP 会话 + 双倍计费 + 老 thread 永久失联）。
	if sess := h.lookupBridgeSession(claudeKey); sess != nil {
		// 同指纹=原样重发，续跑老会话（还卡在某次调用上就原样重发它）；不同=新任务，关掉老的往下走。
		if h.resumeExistingBridgeSession(c, sess, body, model, stream) {
			return
		}
	}

	// turn-1：新任务。选 Hoplite 账号（localFs 账号，建 thread 时自动注入强提示逼走 MCP）。
	acc, tok, err := h.pickHopliteAccount(allowed)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, errorBody("api_error", noCapacityMessage))
		return
	}
	bridgeKey := "br_" + uuid.NewString()
	rz := h.bridgeHub.OpenSession(bridgeKey)
	h.bridgeBinder.enqueueWaiter(bridgeKey)
	// 客户端贴的图/文件：登记到内存，prompt 里只留 .hoplite-attachments/<id> 虚拟路径，
	// 模型用 local_fs_read 取时由中继端点直接回图（官方 createThread 只吃文本 prompt）。
	bridgeAttachments.Put(bridgeKey, hoplitert.ExtractAttachments(body))
	sessCtx, cancel := context.WithCancel(context.Background())
	sess := &bridgeSession{
		bridgeKey: bridgeKey, claudeKey: claudeKey, rz: rz,
		toolReqCh: make(chan rendezvous.ToolRequest, 1),
		finalCh:   make(chan bridgeFinal, 1),
		cancel:    cancel,
		fp:        bridgeRequestFingerprint(body),
	}
	h.storeBridgeSession(claudeKey, sess)

	// pump：把会合的工具请求源源不断搬到 toolReqCh（会话 ctx 结束即退）。
	go func() {
		for {
			tr, ok := sess.rz.NextToolRequest(sessCtx)
			if !ok {
				return
			}
			select {
			case sess.toolReqCh <- tr:
			case <-sessCtx.Done():
				return
			}
		}
	}()

	// 把 hoplitebridge/ 前缀剥成裸上游模型名（gpt-5.6-terra 等），否则执行器把整串当模型发给 Hoplite → invalid_model。
	body, _ = sjson.SetBytes(body, "model", stripBridgeModel(model))
	// thread：跑整条 Hoplite thread（绑会话 ctx）。跑动时的 MCP 调用经 Node 中继→内部端点→bridgeHub 会合。
	client := EgressClientForStreaming(h.pools, acc, h.cfg)
	go func() {
		res, rerr := h.hopliteExec.Messages(sessCtx, client, tok, body)
		persistHopliteThread(h.accounts, acc, res)
		text := ""
		if res != nil {
			text = gjson.GetBytes(res.Body, "content.0.text").String()
		}
		sess.finalCh <- bridgeFinal{text: text, err: rerr}
	}()

	log.Infof("proxy: /v1/messages provider=hoplite-bridge account=%s model=%s bridgeKey=%s (park)", acc.ID, model, bridgeKey)
	h.bridgeNextTurn(c, sess, model, stream)
}

// bridgeNextTurn 等「下一个工具请求」或「thread 完成」，据此回 tool_use 或最终文本。
func (h *Handler) bridgeNextTurn(c *gin.Context, sess *bridgeSession, model string, stream bool) {
	h.touchBridgeSession(sess) // 兜底闹钟：本轮超时/断开都不杀会话，只有它和 thread 终态能收
	select {
	case tr := <-sess.toolReqCh:
		h.writeBridgeInflight(c, sess, tr, model, stream)
	case fin := <-sess.finalCh:
		h.closeBridgeSession(sess)
		if fin.err != nil {
			c.JSON(statusOfHoplite(fin.err), errorBody("api_error", publicHopliteErr(fin.err)))
			return
		}
		h.writeBridgeFinal(c, model, fin.text, stream)
	case <-c.Request.Context().Done():
		// 客户端断开：不立刻杀会话（可能只是这轮超时），交给 reaper/Cleanup 按 idleTTL 收。
		return
	case <-time.After(h.agentMaxWait()):
		// 本轮等超时**不杀会话**：云端 thread 是长任务（十几分钟很常见），杀了之后所有 MCP 调用
		// 在 bridgeToolRelay 都会 503「no parked request」，thread 永久失联。只回 504 让客户端重发，
		// 会话留给 idleTTL 回收——与上面「客户端断开」一支保持一致。
		log.Warnf("proxy: hopbridge turn timeout bridgeKey=%s (session kept alive)", sess.bridgeKey)
		c.JSON(http.StatusGatewayTimeout, errorBody("api_error", "bridge turn timeout (thread still running; resend to keep waiting)"))
	}
}

// extractBridgeToolResults 从 Anthropic 请求体里抽 tool_result 块（Claude Code 执行完回送的）。
func extractBridgeToolResults(body []byte) []rendezvous.ToolResult {
	var out []rendezvous.ToolResult
	msgs := gjson.GetBytes(body, "messages")
	if !msgs.IsArray() {
		return nil
	}
	msgs.ForEach(func(_, m gjson.Result) bool {
		if m.Get("role").String() != "user" {
			return true
		}
		content := m.Get("content")
		if !content.IsArray() {
			return true
		}
		content.ForEach(func(_, item gjson.Result) bool {
			if item.Get("type").String() != "tool_result" {
				return true
			}
			id := item.Get("tool_use_id").String()
			if id == "" {
				return true
			}
			// Read 一个 PNG 时 Claude Code 回的是 image 块（无 text 字段）。
			// 图连同文本一起打包进 Content，中继端点再解回 MCP text+image。
			text, images := hoptool.ParseClaudeToolResultContent(item.Get("content"))
			out = append(out, rendezvous.ToolResult{
				CallID:  id,
				Content: hoptool.EncodeToolResult(text, images),
				IsError: item.Get("is_error").Bool(),
			})
			return true
		})
		return true
	})
	// 只取最后一条 user 消息里的 tool_result（Claude Code 每轮把结果放最后一条）——去重保留最新。
	return dedupLastToolResults(out)
}

// bridgeToolResultText 把 tool_result.content 压成纯文本（图片块由
// hoptool.ParseClaudeToolResultContent 单独取走）。保留给既有单测 / 调用点。
func bridgeToolResultText(v gjson.Result) string {
	text, _ := hoptool.ParseClaudeToolResultContent(v)
	return text
}

// dedupLastToolResults 同一 CallID 保留最后出现的一条。
func dedupLastToolResults(in []rendezvous.ToolResult) []rendezvous.ToolResult {
	if len(in) <= 1 {
		return in
	}
	seen := map[string]int{}
	for i, r := range in {
		seen[r.CallID] = i
	}
	var out []rendezvous.ToolResult
	for i, r := range in {
		if seen[r.CallID] == i {
			out = append(out, r)
		}
	}
	return out
}

// writeBridgeInflight 把一次工具调用翻成 tool_use 回客户端，并登记成 inflight（丢了能原样重发）。
func (h *Handler) writeBridgeInflight(c *gin.Context, sess *bridgeSession, tr rendezvous.ToolRequest, model string, stream bool) {
	tu, err := hoptool.ToClaudeToolUse(hoptool.McpToolCall{Name: tr.ToolName, Arguments: tr.Arguments}, "")
	if err != nil {
		// 翻译失败：把错误当结果回给会合（让 Hoplite 知道这步失败），并回客户端错误。
		sess.rz.SubmitToolResult(rendezvous.ToolResult{CallID: tr.CallID, Content: "tool translate error: " + err.Error(), IsError: true})
		c.JSON(http.StatusBadGateway, errorBody("api_error", "bridge tool translate failed"))
		return
	}
	trackBridgeCall(sess, tr)
	h.writeBridgeToolUse(c, model, tr.CallID, tu, stream)
}

// writeBridgeToolUse 回一个带 tool_use 块的 Anthropic 响应（stop_reason=tool_use），让 Claude Code 本机执行。
func (h *Handler) writeBridgeToolUse(c *gin.Context, model, callID string, tu hoptool.ClaudeToolUse, stream bool) {
	if !stream {
		msg := map[string]any{
			"id": "msg_" + uuid.NewString(), "type": "message", "role": "assistant", "model": model,
			"content":     []map[string]any{{"type": "tool_use", "id": callID, "name": tu.Name, "input": json.RawMessage(tu.Input)}},
			"stop_reason": "tool_use", "stop_sequence": nil,
			"usage": map[string]int{"input_tokens": 0, "output_tokens": 0},
		}
		b, _ := json.Marshal(msg)
		c.Data(http.StatusOK, "application/json", b)
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Status(http.StatusOK)
	c.Writer.Flush()
	msgID := "msg_" + uuid.NewString()
	start, _ := json.Marshal(map[string]any{"type": "message_start", "message": map[string]any{
		"id": msgID, "type": "message", "role": "assistant", "model": model, "content": []any{},
		"stop_reason": nil, "usage": map[string]int{"input_tokens": 0, "output_tokens": 0}}})
	blockStart, _ := json.Marshal(map[string]any{"type": "content_block_start", "index": 0,
		"content_block": map[string]any{"type": "tool_use", "id": callID, "name": tu.Name, "input": map[string]any{}}})
	inputJSON := string(tu.Input)
	if inputJSON == "" {
		inputJSON = "{}"
	}
	delta, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": 0,
		"delta": map[string]any{"type": "input_json_delta", "partial_json": inputJSON}})
	blockStop, _ := json.Marshal(map[string]any{"type": "content_block_stop", "index": 0})
	msgDelta, _ := json.Marshal(map[string]any{"type": "message_delta",
		"delta": map[string]any{"stop_reason": "tool_use", "stop_sequence": nil}, "usage": map[string]int{"output_tokens": 0}})
	for _, f := range [][2]string{{"message_start", string(start)}, {"content_block_start", string(blockStart)},
		{"content_block_delta", string(delta)}, {"content_block_stop", string(blockStop)},
		{"message_delta", string(msgDelta)}, {"message_stop", `{"type":"message_stop"}`}} {
		c.Writer.Write([]byte("event: " + f[0] + "\ndata: " + f[1] + "\n\n"))
	}
	c.Writer.Flush()
}

// writeBridgeFinal 回最终文本（end_turn），一轮桥任务结束。
func (h *Handler) writeBridgeFinal(c *gin.Context, model, text string, stream bool) {
	if !stream {
		c.Data(http.StatusOK, "application/json", agentAnthropicMessage(model, text))
		return
	}
	c.Header("Content-Type", "text/event-stream")
	c.Status(http.StatusOK)
	c.Writer.Flush()
	for _, chunk := range agentAnthropicSSE(model, text) {
		c.Writer.Write(chunk)
	}
	c.Writer.Flush()
}
