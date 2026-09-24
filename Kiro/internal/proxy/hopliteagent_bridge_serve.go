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
//
// 会话只由三件事终结：thread 跑完 / thread 报错 / 同会话换了新任务（外加 idleTTL 兜底回收）。
// **单轮 HTTP 的超时与断开都不终结它**——客户端重发就接着跑，找回逻辑见 hopliteagent_bridge_recover.go。

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"kiro-proxy/internal/hopbridge/hoptool"
	"kiro-proxy/internal/hopbridge/rendezvous"

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
// mu 护后三个字段（它们跨多轮 HTTP 请求被读写）；与 Handler.bridgeSessMu 的锁序固定为
// 「先 bridgeSessMu 后 session.mu」，反向不出现——持 session.mu 时绝不去碰 Handler 的表。
type bridgeSession struct {
	bridgeKey string
	rz        *rendezvous.Session
	toolReqCh chan rendezvous.ToolRequest // pump goroutine 把工具请求送这里
	finalCh   chan bridgeFinal            // thread goroutine 把终态送这里
	cancel    context.CancelFunc          // 取消会话 ctx（关 thread + pump）
	closeOnce sync.Once

	mu        sync.Mutex
	claudeKey string                  // 客户端会话键（可能漂移；靠 tool_use id 找回后改写）
	inflight  *rendezvous.ToolRequest // 已发给客户端、还没收回结果的那次调用（重发用）
	promptFP  string                  // turn-1 请求指纹：分辨「同轮重试」与「同会话新任务」
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
		s.cancel()
		s.rz.Close()
		h.bridgeBinder.cancelWaiter(s.bridgeKey)
		bridgeCallIndex.dropSession(s)
		h.bridgeSessMu.Lock()
		// 按值删：会话键可能已被 rebind 改过，只按当前键删会留下悬空项。
		for k, cur := range h.bridgeSessions {
			if cur == s {
				delete(h.bridgeSessions, k)
			}
		}
		h.bridgeSessMu.Unlock()
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
		sess, why := h.resolveBridgeSession(claudeKey, results)
		if sess == nil {
			log.Warnf("proxy: hoplite-bridge no session for tool results key=%s calls=%s live=%d (%s)",
				claudeKey, bridgeCallIDs(results), h.bridgeSessionCount(), why)
			c.JSON(http.StatusConflict, errorBody("invalid_request_error", "no active bridge session for these tool results ("+why+")"))
			return
		}
		sess.noteToolResults(results)
		for _, r := range results {
			sess.rz.SubmitToolResult(r)
		}
		h.bridgeNextTurn(c, sess, model, stream)
		return
	}

	// 不带 tool_result 的请求有两种，必须分开：
	//   a) 上一轮 504/断线后客户端**原样重发** → 接着等同一条会话，绝不再起第二条云端 thread
	//      （再起一条 = 两个 waiter 抢同一个 MCP 会话 + 双倍计费 + 老 thread 永久失联）。
	//   b) 同一个客户端会话里**换了新任务** → 老会话作废，正常起新 thread。
	fp := bridgeRequestFingerprint(body)
	if sess := h.lookupBridgeSession(claudeKey); sess != nil {
		switch {
		case !sess.alive():
			h.closeBridgeSession(sess) // 已被 idleTTL 回收的空壳，让位
		case sess.sameRequest(fp):
			log.Infof("proxy: /v1/messages provider=hoplite-bridge resume parked bridgeKey=%s", sess.bridgeKey)
			h.bridgeNextTurn(c, sess, model, stream)
			return
		default:
			log.Infof("proxy: /v1/messages provider=hoplite-bridge new task supersedes bridgeKey=%s", sess.bridgeKey)
			h.closeBridgeSession(sess)
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
	sessCtx, cancel := context.WithCancel(context.Background())
	sess := &bridgeSession{
		bridgeKey: bridgeKey, claudeKey: claudeKey, rz: rz,
		toolReqCh: make(chan rendezvous.ToolRequest, 1),
		finalCh:   make(chan bridgeFinal, 1),
		cancel:    cancel,
		promptFP:  fp,
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

// bridgeSessionCount 报告在册桥会话数（排障日志用）。
func (h *Handler) bridgeSessionCount() int {
	h.bridgeSessMu.Lock()
	defer h.bridgeSessMu.Unlock()
	return len(h.bridgeSessions)
}

// bridgeCallIDs 把一批结果的 CallID 拼成日志串。
func bridgeCallIDs(results []rendezvous.ToolResult) string {
	ids := make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.CallID)
	}
	return strings.Join(ids, ",")
}

// bridgeNextTurn 等「下一个工具请求」或「thread 完成」，据此回 tool_use 或最终文本。
func (h *Handler) bridgeNextTurn(c *gin.Context, sess *bridgeSession, model string, stream bool) {
	// 上一轮发出去但没收回结果的调用：原样重发同一个 CallID。
	// 不重发就会在这儿干等「下一个」工具请求——而云端 thread 正卡在那次调用上等结果，两边互等到 TTL。
	if tr, ok := sess.pendingInflight(); ok {
		log.Infof("proxy: hoplite-bridge replay tool_use bridgeKey=%s call=%s", sess.bridgeKey, tr.CallID)
		h.emitBridgeToolUse(c, sess, tr, model, stream)
		return
	}
	select {
	case tr := <-sess.toolReqCh:
		h.emitBridgeToolUse(c, sess, tr, model, stream)
	case fin := <-sess.finalCh:
		h.closeBridgeSession(sess)
		if fin.err != nil {
			c.JSON(statusOfHoplite(fin.err), errorBody("api_error", publicHopliteErr(fin.err)))
			return
		}
		h.writeBridgeFinal(c, model, fin.text, stream)
	case <-c.Request.Context().Done():
		// 客户端断开：不杀会话（可能只是这轮超时），交给 reaper/Cleanup 按 idleTTL 收。
		return
	case <-time.After(h.agentMaxWait()):
		// 本轮等超时：**只结束这一轮 HTTP**。会话、云端 thread、未收回的调用全留着，
		// 客户端重发即从原地接着跑；在这儿杀会话正是 409 的头号来源。
		log.Warnf("proxy: hoplite-bridge turn timeout bridgeKey=%s (session kept)", sess.bridgeKey)
		c.JSON(http.StatusGatewayTimeout, errorBody("api_error", "bridge turn timeout (session kept, resend to resume)"))
	}
}

// emitBridgeToolUse 把一次会合来的工具请求翻成 tool_use 发给客户端，并记好账（重发 + 反查）。
func (h *Handler) emitBridgeToolUse(c *gin.Context, sess *bridgeSession, tr rendezvous.ToolRequest, model string, stream bool) {
	tu, err := hoptool.ToClaudeToolUse(hoptool.McpToolCall{Name: tr.ToolName, Arguments: tr.Arguments}, "")
	if err != nil {
		// 翻译失败：把错误当结果回给会合（让 Hoplite 知道这步失败），并回客户端错误。
		sess.clearInflight(tr.CallID)
		sess.rz.SubmitToolResult(rendezvous.ToolResult{CallID: tr.CallID, Content: "tool translate error: " + err.Error(), IsError: true})
		c.JSON(http.StatusBadGateway, errorBody("api_error", "bridge tool translate failed"))
		return
	}
	sess.setInflight(tr)                 // 客户端没回结果前，这次调用要能重发
	bridgeCallIndex.put(tr.CallID, sess) // 会话键漂移时靠它反查回来
	h.writeBridgeToolUse(c, model, tr.CallID, tu, stream)
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
			out = append(out, rendezvous.ToolResult{
				CallID:  id,
				Content: bridgeToolResultText(item.Get("content")),
				IsError: item.Get("is_error").Bool(),
			})
			return true
		})
		return true
	})
	// 只取最后一条 user 消息里的 tool_result（Claude Code 每轮把结果放最后一条）——去重保留最新。
	return dedupLastToolResults(out)
}

// bridgeToolResultText 把 tool_result.content（字符串或块数组）压成纯文本。
func bridgeToolResultText(v gjson.Result) string {
	if v.Type == gjson.String {
		return v.String()
	}
	if v.IsArray() {
		var s string
		v.ForEach(func(_, item gjson.Result) bool {
			if t := item.Get("text").String(); t != "" {
				if s != "" {
					s += "\n"
				}
				s += t
			}
			return true
		})
		return s
	}
	return v.String()
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
