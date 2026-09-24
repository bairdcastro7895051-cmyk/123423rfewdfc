package proxy

// hopliteagent_bridge.go —— 「Hoplite 模型 → 本地 Claude Code 执行文件」桥的 kiro-proxy 侧集成。
//
// 全链：本地 Claude Code(客户端) ──/v1/messages──▶ kiro-proxy provider(挂起,起 Hoplite thread)
//        Hoplite 云端模型 ──MCP──▶ .40 Node MCP server(官方 SDK 扛 session/SSE)
//        Node 收到 local_fs_* 工具调用 ──loopback POST──▶ 本文件的内部中继端点
//        中继端点 ──bridgeHub.DeliverToolCall(sessionId)──▶ 会合到挂起的 provider
//        provider 把工具调用翻成 Claude Code tool_use 回客户端 → 客户端本机执行 → tool_result 回来
//        ──SubmitToolResult──▶ 唤醒中继端点 → 回 MCP 结果给 Node → 回给 Hoplite 云端模型
//
// 本文件负责：① 内部中继端点(收 Node 转发) ② 绑定器(bind-on-first-contact：把新 Mcp-Session-Id
// 绑给最早在等的挂起 provider —— 单会话直接成；多会话靠串行化建 thread 不串)
// ③ 附件出口：客户端贴的图不在磁盘上，指向 .hoplite-attachments/ 的 local_fs_read 由本文件
//    直接从内存作答，不下发给 Claude Code。
// provider 侧 park/resume 循环见 hopliteagent_bridge_serve.go。

import (
	"encoding/json"
	"errors"
	"net/http"
	"sync"

	"kiro-proxy/internal/hopbridge/hoptool"
	"kiro-proxy/internal/hopbridge/rendezvous"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

// bridgeInternalPath 是 Node MCP 中继回打的 loopback 端点（仅本机可达；下方 handler 再校验来源）。
const bridgeInternalPath = "/internal/hopbridge/tool"

// bridgeRelayToken 头：Node 中继与 kiro-proxy 之间的共享秘钥（防本机其它进程乱打）。运行时从配置注入。
const bridgeRelayTokenHeader = "X-Hopbridge-Relay-Token"

// bridgeAttachments 存活跃桥会话的内联附件（客户端贴的图/文件），按 bridgeKey 索引。
// 进程级单例：与 Handler 同生命周期，不改 Handler 结构体（热区）。
var bridgeAttachments = hoptool.NewAttachmentStore()

// bridgeBinder 解决关联缝：provider 挂起时知道 thread，但 MCP 调用来时带的是 Node 分配的 Mcp-Session-Id，
// 两者对不上。做法：provider 起 thread 后把自己排进等待队列(FIFO)；首个带**新** sessionId 的工具调用
// 到来时，弹出最早的等待者、把这个 sessionId 绑给它。之后同 sessionId 的调用直接走 bridgeHub 会合。
//
// 单会话：队列里就一个等待者，无歧义。多会话并发：provider 侧串行化「起 thread→等绑定」这一小段即可
// 保证 FIFO 顺序不错配（见 serve 侧注释）。
// 无竞态设计：provider 先用自己生成的 bridgeKey `OpenSession(bridgeKey)`（会合槽先就位），再 enqueueWaiter。
// relay 端点用 resolve(mcpSessionId) 拿到 bridgeKey → DeliverToolCall(bridgeKey)——此时 session 必已开，
// 不会撞「session 还没开」的竞态。binder 只做 mcpSessionId→bridgeKey 的 FIFO 映射。
type bridgeBinder struct {
	mu      sync.Mutex
	waiters []string          // FIFO：挂起 provider 的 bridgeKey，等一个 MCP 会话来认领
	mapping map[string]string // mcpSessionId -> bridgeKey（已认领）
}

func newBridgeBinder() *bridgeBinder {
	return &bridgeBinder{mapping: map[string]string{}}
}

// enqueueWaiter 由 provider 侧调用：把自己的 bridgeKey 排进等待队列。
func (b *bridgeBinder) enqueueWaiter(bridgeKey string) {
	b.mu.Lock()
	b.waiters = append(b.waiters, bridgeKey)
	b.mu.Unlock()
}

// cancelWaiter 由 provider 侧调用（请求断开/超时/会话结束）：摘掉自己的 bridgeKey + 清掉指向它的映射。
func (b *bridgeBinder) cancelWaiter(bridgeKey string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, w := range b.waiters {
		if w == bridgeKey {
			b.waiters = append(b.waiters[:i], b.waiters[i+1:]...)
			break
		}
	}
	for sid, bk := range b.mapping {
		if bk == bridgeKey {
			delete(b.mapping, sid)
		}
	}
}

// resolve 由中继端点调用：mcpSessionId 已认领 → 返回其 bridgeKey；否则弹出最早等待者认领之；无等待者 → (,false)。
func (b *bridgeBinder) resolve(mcpSessionID string) (bridgeKey string, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bk, exists := b.mapping[mcpSessionID]; exists {
		return bk, true // 已知会话，bridgeHub 里已有对应 Session
	}
	if len(b.waiters) > 0 {
		bk := b.waiters[0]
		b.waiters = b.waiters[1:]
		b.mapping[mcpSessionID] = bk
		return bk, true
	}
	return "", false // 没有等待中的 provider
}

// registerHopliteBridge 挂内部中继端点（由 Handler.Register 调用）。
func (h *Handler) registerHopliteBridge(r gin.IRoutes) {
	r.POST(bridgeInternalPath, h.bridgeToolRelay)
}

// bridgeToolRelayReq 是 Node MCP 中继转发来的一次工具调用。
type bridgeToolRelayReq struct {
	SessionID string          `json:"sessionId"`
	CallID    string          `json:"callId"`
	Tool      string          `json:"tool"`
	Arguments json.RawMessage `json:"arguments"`
}

// bridgeToolRelay 收 Node 转发的工具调用，会合到挂起的 provider，阻塞等 Claude Code 执行结果再回。
func (h *Handler) bridgeToolRelay(c *gin.Context) {
	// 仅允许本机来源（Node 中继与 kiro-proxy 同机 loopback）+ 共享秘钥。
	if !isLoopbackClient(c.ClientIP()) {
		c.JSON(http.StatusForbidden, gin.H{"error": "loopback only"})
		return
	}
	if tok := h.bridgeRelayToken(); tok != "" && c.GetHeader(bridgeRelayTokenHeader) != tok {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "bad relay token"})
		return
	}
	var req bridgeToolRelayReq
	if err := c.ShouldBindJSON(&req); err != nil || req.SessionID == "" || req.Tool == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "sessionId+tool required"})
		return
	}
	// 绑定：新会话认领最早挂起 provider 的 bridgeKey；无人挂起 → 无处会合。
	bridgeKey, ok := h.bridgeBinder.resolve(req.SessionID)
	if !ok {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "no parked request for session"})
		return
	}
	// 虚拟附件（客户端贴的图）：压根不在磁盘上，直接从内存回 MCP image content，不下发 Claude Code。
	if out, handled := bridgeAttachments.Serve(bridgeKey, req.Tool, req.Arguments); handled {
		c.Data(http.StatusOK, "application/json", out)
		return
	}
	callID := req.CallID
	if callID == "" {
		callID = "toolu_" + uuid.NewString()
	}
	res, err := h.bridgeHub.DeliverToolCall(c.Request.Context(), bridgeKey, rendezvous.ToolRequest{
		CallID:    callID,
		ToolName:  req.Tool,
		Arguments: []byte(req.Arguments),
	})
	if err != nil {
		log.Warnf("proxy: hopbridge relay session=%s tool=%s deliver failed: %v", req.SessionID, req.Tool, err)
		c.JSON(bridgeRelayStatus(err), gin.H{"error": err.Error()})
		return
	}
	// 回 MCP 工具结果形状（Node 直接透传给 Hoplite）。Claude Code 读 PNG 回的是 image 块，
	// 经 EncodeToolResult 封在 Content 里带过来，这里解回 text+image 两种块。
	text, images := hoptool.DecodeToolResult(res.Content)
	out, _ := hoptool.FromClaudeToolResultWithImages(text, images, res.IsError)
	c.Data(http.StatusOK, "application/json", out)
}

func bridgeRelayStatus(err error) int {
	switch {
	case errors.Is(err, rendezvous.ErrNoSession), errors.Is(err, rendezvous.ErrSessionClosed):
		return http.StatusServiceUnavailable
	case errors.Is(err, rendezvous.ErrTimeout):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

// isLoopbackClient 判定来源是不是本机 loopback。
func isLoopbackClient(ip string) bool {
	return ip == "127.0.0.1" || ip == "::1" || ip == ""
}

// bridgeRelayToken 从配置取 Node↔proxy 共享秘钥（空 = 不校验，仅靠 loopback 限制）。
func (h *Handler) bridgeRelayToken() string {
	if h == nil || h.cfg == nil {
		return ""
	}
	return h.cfg.HopliteAgent.BridgeRelayToken
}
