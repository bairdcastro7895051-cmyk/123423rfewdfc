package proxy

// hopliteagent_bridge_recover.go —— 桥会话的「找得回来」层：专治
// `409 no active bridge session for these tool results (session expired?)`。
//
// 409 的成因从来不是「会话真的过期」，而是三条会话丢失的路径：
//
//  1. 本轮 HTTP 等超时（agentMaxWait）后把整条桥会话连同云端 thread 一起杀了。客户端按常规
//     重发同一条 tool_result，会话已经没了 → 409。
//  2. 会话键漂移。会话按 coreauth 的 8 级探测键（claudeKey）存；客户端压缩上下文/换
//     metadata.user_id/走哈希兜底时这个键会变，老会话还活着却查不到 → 409。
//  3. tool_use 响应在半路丢了（客户端断线/超时）。云端 thread 卡在那次 MCP 调用上等结果，
//     客户端重发后我们却在干等「下一个」工具请求，一直等到 agentMaxWait → 504 → 再一次 409。
//
// 对应三件事：① 超时只结束这一轮 HTTP，会话留着（见 bridgeNextTurn）；② 用我们自己发出去的
// tool_use id 反查会话并改挂到新键上（bridgeCallIndex + resolveBridgeSession）；③ 记住「已发出
// 未收回」的那次调用，客户端再来时原样重发同一个 CallID（bridgeSession.inflight）。
//
// 只有「三条都没找回来」才回 409，且带上可排障的原因。
// 全部逻辑落在本文件，serve/relay 两个既有文件只留调用点。

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"sync"
	"time"

	"kiro-proxy/internal/hopbridge/rendezvous"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// bridgeIdleTTLFloor 是「这一轮 HTTP 已经结束、但会话留着等重发」的最短存活时间。
const bridgeIdleTTLFloor = 10 * time.Minute

// bridgeIdleTTL 无人接手时收会话的等待时长：给客户端足够时间重发（它的重试间隔按单轮上限走），
// 又不至于让一去不返的会话带着 thread / pump 两个 goroutine 常驻。
func (h *Handler) bridgeIdleTTL() time.Duration {
	d := 3 * h.agentMaxWait()
	if d < bridgeIdleTTLFloor {
		d = bridgeIdleTTLFloor
	}
	return d
}

// —— ① tool_use id → 桥会话 的全局索引 ——
//
// 进程内、纯内存，键是我们自己生成的 CallID（全局唯一）。会话关闭时整批清掉，所以不会无限涨。
// 放包级而不是挂 Handler：CallID 唯一，多 Handler（测试）也不会互相看见对方的 id。
type bridgeCallRegistry struct {
	mu sync.Mutex
	m  map[string]*bridgeSession
	fp map[string]*bridgeSession // 请求指纹 → 会话，见 recoverByFingerprint
}

var bridgeCallIndex = &bridgeCallRegistry{m: map[string]*bridgeSession{}, fp: map[string]*bridgeSession{}}

func (r *bridgeCallRegistry) put(callID string, s *bridgeSession) {
	if callID == "" || s == nil {
		return
	}
	r.mu.Lock()
	r.m[callID] = s
	r.mu.Unlock()
}

func (r *bridgeCallRegistry) get(callID string) *bridgeSession {
	if callID == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.m[callID]
}

// dropSession 清掉某会话名下的全部 CallID（会话 Close 时调，防止索引常驻）。
func (r *bridgeCallRegistry) dropSession(s *bridgeSession) {
	if s == nil {
		return
	}
	r.mu.Lock()
	for id, sess := range r.m {
		if sess == s {
			delete(r.m, id)
		}
	}
	for fp, sess := range r.fp {
		if sess == s {
			delete(r.fp, fp)
		}
	}
	r.mu.Unlock()
}

// putFingerprint 登记「起这条会话的那次请求」的指纹（turn-1 时调）。
func (r *bridgeCallRegistry) putFingerprint(fp string, s *bridgeSession) {
	if fp == "" || s == nil {
		return
	}
	r.mu.Lock()
	r.fp[fp] = s
	r.mu.Unlock()
}

func (r *bridgeCallRegistry) getByFingerprint(fp string) *bridgeSession {
	if fp == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.fp[fp]
}

// recoverByFingerprint 补上「**不带** tool_result 的原样重发 + 会话键同时漂了」这一格：
// 这条路径上客户端没回任何 tool_use id，按 id 反查用不上，只剩请求指纹能认人。
// 认出来就改挂到当前键续跑，否则会照常走 turn-1 再起一条云端 thread（双倍计费 + 老 thread 失联）。
func (h *Handler) recoverByFingerprint(claudeKey, scopedFP string) *bridgeSession {
	s := bridgeCallIndex.getByFingerprint(scopedFP)
	if s == nil || !s.alive() {
		return nil
	}
	h.rebindBridgeSession(s, claudeKey)
	return s
}

// —— ② 会话找回 ——

// resolveBridgeSession 给一批 tool_result 找回它们所属的桥会话。
// **先按 tool_use id 反查，键查在后**：id 是我们自己铸的、全局唯一，它指向的会话一定是这批结果
// 真正的主人；会话键却会漂，而且漂走之后这个键上可能已经挂了另一条会话——按键投递就把结果投给了
// 错的会话（真主人继续干等、错收方收到认不出的 CallID 直接丢掉，两边一起等到超时）。
// 命中即把会话改挂到当前会话键上（键漂移后续轮才继续粘得住）。都没有 → (nil, 给客户端看的原因)。
//
// 同一个键上撞着另一条活会话时，rebind 会把它从键表里挤下来：那条会话仍能靠 CallID / 指纹被认回，
// 最终由兜底闹钟收；这比把结果投错强——投错是必然双边超时。
func (h *Handler) resolveBridgeSession(claudeKey string, results []rendezvous.ToolResult) (*bridgeSession, string) {
	deadShell := false
	// 从**最后一条**结果往前找：请求体里带着整段历史的 tool_result，靠前的那些可能属于同一个客户端
	// 早先的、还没被收掉的另一条会话；末尾那条才是这一轮真正要投递的调用。
	for i := len(results) - 1; i >= 0; i-- {
		r := results[i]
		if s := bridgeCallIndex.get(r.CallID); s != nil {
			if s.alive() {
				h.rebindBridgeSession(s, claudeKey)
				return s, ""
			}
			deadShell = true
		}
	}
	if s := h.lookupBridgeSession(claudeKey); s != nil {
		if s.alive() {
			return s, ""
		}
		deadShell = true
		h.closeBridgeSession(s) // 空壳先摘掉，免得挡住后面的新任务
	}
	if deadShell {
		return nil, "bridge session was closed (idle timeout or upstream thread ended)"
	}
	return nil, "session expired?"
}

// rebindBridgeSession 把会话改挂到新的会话键：先摘掉指向它的旧键，再按新键登记。
// 用于会话键漂移（客户端换 session_id / 走哈希兜底）后的续跑。
func (h *Handler) rebindBridgeSession(s *bridgeSession, claudeKey string) {
	if s == nil || claudeKey == "" || s.currentClaudeKey() == claudeKey {
		return
	}
	s.setClaudeKey(claudeKey)
	h.bridgeSessMu.Lock()
	if h.bridgeSessions == nil {
		h.bridgeSessions = map[string]*bridgeSession{}
	}
	for k, cur := range h.bridgeSessions {
		if cur == s && k != claudeKey {
			delete(h.bridgeSessions, k)
		}
	}
	h.bridgeSessions[claudeKey] = s
	h.bridgeSessMu.Unlock()
}

// —— ③ 「已发出未收回」的工具调用 ——

// setInflight 记下刚发给客户端的那次工具调用（还没拿到 tool_result）。
func (s *bridgeSession) setInflight(tr rendezvous.ToolRequest) {
	s.mu.Lock()
	cp := tr
	s.inflight = &cp
	s.mu.Unlock()
}

// pendingInflight 返回还欠着结果的那次调用（有则该重发，而不是干等下一个）。
func (s *bridgeSession) pendingInflight() (rendezvous.ToolRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight == nil {
		return rendezvous.ToolRequest{}, false
	}
	return *s.inflight, true
}

// clearInflight 只在 CallID 对得上时清（客户端回的可能是上一轮的旧结果，别误清新的那次）。
func (s *bridgeSession) clearInflight(callID string) {
	s.mu.Lock()
	if s.inflight != nil && s.inflight.CallID == callID {
		s.inflight = nil
	}
	s.mu.Unlock()
}

// noteToolResults 收到一批结果时按 CallID 销账。
func (s *bridgeSession) noteToolResults(results []rendezvous.ToolResult) {
	for _, r := range results {
		s.clearInflight(r.CallID)
	}
}

// —— 兜底回收：超时/断开不再杀会话，就必须有人来收一去不返的那些 ——

// armIdleClose 给「这一轮 HTTP 已经结束、但会话还留着」的情况上一个兜底闹钟：再过 d 还没有
// 新一轮请求来接手，就连同云端 thread 一起收掉。
func (s *bridgeSession) armIdleClose(h *Handler, d time.Duration) {
	s.mu.Lock()
	if s.reaper != nil {
		s.reaper.Stop()
	}
	s.reaper = time.AfterFunc(d, func() { h.closeBridgeSession(s) })
	s.mu.Unlock()
}

// cancelIdleClose 有新一轮请求接手时撤掉闹钟。
func (s *bridgeSession) cancelIdleClose() {
	s.mu.Lock()
	if s.reaper != nil {
		s.reaper.Stop()
		s.reaper = nil
	}
	s.mu.Unlock()
}

// alive 报告会话还在不在。判据用会话自己的 ctx（closeBridgeSession 必调 cancel），
// 不去碰 rendezvous.Session 的内部状态——那边是会合层，桥不该依赖它的实现细节。
func (s *bridgeSession) alive() bool {
	return s != nil && s.ctx != nil && s.ctx.Err() == nil
}

func (s *bridgeSession) currentClaudeKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claudeKey
}

func (s *bridgeSession) setClaudeKey(k string) {
	s.mu.Lock()
	s.claudeKey = k
	s.mu.Unlock()
}

// —— ④ 每条会话同一时刻只允许一轮 HTTP 在等 ——
//
// 客户端自己的 HTTP 超时比我们的单轮上限短时，它会在上一轮还挂着的时候就重发。两轮同时守着
// 同一个 toolReqCh：老那轮（连接已经断了）可能先抢到下一次工具调用，写进一条没人读的响应，
// 还把 inflight 覆盖成这次调用——上一次调用的结果就此永久欠账，云端 thread 卡死到 TTL，
// 客户端再重发就是 409。所以新一轮开工时直接抢占老那轮。

// beginTurn 把本轮登记为会话的当前轮并抢占上一轮，返回本轮的 ctx 与收工函数。
// 抢占只结束老那轮的等待，不动会话本身（会话归新一轮管）。
func (s *bridgeSession) beginTurn() (context.Context, func()) {
	s.mu.Lock()
	if s.turnCancel != nil {
		s.turnCancel()
	}
	s.turnGen++
	gen := s.turnGen
	ctx, cancel := context.WithCancel(s.ctx)
	s.turnCancel = cancel
	s.mu.Unlock()
	return ctx, func() {
		s.mu.Lock()
		if s.turnGen == gen {
			s.turnCancel = nil
		}
		s.mu.Unlock()
		cancel()
	}
}

// —— 指纹的作用域：同一条 prompt 不能跨客户端凭据认亲 ——

// bridgeFingerprintKey 把请求指纹限定在「同一份客户端凭据」内。
// 指纹只看消息条数 + 末条 user 文本，两个客户端发同一条 prompt 就会撞上；不加作用域的话
// recoverByFingerprint 会把 A 的会话交给 B（A 的任务被顶走、B 收到别人的 thread）。
// 会话键会漂，凭据不会——它正好是这里需要的稳定身份。
func bridgeFingerprintKey(c *gin.Context, fp string) string {
	if fp == "" {
		return ""
	}
	return bridgeClientScope(c) + "|" + fp
}

// bridgeClientScope 取客户端凭据的哈希（只用于分桶，不落日志、不回客户端）。
func bridgeClientScope(c *gin.Context) string {
	cred := strings.TrimSpace(c.GetHeader("x-api-key"))
	if cred == "" {
		auth := strings.TrimSpace(c.GetHeader("Authorization"))
		if rest, ok := strings.CutPrefix(auth, "Bearer "); ok {
			cred = strings.TrimSpace(rest)
		} else {
			cred = auth
		}
	}
	hs := fnv.New64a()
	io.WriteString(hs, cred)
	return fmt.Sprintf("cred:%016x", hs.Sum64())
}

// —— 请求指纹：分辨「同一轮重试」与「同一个客户端会话里的新任务」——

// sameRequest 判定这次不带 tool_result 的请求是不是「上一轮 504/断线后的原样重发」。
func (s *bridgeSession) sameRequest(fp string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return fp != "" && fp == s.promptFP
}

// bridgeRequestFingerprint 取「消息条数 + 最后一条 user 文本」的哈希。
// 重发同一轮时两者都不变；同会话里开新任务时消息数或末条文本必变——用它挡住「把新任务误当重试
// 挂到老 thread 上」，也挡住「把重试误当新任务再起一条云端 thread」（双倍计费 + 老 thread 失联）。
func bridgeRequestFingerprint(body []byte) string {
	msgs := gjson.GetBytes(body, "messages")
	if !msgs.IsArray() {
		return ""
	}
	n := 0
	lastUser := ""
	msgs.ForEach(func(_, m gjson.Result) bool {
		n++
		if m.Get("role").String() == "user" {
			if t := bridgeToolResultText(m.Get("content")); t != "" {
				lastUser = t
			}
		}
		return true
	})
	if n == 0 {
		return ""
	}
	if len(lastUser) > 512 {
		lastUser = lastUser[:512]
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "n:%d|last:%s", n, lastUser)
	return fmt.Sprintf("fp:%016x", h.Sum64())
}
