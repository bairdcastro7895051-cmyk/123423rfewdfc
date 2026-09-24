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
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"kiro-proxy/internal/hopbridge/rendezvous"

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
func (h *Handler) recoverByFingerprint(claudeKey, fp string) *bridgeSession {
	s := bridgeCallIndex.getByFingerprint(fp)
	if s == nil || !s.alive() {
		return nil
	}
	h.rebindBridgeSession(s, claudeKey)
	return s
}

// —— ② 会话找回 ——

// resolveBridgeSession 给一批 tool_result 找回它们所属的桥会话。
// 先按会话键查；查不到（或查到的是已被回收的空壳）就按 tool_use id 反查，命中即把会话改挂到
// 当前会话键上（键漂移后续轮才继续粘得住）。都没有 → (nil, 给客户端看的原因)。
func (h *Handler) resolveBridgeSession(claudeKey string, results []rendezvous.ToolResult) (*bridgeSession, string) {
	deadShell := false
	if s := h.lookupBridgeSession(claudeKey); s != nil {
		if s.alive() {
			return s, ""
		}
		deadShell = true
		h.closeBridgeSession(s) // 空壳先摘掉，免得挡住后面的新任务
	}
	for _, r := range results {
		if s := bridgeCallIndex.get(r.CallID); s != nil {
			if s.alive() {
				h.rebindBridgeSession(s, claudeKey)
				return s, ""
			}
			deadShell = true
		}
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
