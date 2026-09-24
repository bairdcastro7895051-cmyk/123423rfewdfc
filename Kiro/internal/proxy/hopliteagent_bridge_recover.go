package proxy

// hopliteagent_bridge_recover.go —— 桥会话「找回层」：治 409 no active bridge session。
//
// 409 的三条成因，全部在本文件处理，serve 侧只留调用点：
//  ① 会话键漂移：会话按 coreauth 探测键存，客户端压缩上下文 / 换 metadata.user_id 时键会变，
//     老会话还活着却查不到 → 用「我们自己发出去的 tool_use id」反查，命中即改挂到当前键。
//  ② tool_use 半路丢：云端 thread 卡在那次 MCP 调用上等结果，provider 侧干等「下一个」调用 →
//     会话记住 inflight，客户端不带 tool_result 重发时原样重发同一个 CallID。
//  ③ 不带 tool_result 的重发被当成新任务 → 再起一条云端 thread（双倍计费 + 老 thread 永久失联）
//     → 按请求指纹分流：同指纹 = 续跑老会话，不同 = 新任务（先关老的）。
//
// 另配兜底闹钟：单轮 HTTP 超时/断开不再终结会话（那是 409 的头号来源），需要一个 idle 闹钟
// 收掉「一去不返」的会话，否则 thread/pump 两个 goroutine 常驻。

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"sync"
	"time"

	"kiro-proxy/internal/hopbridge/rendezvous"
	hoplitert "kiro-proxy/internal/runtime/hoplite"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// bridgeCallRetain 每条会话在反查索引里保留的最近 callID 数：够覆盖重发，又不让索引无界增长。
const bridgeCallRetain = 32

// bridgeIdleFloor 兜底闹钟下限；实际取 max(3×agentMaxWait, 本值)。单测会调小它。
var bridgeIdleFloor = 10 * time.Minute

// 409 的两种终态文案：区分「有过但已关」与「从没有过」，排障时一眼分得清。
const (
	bridgeSessionClosedMsg = "bridge session was closed (idle timeout or upstream thread ended)"
	bridgeSessionGoneMsg   = "no active bridge session for these tool results (session expired?)"
)

// bridgeCalls：tool_use id -> 会话。进程级单例（与 bridgeAttachments 同路数，不动 Handler 结构体）。
var bridgeCalls = struct {
	mu sync.Mutex
	m  map[string]*bridgeSession
}{m: map[string]*bridgeSession{}}

// trackBridgeCall 登记一次发给客户端的 tool_use：记 inflight（未收回）+ 进反查索引。
func trackBridgeCall(sess *bridgeSession, tr rendezvous.ToolRequest) {
	if sess == nil || tr.CallID == "" {
		return
	}
	sess.mu.Lock()
	inflight := tr
	sess.inflight = &inflight
	sess.callIDs = append(sess.callIDs, tr.CallID)
	var evicted []string
	if n := len(sess.callIDs) - bridgeCallRetain; n > 0 {
		evicted = append(evicted, sess.callIDs[:n]...)
		sess.callIDs = append([]string(nil), sess.callIDs[n:]...)
	}
	sess.mu.Unlock()

	bridgeCalls.mu.Lock()
	bridgeCalls.m[tr.CallID] = sess
	for _, id := range evicted {
		if bridgeCalls.m[id] == sess {
			delete(bridgeCalls.m, id)
		}
	}
	bridgeCalls.mu.Unlock()
}

// lookupBridgeCall 按 tool_use id 反查会话。
func lookupBridgeCall(callID string) *bridgeSession {
	if callID == "" {
		return nil
	}
	bridgeCalls.mu.Lock()
	defer bridgeCalls.mu.Unlock()
	return bridgeCalls.m[callID]
}

// dropBridgeCalls 会话关闭时清掉它的索引项（只删仍指向自己的，避免误删改挂后的新主）。
func dropBridgeCalls(sess *bridgeSession) {
	sess.mu.Lock()
	ids := sess.callIDs
	sess.callIDs = nil
	sess.mu.Unlock()
	bridgeCalls.mu.Lock()
	for _, id := range ids {
		if bridgeCalls.m[id] == sess {
			delete(bridgeCalls.m, id)
		}
	}
	bridgeCalls.mu.Unlock()
}

// settleCalls 按 CallID 销账：只有收回的正是 inflight 那次才清，旧结果不会误清新调用。
func (s *bridgeSession) settleCalls(results []rendezvous.ToolResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight == nil {
		return
	}
	for _, r := range results {
		if r.CallID == s.inflight.CallID {
			s.inflight = nil
			return
		}
	}
}

// inflightCall 取「已发出未收回」的那次调用。
func (s *bridgeSession) inflightCall() (rendezvous.ToolRequest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inflight == nil {
		return rendezvous.ToolRequest{}, false
	}
	return *s.inflight, true
}

func (s *bridgeSession) markClosed() {
	s.mu.Lock()
	s.closed = true
	if s.idle != nil {
		s.idle.Stop()
		s.idle = nil
	}
	s.mu.Unlock()
}

func (s *bridgeSession) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// resolveBridgeSessionForResults 三层找回：当前键 → tool_use id 反查（命中则改挂当前键）→ 放弃回 409。
// 第二个返回值为空表示找回成功；非空是给客户端的 409 文案。
func (h *Handler) resolveBridgeSessionForResults(claudeKey string, results []rendezvous.ToolResult) (*bridgeSession, string) {
	if sess := h.lookupBridgeSession(claudeKey); sess != nil {
		return sess, ""
	}
	closedSeen := false
	for _, r := range results {
		sess := lookupBridgeCall(r.CallID)
		if sess == nil {
			continue
		}
		if sess.isClosed() {
			closedSeen = true
			continue
		}
		log.Warnf("proxy: hopbridge session key drift %s -> %s (recovered by call %s) bridgeKey=%s",
			sess.claudeKey, claudeKey, r.CallID, sess.bridgeKey)
		h.rebindBridgeSession(sess, claudeKey)
		return sess, ""
	}
	if closedSeen {
		log.Warnf("proxy: hopbridge 409 (closed session) key=%s calls=%v live=%d",
			claudeKey, bridgeResultCallIDs(results), h.bridgeSessionCount())
		return nil, bridgeSessionClosedMsg
	}
	log.Warnf("proxy: hopbridge 409 (unknown session) key=%s calls=%v live=%d",
		claudeKey, bridgeResultCallIDs(results), h.bridgeSessionCount())
	return nil, bridgeSessionGoneMsg
}

// rebindBridgeSession 把会话从老键改挂到当前键（老键按值删，避免留悬空项）。
func (h *Handler) rebindBridgeSession(sess *bridgeSession, claudeKey string) {
	if sess == nil || claudeKey == "" || sess.claudeKey == claudeKey {
		return
	}
	h.bridgeSessMu.Lock()
	if h.bridgeSessions == nil {
		h.bridgeSessions = map[string]*bridgeSession{}
	}
	for k, v := range h.bridgeSessions {
		if v == sess {
			delete(h.bridgeSessions, k)
		}
	}
	sess.claudeKey = claudeKey
	h.bridgeSessions[claudeKey] = sess
	h.bridgeSessMu.Unlock()
}

// dropBridgeSessionEntries 按**值**删会话表项：键可能被 rebind 改过，按旧键删会留悬空项。
func (h *Handler) dropBridgeSessionEntries(sess *bridgeSession) {
	h.bridgeSessMu.Lock()
	for k, v := range h.bridgeSessions {
		if v == sess {
			delete(h.bridgeSessions, k)
		}
	}
	h.bridgeSessMu.Unlock()
}

func (h *Handler) bridgeSessionCount() int {
	h.bridgeSessMu.Lock()
	defer h.bridgeSessMu.Unlock()
	return len(h.bridgeSessions)
}

func bridgeResultCallIDs(results []rendezvous.ToolResult) []string {
	ids := make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.CallID)
	}
	return ids
}

// bridgeRequestFingerprint 认「同一个任务」的指纹：消息条数 + 末条 user 文本哈希。
// 客户端 504 后原样重发 → 指纹相同；换了新任务 → 指纹不同。
func bridgeRequestFingerprint(body []byte) string {
	msgs := gjson.GetBytes(body, "messages")
	n := 0
	last := ""
	if msgs.IsArray() {
		arr := msgs.Array()
		n = len(arr)
		for i := len(arr) - 1; i >= 0; i-- {
			if arr[i].Get("role").String() != "user" {
				continue
			}
			last = hoplitert.FlattenAnthropic([]byte(`{"messages":[` + arr[i].Raw + `]}`))
			break
		}
	}
	sum := sha256.Sum256([]byte(last))
	return strconv.Itoa(n) + ":" + hex.EncodeToString(sum[:8])
}

// resumeExistingBridgeSession 处理「按键找到了老会话、但本轮不带 tool_result」的重发。
// 返回 false 表示这是个**新任务**（指纹不同，老会话已关），调用方继续走 turn-1。
func (h *Handler) resumeExistingBridgeSession(c *gin.Context, sess *bridgeSession, body []byte, model string, stream bool) bool {
	if !sameBridgeTask(sess, bridgeRequestFingerprint(body)) {
		log.Infof("proxy: hopbridge new task on same key (fingerprint changed) bridgeKey=%s", sess.bridgeKey)
		h.closeBridgeSession(sess)
		return false
	}
	h.touchBridgeSession(sess)
	bridgeAttachments.Put(sess.bridgeKey, hoplitert.ExtractAttachments(body))
	// tool_use 半路丢：云端 thread 还卡在那次调用上等结果，原样重发同一个 CallID。
	if tr, ok := sess.inflightCall(); ok {
		log.Infof("proxy: hopbridge resend inflight call %s tool=%s bridgeKey=%s", tr.CallID, tr.ToolName, sess.bridgeKey)
		h.writeBridgeInflight(c, sess, tr, model, stream)
		return true
	}
	log.Infof("proxy: /v1/messages provider=hoplite-bridge resume parked session bridgeKey=%s", sess.bridgeKey)
	h.bridgeNextTurn(c, sess, model, stream)
	return true
}

// sameBridgeTask 判定本次重发是不是还在跑同一个任务（老会话没记指纹时从宽认同）。
func sameBridgeTask(sess *bridgeSession, fp string) bool {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.fp == "" || sess.fp == fp
}

// touchBridgeSession 每轮 HTTP 进来时重置兜底闹钟。
func (h *Handler) touchBridgeSession(sess *bridgeSession) {
	if sess == nil {
		return
	}
	ttl := 3 * h.agentMaxWait()
	if ttl < bridgeIdleFloor {
		ttl = bridgeIdleFloor
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.closed {
		return
	}
	if sess.idle != nil {
		sess.idle.Reset(ttl)
		return
	}
	sess.idle = time.AfterFunc(ttl, func() {
		log.Warnf("proxy: hopbridge session idle timeout, closing bridgeKey=%s", sess.bridgeKey)
		h.closeBridgeSession(sess)
	})
}
