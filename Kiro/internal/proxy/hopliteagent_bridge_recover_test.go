package proxy

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"kiro-proxy/internal/hopbridge/rendezvous"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

// newRecoverTestSession 造一条不带 rendezvous 会话的壳：本文件的用例只走找回层，不碰 rz。
func newRecoverTestSession(claudeKey string) *bridgeSession {
	return &bridgeSession{bridgeKey: "br_test_" + claudeKey, claudeKey: claudeKey, cancel: func() {}}
}

func resetBridgeCalls() {
	bridgeCalls.mu.Lock()
	bridgeCalls.m = map[string]*bridgeSession{}
	bridgeCalls.mu.Unlock()
}

func TestResolveBridgeSession_RecoversAfterKeyDrift(t *testing.T) {
	resetBridgeCalls()
	h := &Handler{}
	sess := newRecoverTestSession("key-old")
	h.storeBridgeSession("key-old", sess)
	trackBridgeCall(sess, rendezvous.ToolRequest{CallID: "toolu_1", ToolName: "local_fs_read"})

	got, msg := h.resolveBridgeSessionForResults("key-new", []rendezvous.ToolResult{{CallID: "toolu_1"}})
	if got != sess || msg != "" {
		t.Fatalf("want recovered session, got %v msg=%q", got, msg)
	}
	// 改挂到新键，老键按值删干净（否则留悬空项，close 时删不掉）。
	if h.lookupBridgeSession("key-new") != sess || h.lookupBridgeSession("key-old") != nil {
		t.Fatalf("rebind failed: new=%v old=%v", h.lookupBridgeSession("key-new"), h.lookupBridgeSession("key-old"))
	}
	if sess.claudeKey != "key-new" || h.bridgeSessionCount() != 1 {
		t.Fatalf("claudeKey=%s count=%d", sess.claudeKey, h.bridgeSessionCount())
	}
}

func TestResolveBridgeSession_ClosedVsUnknown(t *testing.T) {
	resetBridgeCalls()
	h := &Handler{}
	sess := newRecoverTestSession("k")
	trackBridgeCall(sess, rendezvous.ToolRequest{CallID: "toolu_closed"})
	sess.markClosed()

	if got, msg := h.resolveBridgeSessionForResults("k", []rendezvous.ToolResult{{CallID: "toolu_closed"}}); got != nil || msg != bridgeSessionClosedMsg {
		t.Fatalf("closed: got %v msg=%q", got, msg)
	}
	if got, msg := h.resolveBridgeSessionForResults("k", []rendezvous.ToolResult{{CallID: "toolu_never"}}); got != nil || msg != bridgeSessionGoneMsg {
		t.Fatalf("unknown: got %v msg=%q", got, msg)
	}
}

func TestSettleCalls_ByCallID(t *testing.T) {
	sess := newRecoverTestSession("k")
	trackBridgeCall(sess, rendezvous.ToolRequest{CallID: "toolu_2"})
	// 迟到的旧结果不许销掉当前在飞的调用。
	sess.settleCalls([]rendezvous.ToolResult{{CallID: "toolu_1"}})
	if _, ok := sess.inflightCall(); !ok {
		t.Fatal("stale result cleared the inflight call")
	}
	sess.settleCalls([]rendezvous.ToolResult{{CallID: "toolu_2"}})
	if _, ok := sess.inflightCall(); ok {
		t.Fatal("inflight not settled by its own CallID")
	}
}

func TestTrackBridgeCall_EvictsBeyondRetain(t *testing.T) {
	resetBridgeCalls()
	sess := newRecoverTestSession("k")
	for i := 0; i < bridgeCallRetain+5; i++ {
		trackBridgeCall(sess, rendezvous.ToolRequest{CallID: "toolu_" + string(rune('a'+i%26)) + string(rune('0'+i/26))})
	}
	bridgeCalls.mu.Lock()
	n := len(bridgeCalls.m)
	bridgeCalls.mu.Unlock()
	if n > bridgeCallRetain {
		t.Fatalf("call index unbounded: %d entries", n)
	}
}

func TestDropBridgeCallsAndEntries(t *testing.T) {
	resetBridgeCalls()
	h := &Handler{}
	sess := newRecoverTestSession("k1")
	h.storeBridgeSession("k1", sess)
	h.storeBridgeSession("k2", sess) // 模拟 rebind 留下的第二个键
	trackBridgeCall(sess, rendezvous.ToolRequest{CallID: "toolu_x"})

	dropBridgeCalls(sess)
	h.dropBridgeSessionEntries(sess)
	if lookupBridgeCall("toolu_x") != nil {
		t.Fatal("call index not cleared on close")
	}
	if h.bridgeSessionCount() != 0 {
		t.Fatalf("session entries not deleted by value: %d left", h.bridgeSessionCount())
	}
}

func TestBridgeRequestFingerprint_SplitsResendFromNewTask(t *testing.T) {
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"改 A 文件"}]}]}`)
	resend := []byte(`{"messages":[{"role":"user","content":"改 A 文件"}]}`)
	newTask := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"改 B 文件"}]}]}`)

	sess := newRecoverTestSession("k")
	sess.fp = bridgeRequestFingerprint(body)
	if !sameBridgeTask(sess, bridgeRequestFingerprint(resend)) {
		t.Fatal("same prompt must be treated as a resend (else we start a second cloud thread)")
	}
	if sameBridgeTask(sess, bridgeRequestFingerprint(newTask)) {
		t.Fatal("different prompt must be treated as a new task")
	}
}

func TestResumeExistingBridgeSession_ResendsInflightCall(t *testing.T) {
	resetBridgeCalls()
	gin.SetMode(gin.TestMode)
	h := &Handler{}
	sess := newRecoverTestSession("k")
	h.storeBridgeSession("k", sess)
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"任务"}]}]}`)
	sess.fp = bridgeRequestFingerprint(body)
	args, _ := json.Marshal(map[string]string{"file": "a.go"})
	trackBridgeCall(sess, rendezvous.ToolRequest{CallID: "toolu_keep", ToolName: "local_fs_read", Arguments: args})

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	if !h.resumeExistingBridgeSession(c, sess, body, "hoplitebridge/gpt-5.6-terra", false) {
		t.Fatal("resend of the same prompt must be handled as a resume")
	}
	if rec.Code != 200 {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	// 必须原样重发同一个 CallID，否则云端 thread 永远等不到那次结果。
	if id := gjson.Get(rec.Body.String(), "content.0.id").String(); id != "toolu_keep" {
		t.Fatalf("resent call id = %q, want toolu_keep", id)
	}
	if stop := gjson.Get(rec.Body.String(), "stop_reason").String(); stop != "tool_use" {
		t.Fatalf("stop_reason=%q", stop)
	}
}

func TestTouchBridgeSession_ArmsAndStopsIdleAlarm(t *testing.T) {
	old := bridgeIdleFloor
	bridgeIdleFloor = 30 * time.Millisecond
	defer func() { bridgeIdleFloor = old }()

	h := &Handler{}
	sess := newRecoverTestSession("k")
	h.touchBridgeSession(sess)
	sess.mu.Lock()
	armed := sess.idle != nil
	sess.mu.Unlock()
	if !armed {
		t.Fatal("idle alarm not armed")
	}
	// 关会话必须撤掉闹钟，否则会话关了闹钟还在空转。
	sess.markClosed()
	sess.mu.Lock()
	stopped := sess.idle == nil
	sess.mu.Unlock()
	if !stopped {
		t.Fatal("idle alarm not disarmed on close")
	}
	// 已关的会话不再重新上闹钟。
	h.touchBridgeSession(sess)
	sess.mu.Lock()
	rearmed := sess.idle != nil
	sess.mu.Unlock()
	if rearmed {
		t.Fatal("closed session must not re-arm the idle alarm")
	}
}
