package proxy

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"kiro-proxy/internal/hopbridge/rendezvous"
)

// newBridgeRecoverTestSession 造一条挂在 hub 上的桥会话（不起 thread，只要会合槽和登记）。
func newBridgeRecoverTestSession(t *testing.T, h *Handler, claudeKey string) *bridgeSession {
	t.Helper()
	bridgeKey := "br_" + claudeKey
	ctx, cancel := context.WithCancel(context.Background())
	s := &bridgeSession{
		bridgeKey: bridgeKey,
		claudeKey: claudeKey,
		rz:        h.bridgeHub.OpenSession(bridgeKey),
		toolReqCh: make(chan rendezvous.ToolRequest, 1),
		finalCh:   make(chan bridgeFinal, 1),
		ctx:       ctx,
		cancel:    cancel,
	}
	h.storeBridgeSession(claudeKey, s)
	return s
}

func newBridgeRecoverTestHandler() *Handler {
	return &Handler{bridgeBinder: newBridgeBinder(), bridgeHub: rendezvous.New(time.Minute)}
}

// 会话键漂移（客户端换了 session_id / 走哈希兜底）后，靠我们自己发出去的 tool_use id 把会话找回来，
// 并改挂到新键上——这是 409 的第二条成因。
func TestResolveBridgeSessionByToolUseID(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:old")
	bridgeCallIndex.put("toolu_1", sess)
	defer h.closeBridgeSession(sess)

	got, why := h.resolveBridgeSession("claude:new", []rendezvous.ToolResult{{CallID: "toolu_1"}})
	if got != sess {
		t.Fatalf("按 CallID 没找回会话: got=%v why=%q", got, why)
	}
	if h.lookupBridgeSession("claude:new") != sess {
		t.Fatal("找回后没改挂到新会话键")
	}
	if h.lookupBridgeSession("claude:old") != nil {
		t.Fatal("旧键残留，会挡住同会话的新任务")
	}
	if sess.currentClaudeKey() != "claude:new" {
		t.Fatalf("会话自身的键没更新: %s", sess.currentClaudeKey())
	}
}

// 会话真的被回收时才该报错，且原因要能区分「关了」与「从来没有过」。
func TestResolveBridgeSessionDeadShell(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k")
	sess.cancel() // 模拟被兜底闹钟/thread 终态收掉后的空壳（表项还在）

	got, why := h.resolveBridgeSession("claude:k", []rendezvous.ToolResult{{CallID: "toolu_x"}})
	if got != nil {
		t.Fatal("已关闭的会话不该被当成可用")
	}
	if why == "" || why == "session expired?" {
		t.Fatalf("原因没区分「已关闭」: %q", why)
	}
	if h.bridgeSessionCount() != 0 {
		t.Fatalf("死壳没从表里摘掉: %d", h.bridgeSessionCount())
	}

	got2, why2 := h.resolveBridgeSession("claude:none", []rendezvous.ToolResult{{CallID: "toolu_y"}})
	if got2 != nil || why2 != "session expired?" {
		t.Fatalf("从来没有过的会话应回默认原因: got=%v why=%q", got2, why2)
	}
}

// 「已发出未收回」的调用要能原样重发；客户端回的若是别的 CallID（上一轮的旧结果）不能误销账。
func TestBridgeInflightReplayAndClear(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k")
	defer h.closeBridgeSession(sess)

	if _, ok := sess.pendingInflight(); ok {
		t.Fatal("新会话不该有欠账")
	}
	sess.setInflight(rendezvous.ToolRequest{CallID: "toolu_2", ToolName: "local_fs_read"})
	tr, ok := sess.pendingInflight()
	if !ok || tr.CallID != "toolu_2" {
		t.Fatalf("欠账没记住: %+v ok=%v", tr, ok)
	}
	sess.noteToolResults([]rendezvous.ToolResult{{CallID: "toolu_1"}})
	if _, ok := sess.pendingInflight(); !ok {
		t.Fatal("被上一轮的旧结果误销账了")
	}
	sess.noteToolResults([]rendezvous.ToolResult{{CallID: "toolu_2"}})
	if _, ok := sess.pendingInflight(); ok {
		t.Fatal("对上号的结果没销账，会无限重发同一次调用")
	}
}

// 指纹要能分开「同一轮原样重发」与「同会话里的新任务」。
func TestBridgeRequestFingerprint(t *testing.T) {
	mk := func(msgs ...map[string]any) []byte {
		b, _ := json.Marshal(map[string]any{"messages": msgs})
		return b
	}
	retry := mk(map[string]any{"role": "user", "content": "改一下 A 文件"})
	same := mk(map[string]any{"role": "user", "content": "改一下 A 文件"})
	newTask := mk(map[string]any{"role": "user", "content": "改一下 B 文件"})
	longer := mk(
		map[string]any{"role": "user", "content": "改一下 A 文件"},
		map[string]any{"role": "assistant", "content": "好"},
		map[string]any{"role": "user", "content": "改一下 A 文件"},
	)

	if bridgeRequestFingerprint(retry) != bridgeRequestFingerprint(same) {
		t.Fatal("原样重发的指纹必须相同，否则会再起一条云端 thread")
	}
	if bridgeRequestFingerprint(retry) == bridgeRequestFingerprint(newTask) {
		t.Fatal("换了任务指纹必须变，否则新任务会被挂到老 thread 上")
	}
	if bridgeRequestFingerprint(retry) == bridgeRequestFingerprint(longer) {
		t.Fatal("消息条数变了指纹必须变")
	}
	if bridgeRequestFingerprint([]byte(`{}`)) != "" {
		t.Fatal("没有 messages 时应回空指纹（不参与判定）")
	}
	// 块数组形态的 content（Claude Code 常见形状）也要能取到文本。
	blocks, _ := json.Marshal(map[string]any{"messages": []map[string]any{
		{"role": "user", "content": []map[string]any{{"type": "text", "text": "改一下 A 文件"}}},
	}})
	if bridgeRequestFingerprint(blocks) == "" {
		t.Fatal("块数组 content 没取到文本")
	}
}

// 会话关闭要把它名下的 CallID 索引一起清掉，否则索引会常驻泄漏。
func TestBridgeCallIndexDroppedOnClose(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k")
	bridgeCallIndex.put("toolu_3", sess)
	h.closeBridgeSession(sess)
	if bridgeCallIndex.get("toolu_3") != nil {
		t.Fatal("会话关闭后 CallID 索引没清")
	}
	if h.bridgeSessionCount() != 0 {
		t.Fatal("会话关闭后没从表里摘掉")
	}
}

// 超时分支不再杀会话，配套的兜底闹钟必须真的会收：到点没人接手 → 会话连同索引一起清掉；
// 有人接手（新一轮请求）→ 闹钟撤掉，会话留着。
func TestBridgeIdleCloseArmAndCancel(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k")
	sess.armIdleClose(h, 10*time.Millisecond)
	time.Sleep(100 * time.Millisecond)
	if sess.alive() || h.bridgeSessionCount() != 0 {
		t.Fatal("兜底闹钟没收掉无人接手的会话")
	}

	sess2 := newBridgeRecoverTestSession(t, h, "claude:k2")
	defer h.closeBridgeSession(sess2)
	sess2.armIdleClose(h, 10*time.Millisecond)
	sess2.cancelIdleClose()
	time.Sleep(100 * time.Millisecond)
	if !sess2.alive() {
		t.Fatal("已被接手的会话不该被闹钟收掉")
	}
}

// 会话键漂移后按 id 找回、又重发欠账调用——两条修复要能串起来跑（409 的主链路）。
func TestBridgeRecoverAcrossKeyDriftKeepsInflight(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k1")
	defer h.closeBridgeSession(sess)
	sess.setInflight(rendezvous.ToolRequest{CallID: "toolu_9", ToolName: "local_fs_write"})
	bridgeCallIndex.put("toolu_9", sess)

	got, _ := h.resolveBridgeSession("claude:k2", []rendezvous.ToolResult{{CallID: "toolu_9", Content: "ok"}})
	if got != sess {
		t.Fatal("漂移后没找回会话")
	}
	got.noteToolResults([]rendezvous.ToolResult{{CallID: "toolu_9", Content: "ok"}})
	if _, ok := got.pendingInflight(); ok {
		t.Fatal("结果已回，欠账该销掉")
	}
}
