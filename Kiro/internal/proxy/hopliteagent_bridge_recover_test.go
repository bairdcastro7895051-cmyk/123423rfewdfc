package proxy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"kiro-proxy/internal/hopbridge/rendezvous"

	"github.com/gin-gonic/gin"
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

// 不带 tool_result 的原样重发 + 会话键同时漂了：只剩指纹能认人，认回来才不会再起一条云端 thread。
func TestRecoverByFingerprintOnDriftedKey(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k1")
	defer h.closeBridgeSession(sess)
	body := []byte(`{"messages":[{"role":"user","content":[{"type":"text","text":"修 409"}]}]}`)
	fp := bridgeRequestFingerprint(body)
	sess.promptFP = fp
	bridgeCallIndex.putFingerprint(fp, sess)

	if got := h.recoverByFingerprint("claude:k2", fp); got != sess {
		t.Fatal("指纹没认回漂移后的会话")
	}
	if h.lookupBridgeSession("claude:k2") != sess || h.lookupBridgeSession("claude:k1") != nil {
		t.Fatal("会话没改挂到新键上")
	}
	// 换了任务的指纹不该认回任何会话。
	if got := h.recoverByFingerprint("claude:k3", bridgeRequestFingerprint([]byte(`{"messages":[{"role":"user","content":"换个任务"}]}`))); got != nil {
		t.Fatal("不同指纹不该命中")
	}
	// 会话关掉后指纹索引要一起清，死壳不能被捡回来。
	h.closeBridgeSession(sess)
	if got := h.recoverByFingerprint("claude:k4", fp); got != nil {
		t.Fatal("关闭后指纹索引没清")
	}
}

// 会话键漂走之后，同一个键上可能已经挂了另一条活会话。这批结果必须按 CallID 投给真正的主人，
// 而不是按键投给那条新会话——投错的话真主人继续干等、错收方认不出 CallID 直接丢掉，双边超时。
func TestResolveBridgeSessionCallIDBeatsKeyCollision(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	owner := newBridgeRecoverTestSession(t, h, "claude:owner")
	defer h.closeBridgeSession(owner)
	bridgeCallIndex.put("toolu_own", owner)

	// 另一条活会话抢占了客户端这次用的键（键漂移的典型后果）。
	squatter := newBridgeRecoverTestSession(t, h, "claude:drifted")
	defer h.closeBridgeSession(squatter)

	got, why := h.resolveBridgeSession("claude:drifted", []rendezvous.ToolResult{{CallID: "toolu_own"}})
	if got != owner {
		t.Fatalf("结果被投给了键上那条会话而不是 CallID 的主人: got=%v why=%q", got, why)
	}
	if h.lookupBridgeSession("claude:drifted") != owner {
		t.Fatal("找回后没改挂到当前键")
	}
	if !squatter.alive() {
		t.Fatal("被挤下键表的会话不该被顺手杀掉（它还得靠指纹/CallID 认回）")
	}
}

// 客户端在上一轮还挂着的时候就重发（它自己的 HTTP 超时更短）：新一轮必须抢占老那轮，
// 否则两轮同时守着 toolReqCh，老那轮会把下一次调用写进没人读的响应并覆盖 inflight。
func TestBridgeTurnPreemptsPreviousTurn(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k")
	defer h.closeBridgeSession(sess)

	first, endFirst := sess.beginTurn()
	second, endSecond := sess.beginTurn()
	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("老那轮没被抢占，会和新一轮抢同一个工具请求")
	}
	if second.Err() != nil {
		t.Fatal("新一轮不该被自己的抢占动作带走")
	}
	endFirst() // 老那轮收工不该影响当前轮的登记
	sess.mu.Lock()
	cur := sess.turnCancel
	sess.mu.Unlock()
	if cur == nil {
		t.Fatal("老那轮收工把当前轮的登记清掉了")
	}
	endSecond()
	if second.Err() == nil {
		t.Fatal("收工没结束本轮 ctx")
	}
	sess.mu.Lock()
	cur = sess.turnCancel
	sess.mu.Unlock()
	if cur != nil {
		t.Fatal("当前轮收工后没清登记")
	}
}

// 会话被关掉时，正在等的那一轮也要醒（否则要挂到单轮上限才返回）。
func TestBridgeTurnEndsWhenSessionCloses(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:k")
	turn, end := sess.beginTurn()
	defer end()
	h.closeBridgeSession(sess)
	select {
	case <-turn.Done():
	case <-time.After(time.Second):
		t.Fatal("会话关了，在等的那一轮没醒")
	}
	if sess.alive() {
		t.Fatal("关掉的会话仍报活着")
	}
}

// 指纹只看「消息条数 + 末条 user 文本」，两个客户端发同一条 prompt 必然撞；
// 认亲必须限定在同一份客户端凭据内，否则 A 的会话会被交给 B。
func TestBridgeFingerprintScopedToClientCredential(t *testing.T) {
	gin.SetMode(gin.TestMode)
	mkCtx := func(key string) *gin.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		c.Request.Header.Set("x-api-key", key)
		return c
	}
	a, b := mkCtx("sk-aaa"), mkCtx("sk-bbb")
	fp := "fp:deadbeef"
	if bridgeFingerprintKey(a, fp) == bridgeFingerprintKey(b, fp) {
		t.Fatal("不同凭据的同一条 prompt 落进了同一个桶")
	}
	if bridgeFingerprintKey(a, fp) != bridgeFingerprintKey(mkCtx("sk-aaa"), fp) {
		t.Fatal("同一份凭据的桶必须稳定，否则键漂之后认不回来")
	}
	if bridgeFingerprintKey(a, "") != "" {
		t.Fatal("空指纹不该参与认亲")
	}
	// Authorization: Bearer 与 x-api-key 两种写法都要能取到凭据。
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	c.Request.Header.Set("Authorization", "Bearer sk-aaa")
	if bridgeFingerprintKey(c, fp) != bridgeFingerprintKey(a, fp) {
		t.Fatal("Bearer 头没取到与 x-api-key 相同的凭据")
	}

	h := newBridgeRecoverTestHandler()
	sess := newBridgeRecoverTestSession(t, h, "claude:owner")
	defer h.closeBridgeSession(sess)
	bridgeCallIndex.putFingerprint(bridgeFingerprintKey(a, fp), sess)
	if got := h.recoverByFingerprint("claude:other", bridgeFingerprintKey(b, fp)); got != nil {
		t.Fatal("别人的凭据按指纹认走了这条会话")
	}
	if got := h.recoverByFingerprint("claude:drifted", bridgeFingerprintKey(a, fp)); got != sess {
		t.Fatal("同一份凭据键漂之后没认回自己的会话")
	}
}

// 请求体里带着整段历史的 tool_result：靠前那些可能属于同一客户端早先那条还活着的会话，
// 末尾那条才是这一轮真正要投递的调用——认主人必须从最后一条往前找。
func TestResolveBridgeSessionPrefersNewestResult(t *testing.T) {
	h := newBridgeRecoverTestHandler()
	stale := newBridgeRecoverTestSession(t, h, "claude:stale")
	defer h.closeBridgeSession(stale)
	current := newBridgeRecoverTestSession(t, h, "claude:current")
	defer h.closeBridgeSession(current)
	bridgeCallIndex.put("toolu_old", stale)
	bridgeCallIndex.put("toolu_new", current)

	got, why := h.resolveBridgeSession("claude:current", []rendezvous.ToolResult{
		{CallID: "toolu_old"}, {CallID: "toolu_new"},
	})
	if got != current {
		t.Fatalf("按历史里的老 CallID 投给了早先那条会话: got=%v why=%q", got, why)
	}
}
