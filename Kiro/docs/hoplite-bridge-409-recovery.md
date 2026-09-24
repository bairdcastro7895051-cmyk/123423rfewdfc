# Hoplite 桥：`409 no active bridge session` 的成因与找回层

> 代码：`internal/proxy/hopliteagent_bridge_recover.go`（找回层全部逻辑）、
> `internal/proxy/hopliteagent_bridge_serve.go`（只留调用点）。桥的总体架构见两个源文件头注释。

## 1. 409 从哪来

全仓只有一处产生它：客户端回送 `tool_result` 时，按会话键 `agentSessionKey(c, body)`
在 `h.bridgeSessions` 里查不到 `bridgeSession`。四条真实成因，**都不是「会话真过期」**：

| # | 成因 | 现象 |
|---|---|---|
| ① | **自己把会话杀了**：`bridgeNextTurn` 本轮 HTTP 等超时就 `closeBridgeSession` | 客户端常规重发 → 409。**头号来源** |
| ② | **会话键漂移**：客户端压缩上下文、换 `metadata.user_id`、或落到 `agentSessionKey` 哈希兜底（哈希含首条 user/assistant 文本） | 会话还活着，键变了 → 409 |
| ③ | **`tool_use` 半路丢了**（客户端断线/本轮 504） | 云端 thread 卡在那次 MCP 调用上等结果，provider 侧重来后干等「下一个」请求，两边互等到超时 |
| ④ | **不带 `tool_result` 的重发被当成新任务** | 再起一条云端 thread：两个 waiter 抢同一个 MCP 会话 + 双倍计费 + 老 thread 永久失联（它后续 MCP 全 503）→ 绕回 409 |
| ⑤ | **两轮 HTTP 同时守着一条会话**：客户端自己的超时比我们的单轮上限短，上一轮还挂着它就重发 | 老那轮（连接已断）抢到下一次工具调用写进没人读的响应，还把 `inflight` 覆盖掉 → 上一次调用永久欠账、thread 卡死 → 再重发就是 409 |

## 2. 找回层做了什么

1. **超时只结束这一轮 HTTP（治 ①）** —— 会话、thread、未收回的调用全留着，回 504 且文案写明
   `session kept, resend to resume`。配套：无人接手时由 `armIdleClose` 兜底闹钟收
   （`bridgeIdleTTL` = 3×`agentMaxWait`，下限 10 分钟），新一轮请求进来即 `cancelIdleClose`。
2. **按 `tool_use id` 反查（治 ②）** —— 包级 `bridgeCallIndex` 记「我们发出去的 CallID → 会话」。
   `resolveBridgeSession` **先按 id 反查、键查在后**，命中即 `rebindBridgeSession` 改挂到当前键上，
   后续轮继续粘住。次序是刻意的：CallID 由我们自己铸、全局唯一，指向的一定是这批结果真正的主人；
   会话键会漂，而漂走后这个键上可能已经挂了**另一条活会话**——按键投递就把结果投给了错的会话
   （真主人继续干等，错收方认不出 CallID 直接丢掉，两边一起等到超时）。撞键时被挤下键表的那条
   会话仍能靠 CallID / 指纹认回，最坏由兜底闹钟收，代价远小于投错。
3. **欠账重发（治 ③）** —— `setInflight` 记住已发未收回的那次调用，客户端再来时
   `bridgeNextTurn` **原样重发同一个 CallID**；`clearInflight` 只在 CallID 对得上时销账，
   旧结果不会误清新调用。
4. **指纹分流（治 ④）** —— `bridgeRequestFingerprint` = 消息条数 + 末条 user 文本（截 512）的 fnv64。
   不带 `tool_result` 的请求：同指纹 = 原样重发 → 续跑老会话；异指纹 = 新任务 → 关老会话起新 thread。
   键**同时**漂了时按 `bridgeCallIndex.getByFingerprint` 认回（`recoverByFingerprint`）——
   这条路上客户端没回任何 id，只剩指纹能认人。

5. **同一会话同一时刻只许一轮在等（治 ⑤）** —— `beginTurn` 在每轮开头登记本轮并**抢占上一轮**，
   老那轮立刻收工回 504 `superseded by a newer request (session kept)`，不碰兜底闹钟（会话归新一轮管）。
   会话被关时在等的那轮也随之醒来，回 409 而不是挂到单轮上限。
6. **指纹按客户端凭据分桶** —— 指纹只看条数 + 末条文本，两个客户端发同一条 prompt 必然撞；
   `bridgeFingerprintKey` 用 `x-api-key` / `Authorization: Bearer` 的 fnv64 哈希给指纹加作用域，
   避免 `recoverByFingerprint` 把 A 的会话交给 B（会话键会漂，凭据不会）。哈希只用于分桶，不落日志。
7. **认主人从最后一条结果往前找** —— 请求体带着整段历史的 `tool_result`，靠前那些可能属于同一客户端
   早先那条还活着的会话；末尾那条才是这一轮要投递的调用。

真找不回时仍回 409，但区分两种原因：`bridge session was closed (idle timeout or upstream thread
ended)` / `session expired?`，并打一条带 claudeKey、CallID、在册会话数的 warn 日志。

## 3. 会话的终结点（只此四处）

thread 跑完 / thread 报错 / 同会话换了新任务（指纹不同）/ 无人接手的兜底闹钟。
兜底闹钟在**每一次把会话留在挂起态的轮次结束时**都要上：超时、客户端断开，以及**发完 `tool_use` 正常收工**那一轮——客户端一去不返（用户 Esc / 客户端崩了）时，
没有闹钟就是 thread + pump 两个 goroutine 连同云端 thread 永久常驻。
**单轮 HTTP 超时与客户端断开都不终结会话**——云端 thread 是长任务，杀了就永久失联。
关闭时 `bridgeCallIndex.dropSession` 把 CallID 与指纹两份索引一起清掉，索引不会常驻。

## 4. 仍在仓外的一条（优先级更高）

`.40` 的 Node MCP 中继在桥会话没了之后**不报错，而是静默改读 `/opt/kiro-agent-demo/` 的桩目录**，
于是 `local_fs_write` 把代码写进桩目录、模型却报告「已修改」。静默换机器比直接报错危险一个量级。
**桥没会话就该回错，请删掉中继里那条兜底分支**；本层只能把「会话本不该没」的情况消掉大半。

## 5. 验证状态

- 已跑：隔离 Go 模块（真 `hoptool` / `runtime/hoplite` + `rendezvous`、`Handler` 最小桩），
  Go 1.25.1：`gofmt -l` 无输出、`go vet ./...` 过、`go test ./...` 与 `go test -race ./...` 全绿，
  找回层 **13 条单测全 PASS**（含抢占、会话关闭唤醒、指纹凭据分桶、最新结果优先四条）。
- 没跑：真仓未编译（交付分支只含桥相关文件，无完整 `kiro-proxy` 源码树）；未做端到端真实复现。
