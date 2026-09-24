# Hoplite 桥：会话生命周期与 409 找回层

> 本文对应 `internal/proxy/hopliteagent_bridge_recover.go` + `hopliteagent_bridge_serve.go` 的调用点。
> 待并入 `docs/hoplite-provider.md` 桥那一节（本仓只保留 internal/，那份文档不在本工作树里）。

## 1. 会话只由三件事终结

桥会话（`bridgeSession`）承载的是一条**云端 Hoplite thread**，生命周期比单轮 HTTP 长得多（十几分钟常见）。
它只由三件事终结：

1. thread 跑完（`finalCh` 拿到终态）；
2. thread 报错；
3. 同一会话键上来了**另一个任务**（请求指纹变了）；

外加一个兜底：无人接手时的 idle 闹钟（`touchBridgeSession`，TTL = `max(3×agentMaxWait, 10min)`，每轮重置）。

**单轮 HTTP 的超时（504）与客户端断开都不终结会话。** 杀会话 = 云端 thread 永久失联，
后续所有 MCP 调用在 `bridgeToolRelay` 都会 503，客户端按常规重发 `tool_result` 就撞 409。

## 2. 409 的三条成因与修法

| # | 成因 | 修法 |
|---|---|---|
| 1 | 本轮等超时/断开把整条会话连同 thread 杀了（头号来源） | 只结束这一轮 HTTP，回 504「resend to keep waiting」；会话留给 idle 闹钟收 |
| 2 | 会话键漂移：客户端压缩上下文 / 换 `metadata.user_id` 时 coreauth 探测键会变 | `bridgeCalls`（我们发出去的 `tool_use` id → 会话）反查，命中即 `rebindBridgeSession` 改挂当前键；`closeBridgeSession` 改**按值删**表项（键可能已被改过） |
| 3 | `tool_use` 半路丢：thread 卡在那次 MCP 调用上等结果，provider 侧干等「下一个」调用 → 互等到 TTL | 会话记 `inflight`，客户端不带 `tool_result` 重发时**原样重发同一个 CallID**；按 CallID 销账，迟到的旧结果不会误清新调用 |
| 附 | 不带 `tool_result` 的重发被当成新任务 → 再起一条云端 thread（双倍计费 + 老 thread 失联） | 按请求指纹（消息条数 + 末条 user 文本哈希）分流：同指纹 = 续跑，不同 = 新任务（先关老会话） |

## 3. 找回三层与 409 文案

`resolveBridgeSessionForResults`：当前键 → `tool_use` id 反查（命中改挂）→ 放弃。
放弃时区分两种原因，并打一条带 `key` / `calls` / `live=在册会话数` 的 warn 日志：

- `bridge session was closed (idle timeout or upstream thread ended)` —— 有过，已关；
- `no active bridge session for these tool results (session expired?)` —— 从没有过（键和 callID 都不认识）。

## 4. 调用点（serve 侧只留这几处）

| 位置 | 调用 |
|---|---|
| `serveHopliteBridgeMessages` 带 `tool_result` 分支 | `resolveBridgeSessionForResults` + `sess.settleCalls` |
| `serveHopliteBridgeMessages` 不带 `tool_result` 且键命中 | `resumeExistingBridgeSession`（指纹分流 / inflight 重发） |
| turn-1 建会话 | `fp: bridgeRequestFingerprint(body)` |
| `bridgeNextTurn` 开头 | `touchBridgeSession`（重置 idle 闹钟） |
| `bridgeNextTurn` 工具分支 | `writeBridgeInflight` → `trackBridgeCall` |
| `closeBridgeSession` | `markClosed` + `dropBridgeCalls` + `dropBridgeSessionEntries` |

## 5. 验证状态

- `gofmt -l` 无输出；`go vet ./...`、`go test ./...`、`go test -race ./...` 全绿
  （隔离模块：真文件 + `rendezvous`/`Handler` 最小桩，Go 1.25.1）。
- 8 条单测覆盖：键漂移找回并改键 / 死壳与「从没有过」分开 / inflight 按 id 销账 / 索引有界淘汰 /
  关闭清索引且按值删表项 / 指纹分流 / inflight 原样重发同一 CallID / idle 闹钟上弦与撤销。
- **没做**的：真仓整仓编译（本工作树只有 `Kiro/internal/`）、端到端真实复现（起不了服务、拿不到线上日志）。
  idle 闹钟只测了上弦/撤销，没测真到点触发。

## 6. 仓外遗留（优先级最高）

.40 的 Node MCP 中继有一条**静默兜底**分支：桥会话没了之后它不报错，改去读写 `/opt/kiro-agent-demo`
的桩目录。硬证据：`local_fs_list` 返回的是该 demo 目录（`README.md` / `hello.txt` / 历次交付 md），不是真项目树。
危害：`local_fs_write` 把代码写进桩目录，模型却报告「已修改」。**桥没会话就该回错，请删掉那条兜底分支。**
