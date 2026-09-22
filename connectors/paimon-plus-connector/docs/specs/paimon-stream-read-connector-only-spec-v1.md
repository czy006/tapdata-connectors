# Paimon StreamRead 重构 Spec：仅修改连接器

- 版本：v1.1，2026-09-22；源码审查后的本地实现版本。
- 状态：用户已确认完全按 Spec 全新实现。连接器实现和本地测试已落地；完整结果见验证报告。真实 Engine/生产恢复未验证，仍是发布门槛。
- 唯一修改范围：`connectors/paimon-plus-connector/`。
- 连接器基线：`hotfix-spill`，`7c40958fdaf3c4f76ef8eb0312550ad16ee81faf`。
- Paimon 基线：依赖 1.3.2；源码取 `release-1.3` 的 `5c59e6cb01ed0b29563371f56e14fcade4597a2e`，不使用当前其他分支的工作区代码。
- PDK 基线：2.0.8-SNAPSHOT；核对本机缓存的 `tapdata-pdk-api-2.0.8-20260721.070507-5-sources.jar`。本地 common-lib 当前 pom 为 2.0.10-SNAPSHOT，不能用它代替依赖版本证明。
- Engine 只读证据：本地 `tapdata/develop`，`ba3533f47f`；尚未证明等同于部署版本。
- 文档关系：本文件是本次“仅修改连接器”约束下的第一版 Spec。已有 `paimon-stream-read-consumer-spec.md` 保留原样，作为旧方案参考；其 durable-ack、空批次提交、已拆分 read 类和旧测试计数不作为本方案依据。

## 1. 目标与确定方向

修复现有流读的跳快照、位点越过未交付事件、多表饥饿、空表异常、低流量不刷新和停止资源竞争。保留 TapData 的 source capability、事件回调、offset 恢复、错误处理和生命周期体系。

第一版采取以下方向：

1. 继续使用 Paimon `ReadBuilder → StreamTableScan → TableRead`，不自行解析 manifest，不引入 Flink runtime。
2. 每表每次最多调用一次原生 plan、有界事件队列、单一交付线程；以完整 plan 的结束标记控制候选 offset。
3. 新 offset 明确版本与 next-to-consume 语义。默认拒绝旧格式的歧义恢复，禁止自动 `+1`、自动钳制或按 latest 猜测。
4. 无业务事件的位点更新使用现有 PDK `HeartbeatEvent`，不调用 `accept(emptyList, offset)`。
5. Consumer 提供 `none` 与 `fixed-start` 两种模式；默认 `none`。`fixed-start` 固定保护读取周期的初始位点，在同一周期及其重启中均不前移。
6. 不注册源端 `FlushOffsetFunction` 来推进 Consumer；不把 `accept()`、目标回调或 stateMap 写后可见等同于恢复位点已持久化。
7. 不要求增加 Engine API。平台集成验证失败时，报告现有版本的能力边界，不扩大修改范围。

默认关闭 Consumer 是兼容性与运维成本选择，不表示它不重要。固定起点保护在任务长期运行时会持续保留历史；第一版不将它描述为低成本的长期自动保留方案。用户已确认按本 Spec 实施；选用 `fixed-start` 不改变其安全规则。

## 2. 范围与保证

### 2.1 用户可观察的结果

- 从新版 offset N 恢复时从 N 开始，不额外加一。
- 一张表的 plan 未全部读完，不向 TapData 交付声称已越过该 plan 的 offset。
- 表数大于 worker 数时，在原生 plan/read 能返回的前提下，每张持续可运行的表仍会得到轮询机会。
- 空表等待第一份数据；不足 batch size 的事件到达刷新期限后会发起回调。
- 缺表、元数据错误、历史缺口和非法 offset 明确失败，不伪装成读取成功。
- 取消状态生效后不再准入新读取和新回调；已准入调用纳入退出等待，半个 plan 允许重放，不被标记完成。

### 2.2 保证的准确表述

连接器保证事件与候选 offset 的先后关系，并保证自己不因已规划进度或普通回调返回而释放 Paimon 历史。端到端 at-least-once 仍要求：

1. Engine 对已完成数据处理的 offset 正确保存，并在恢复时传回相应的可恢复位点；
2. Engine 的控制事件不会越过尚未完成的业务事件；
3. 旧 source attempt 不会在新 attempt 后继续有效交付；
4. 所需源数据仍可读取，下游容忍重复；错误跳过策略没有主动丢弃数据。

这些是现有平台的适配条件，不是本连接器能够自行建立的事实。本 Spec 不承诺 exactly-once、任意停机时长可恢复、捕获所有原始事务、无限制内存安全或发现全部 drop/recreate 情况。

### 2.3 允许与禁止

**始终执行：**只改本模块；复用现有 codec、RowKind 转换与停止框架；对位点和输入严格校验；先核对源码再选 API；区分单元、Paimon 实表和真实 Engine 证据。

**需重新评审：**改变默认 Consumer 策略、扩大支持表模式、改变旧位点迁移规则、增加依赖或调整全量读取语义。

**禁止：**修改 TapData 内核/PDK/其他连接器/父 pom；直接访问 Engine 的 MongoDB 集合；另建消息落盘或恢复真相源；自动 ALTER 表；主动删除其他任务 Consumer；用本轮已交付位点推进 Consumer；把固定延迟、重试次数或上次 flushOffset 当成持久化证明。

全量读取的数据选择、batch offset、写入与 compaction 算法不在本次重构范围。允许在本模块 Service 的公共生命周期入口增加源端保护租约的失败检查与资源登记，以覆盖全量加增量的准备阶段。

## 3. 已确认问题与架构事实

### 3.1 当前连接器问题

| 编号 | 源码位置，均为 `service/PaimonService.java` | 事实与后果 |
|---|---|---|
| C1 | 2498–2505 | 恢复 Map 值时 `+1`，而正常 checkpoint 已是 next id。 |
| C2 | 2645–2649、2753–2755 | worker 更新共享 offset，交付线程可能附带仍在队列中的数据所对应的后继位点。 |
| C3 | 2708–2720、2753–2755 | running 变 false 可以截断 plan，之后仍保存完整 plan 的 checkpoint。 |
| C4 | 2608–2622、2683–2686 | 固定 worker 数搭配每表永久循环，排队表可能一直不执行。 |
| C5 | 2696–2700 | nullable checkpoint 写入 ConcurrentHashMap，空表可能 NPE。 |
| C6 | 2637–2666 | 正常运行没有未满批次的时间刷新。 |
| C7 | 2640、2771–2798 | 消费线程退出条件没有覆盖 worker 生命周期，join 超时后仍可能报告完成。 |
| C8 | 2447–2473、2558–2569 | 元数据异常/null/缺表被降级成其他启动方式或跳过。 |
| C9 | 2490、2714；对照 2852、2894 | 流读没有像有限读取一样覆盖完整的生命周期准入和受控 reader。 |

实际流读仍在 `PaimonService` 中；当前不存在旧稿所称已完成拆分的 `read/PaimonStreamTableReader`。

### 3.2 Paimon 与 TapData 契约

| 编号 | 已核对事实 | 对本方案的约束 |
|---|---|---|
| P1 | checkpoint 返回 next id；restore 接收相同语义。[S1] | 新 offset 直接 restore，不加一。 |
| P2 | plan 在读 split 前推进 next；一次调用还可能跨过零 split/不需读取的快照。[S2] | checkpoint 是候选进度；空 plan 也可能有进展。 |
| P3 | notifyCheckpointComplete 写 Consumer 的 nextSnapshot；ConsumerManager 按最小 next 限制过期。[S2][S3][S4] | Consumer 是历史保护元数据，不是 TapData ack 协议。 |
| P4 | Consumer TTL 由写侧维护清理；Table.copy 不会持久化选项。[S5][S6] | 读侧不能通过临时 TTL 配置控制独立 writer。 |
| P5 | from-snapshot 启动器会把过旧位点调整到 earliest；解耦 changelog 下缺历史不一定抛 OutOfRange。[S7][S8] | 自己验证可读范围，不依赖原生异常覆盖全部情况。 |
| T1 | PDK StreamReadConsumer.accept 只调用内部 BiConsumer。[T0] | 不产生 durable ack。 |
| T2 | Engine 仅处理非空 events；入队后更新内存 syncProgress；异常交给 errorHandle。[T1] | 空批次不提交位点；返回不等于全部入队或持久化成功。 |
| T3 | Engine 只在批次最后一条使用新位点；HeartbeatEvent 有现成转换路径。[T2] | 使用普通 PDK 心跳承载 plan 边界，并验证它的下游顺序。 |
| T4 | Engine 调用源 flushOffset 后才保存恢复位点。[T3] | 不用这个前置 hook 释放历史。 |
| T5 | 初始化阶段通过 timestampToStreamOffset 取得增量起点。[T4] | 全量加增量的固定保护必须在此建立，不能等到 streamRead。 |

## 4. 对外契约与 offset

### 4.1 能力入口

保留 `supportStreamRead`、`supportTimestampToStreamOffset` 和现有全量能力。`PaimonConnector` 可在本模块内保留实际参数类型 `StreamReadConsumer`，向内部传递它及 `TapConnectorContext`，以保留 PDK 生命周期通知能力。

- 成功初始化后调用一次 `streamReadStarted()`，在 finally 中配对调用 `streamReadEnded()`；后者只代表退出，不代表数据持久化成功。
- 不调用 `asyncMethodAndNoRetry()`。流读在调用线程管理交付与退出，方法返回时必须已完成子线程退出验证或明确报告停止失败。
- 入口已停止时不创建线程、Consumer 或回调，正常返回。
- 一个 Service 同时只允许一个流读 invocation；并发调用明确失败。进程间单活仍属于平台适配条件。

### 4.2 新 offset 格式

使用普通 `Map<String,Object>`、嵌套 Map 和字符串，不序列化 Paimon 类、线程状态或连接器私有 Java 对象。示例：

```json
{
  "kind": "tapdata-paimon-stream",
  "version": 1,
  "readEpoch": "0cbd7758-0f12-446c-9c9a-5932416949d2",
  "tables": {
    "orders": {
      "physicalTableHash": "<normalized-location-and-branch-sha256>",
      "startSnapshot": "101",
      "nextSnapshot": "108"
    }
  }
}
```

- `readEpoch` 是一个已明确初始化的读取周期，在本周期所有回调及普通恢复中保持不变；不是任务锁。
- `startSnapshot` 是本周期初始 next，永不自动前移。即使 `none` 模式也保留，方便准确说明起点和以后显式启用固定保护。
- `nextSnapshot` 是完整 plan 边界上的候选恢复位点。实际可恢复性由 Engine 保存行为决定。
- 快照号统一十进制正整数字符串，范围 `[1, Long.MAX_VALUE]`；所有递增使用溢出检查。版本字段可接受等值整数数值表示，不接受小数版本。
- `physicalTableHash` 基于规范化的实际表 location 与 branch，编码为 64 位小写十六进制 SHA-256；不包含凭据/query/fragment。只用于来源绑定，不能证明相同路径下从未重建。
- 所有回调提交深复制的快照；提交后不修改任何嵌套 Map。
- 未知版本、未知字段、缺少请求表、null、负数、0、小数、溢出、错误 hash 或 `start > next` 均失败。
- offset 允许保留本次没有请求的表条目，原样携带且不读取、不推进；动态新增表若缺少条目，必须重新显式初始化，不能取 latest 补齐。
- 请求表名非空、不重复；解析到同一实际表的别名重复也失败。

必须用本模块测试验证 PDK offset 编解码往返；不能只验证 JSON 字符串解析。

### 4.3 旧位点迁移与回滚

旧格式 `Map<table, number>` 同时承载过 timestamp 返回的已选中快照和流读返回的 next，不能自动区分；旧代码还可能已提交越过未读数据的值。

第一版拒绝所有无版本号旧 Map，不新增按数值自动猜测的开关。用户已确认“不需要保留旧的实现，完全基于 Spec 全新实现”。错误说明旧格式含歧义，并给出两条人工处理路径：

1. 利用 TapData 现有任务操作，从确认仍可读取的较早时间点重新初始化，接受必要重放并核验下游去重/覆盖策略。
2. 已无法确认缺口或历史过期时，通过现有任务操作重新全量加增量。

不提供直接编辑 Engine 存储的操作；不生成未经核实的任务管理接口命令。切换旧版本前也必须停任务并重新初始化为旧版本可识别的位点，不能将新版 envelope 直接交给旧解析器。重构不能修复升级前已发生的数据丢失。

### 4.4 首次启动与 timestamp

| 输入 | 明确行为 |
|---|---|
| `timestampToStreamOffset(null)` | 在 Connector 入口只捕获一次当前时间 T，所有表按下行显式 T 规则解析；保留现有入口的统一时间边界。不能逐表重新取 latest，否则会跳过初始化期间的提交。 |
| `timestampToStreamOffset(T)`，T 非负 | 为保持当前连接器边界，查找 `timeMillis <= T` 的最后快照 S，返回 S+1；同时考虑可用的解耦 changelog。 |
| T 早于所有现存记录，earliest 为 1 | 返回 1。 |
| T 早于现存历史，earliest 大于 1 | 无法证明历史完整，失败，不自动降级全量。 |
| 空表且显式 T 不晚于初始化时间 | 返回 1。 |
| 负数 T、未来 T | v1 拒绝，防止把空表 1 冒充“等待未来时间点”的过滤行为。 |
| `streamRead` 收到合法 v1 offset | 以传入 next 恢复。 |
| `streamRead` 收到 null | v1 拒绝，提示先走已有 timestampToStreamOffset 初始化入口；不在每次重试重新取 latest。 |

null stream offset 的拒绝是有意的兼容性变化：当前实现会取 latest+1，PDK Javadoc 又将 null 描述为从头。实际支持的 Engine 启动路径须在发布前证明会先初始化；若某版本合法路径仍直接传 null，则不能宣称该版本适配通过，也不能临时退回猜测起点。

时间解析在每表开始时冻结该表可见 latest 上界；按 id 从大到小读取 snapshot 或对应 long-lived changelog 元数据，取第一个 `timeMillis <= T` 的 id，再做溢出检查后返回后继。没有符合项时依照上表处理；途中遇到应有元数据缺失则按 §7.1 重试并失败，不能当作不存在的数据跳过。不扫描数据文件、不一次性装载全部历史；最坏元数据 I/O 为 O(保留历史数)，支持取消并记录耗时。该选择不假设不同 writer 的提交时间严格单调，同毫秒多个快照选最高 id。[S14]

Paimon 原生 from-timestamp 使用严格 `< T` 的前驱，和这里保留的 `<= T` 边界不同；不能直接替换后声称行为等价。[S9] 时间点按 Paimon commit 时间解释，不是每行原始事务时间。多表初始化不是跨表一致快照。

初始化对全部请求表做完整校验后才返回 envelope。任一失败整体失败，不返回部分 Map。没有平台可恢复状态时，本模块不声称能让“时间位点初始化本身”的跨进程重试保持原点；源端 bootstrap 若未被 Engine 保存，属于平台首次启动边界，需黑盒验证并如实记录。

## 5. Consumer 重新评估

### 5.1 决策表

| 方案 | 决定 | 原因 |
|---|---|---|
| accept 返回后推进 | 排除 | 入队和 offset 持久化没有原子确认。 |
| FlushOffsetFunction 中推进 | 排除 | hook 位于恢复位点保存之前。 |
| 按时间滞后、前一次 flush 或累计批次数推进 | 排除 | 不能证明旧位点一定已保存；故障持续时间不受这些数值约束。 |
| 把进度写到 stateMap 再推进 | 排除 | 没有建立数据与 Engine offset 的提交关系；写后可见不能补出这个关系。 |
| 从 Consumer 文件选择恢复点 | 排除 | 将形成第二个恢复真相源。 |
| 无 Consumer | v1 默认 | 无附加持久化写权限和固定历史保留成本；历史不足时失败。 |
| 固定读取周期初始点 | v1 可选 | 不依赖完成 ack；代价是周期内历史持续保留。 |

### 5.2 `none`

不创建、更新或删除 Consumer 文件，不启动 Consumer 心跳，也不要求 stateMap。scan 显式 restore；设置 `consumer.ignore-progress=true` 防止持久化表选项中的 consumer-id 影响启动选择。不得调用 notifyCheckpointComplete，因而不得覆写表上已有 Consumer。

该模式依赖现有 snapshot/changelog 保留政策。历史过期是显式错误，不保证源数据永不失效。

### 5.3 `fixed-start`

对同一 v1 offset 周期，所有 Consumer 写入均满足：

```text
consumer.nextSnapshot == offset.tables[T].startSnapshot
```

即使本次恢复 next 已是 10000、初始 start 是 100，也只能保护 100。不能因重启、心跳、plan 完成或配置重载将它改为 10000。每个 Service 保护会话使用独立 UUID attempt；Consumer id 包含 readEpoch、attempt、表 hash 的安全编码。不同任务即使复制了同一 offset，也不会覆盖同一 Consumer 文件；不依赖 TapContext 随机构造 id 的稳定性。

使用 `FileStoreTable.consumerManager()` 与 `ConsumerManager.resetConsumer(id, new Consumer(start))` 的薄适配层注册和续租，并读回验证值。这里显式调用内部管理 API，是为了让“固定起点续租”与 checkpoint 完成通知分离，避免误用 API 名称表示已提交。[S3]

初始化顺序：

1. 确认实际表、branch、请求模式与所有 next/start 可读边界；要求持久化表 options 中有合法 `consumer.expiration-time`。
2. 启动受 Service 生命周期管理的独立租约调度器。
3. 为各表建立独立 Consumer、读回 start；随后再次校验历史。校验元数据存在只是必要条件，不能证明关联 manifest/data file 完整。并发过期已经选中的删除操作仍可能发生，首次或后续 plan/read 缺文件必须失败并保留原始 cause，不能承诺注册瞬间与 writer 清理形成事务。
4. 全部成功后才返回初始化 offset，或允许恢复后的第一次 plan。

`timestampToStreamOffset` 创建的租约必须持续覆盖后续全量阶段；同一 Service 的 `streamRead` 可复用相同 epoch、表和 start 的租约。若 Engine 使用另一个 Service，后者注册自己的 attempt 文件，前者按自身生命周期结束；不得删除前者文件。

同一 Service 只保护一个 epoch/请求表集合；切换周期或集合需通过现有任务操作重启 Service，不能在仍有全量/流读时替换租约。无 Consumer 文件持久化保护生效前不得开始受保护流读。当前无 snapshot 的空表注册 start=1。启动过程中部分成功后失败的 Consumer 保留到 TTL 清理，不返回成功 offset。

### 5.4 TTL、续租与停止

- v1 使用固定 60 秒心跳间隔，要求表级 TTL 至少 10 分钟。这是连接器的容错余量策略，不是 Paimon 的最小配置要求；TTL 还需由使用者按预期停机/重启窗口设置，推荐值必须结合实际维护周期评估。
- TTL 还必须在安全纳秒期限范围内（不大于 Long.MAX_VALUE/4 纳秒）；异常大值及转换溢出明确失败。TTL 由表的持久化配置及独立写作业维护决定。缺失、非法或过短时，`fixed-start` 失败；`none` 不检查此条件。
- 心跳不读取 planned/submittedNext，只重写 start。不能与业务回调共用线程，否则大 plan、背压或回调阻塞可能使 Consumer 过期。
- 每个 Consumer 同时最多一次续租 I/O，派发队列按受保护表数设上限。续租 I/O 与不执行 I/O 的 deadline watchdog 分离；不能让单个卡住的存储调用同时阻止过期风险检测。
- 首次注册没有“上次成功”可参考：保护 attempt 创建时记录单调时钟 t0，初始化期限先取 `t0 + stopTimeoutSeconds`；读到合法 TTL 后取该期限与 `t0 + TTL - 2 × 60秒` 中更早者。首次注册与读回在独立 I/O 执行器中运行，调用线程以有限等待观察 deadline/取消；到期即 sticky failure，迟到写入不得恢复 ACTIVE。底层调用不能退出时仍须按 §8 报告停止失败并保留资源所有权，不能声称已取消物理 I/O。
- 采用单调时钟，以最后一次成功写入且读回匹配的操作**开始时间**为保守基准，风险期限为该时间加 `TTL - 2 × 60秒`。每秒检查一次；进入源操作和回调前也检查。超过期限即 sticky failure 并取消源会话，迟到成功不得将失败改回正常。此期限是连接器提前停止政策，不能证明服务端 mtime、writer 时钟或清理操作与本地时钟一致。
- 一次失败最多重试 3 次、退避 1 秒，不得越过风险期限；连续失败耗尽即失败。每次续租前重新读取持久化表配置，TTL 缺失、改变或非法即失败并要求重启核对 writer。外部 writer 没有同步配置、长时间 GC、存储阻塞及时钟偏差仍属于验证边界；不声称本地 watchdog 能提供事务租约。
- 进程恢复、已有文件消失或失败后新会话注册，都必须重新检查所需历史；成功读回文件只证明元数据可见，不证明过去未发生保护空窗。
- 正常 stop、异常 stop 均停止本会话续租，不删除 Consumer；过期清理由外部 writer 后续维护触发。TTL 到期不等于恰在该时刻已清理；没有维护就可能继续保留。
- `ConsumerManager.expire()` 根据列举到的旧 mtime 决定删除，没有 CAS/租约 generation；续租读回成功后仍可能被一个此前启动的清理删除。[S3] 不以重建同名文件掩盖保护空窗。手工清理 Consumer、强制 expire、drop/recreate 也不属于本保护保证。
- 暂停超过 TTL 或手工删除 Consumer 后，没有无限期恢复保证。即使 next 可读，fixed-start 的原始 start 已不可验证时也不自动重新锚定；需显式重新初始化读取周期。
- 心跳失败在没有活动 streamRead（例如全量阶段）时，保存源端 sticky failure；后续源操作/回调检查及 streamRead 入口必须观察到失败。不能由后台线程只打一条日志后继续宣称任务受保护。

### 5.5 运维成本与退出

固定起点模式可能阻止本周期起点之后大量快照和关联文件回收；正常重启并不会缩短这个保护区间。只有使用者通过现有任务操作显式开始新的读取周期，并在确认旧周期不再需要后让旧租约自然到期，才能结束旧保护。

v1 不根据磁盘压力自动删除 Consumer，不自动把 start 推到最新，也不提供“无限期运行但历史有界”的承诺。日志给出 start、next、Consumer id、最后续租时间和保护持续时长；不通过全表文件扫描计算精确保留字节数。

## 6. 读取与交付模型

### 6.1 职责与线程

```text
PDK 调用线程：初始化、Delivery Loop、错误传播、退出验证
  ├─ Scheduler ×1：时钟与公平派发，不执行 plan/对象存储 I/O
  ├─ Worker ×min(有效表数, availableProcessors)：每次一表一次原生 plan 及其读取
  ├─ 有界 BlockingQueue<ReadItem>：EventItem / PlanFinished
  └─ 可选 Consumer 租约管理：独立 I/O 执行器 + 无 I/O watchdog，覆盖初始化至停止
```

队列容量为 `Math.multiplyExact(eventBatchSize, 10)`，先验证 batchSize > 0 与乘法无溢出，再创建任何资源。scheduler 不无限提交任务，每表只允许一个 outstanding plan。循环选择 IDLE 表，避免 HashMap 固定前缀持续抢占。

有界队列只约束事件数量，不约束单条记录字节数、Paimon plan 的 split 元数据或全部库缓存。worker 数固定不能证明任何规模下无 OOM。慢表隔离受可用 worker 数约束；全部 worker 被 I/O 阻塞时，其他表仍会等待，不声称无条件公平时延。

原生 `DataTableStreamScan.nextPlan()` 内部有 `while (true)`，会连续跨过零 split 或无需读取的快照，没有固定工作量上限或公开取消入口。[S2] 因此本方案消除的是连接器自己的每表永久循环；不承诺一次原生 plan 严格限时、持续空提交情况下的绝对公平、或底层 I/O 必然在 stopTimeout 内退出。不得用 Future 超时后继续并发操作同一个 scan 来假装取消，也不复制内核扫描规则。此限制纳入验收与故障报告。

每个新线程按 PDK 契约初始化 `connectorContext.configContext()`，使任务日志归属正确。[T0]

### 6.2 每表状态

```text
IDLE → SUBMITTED → READING → MARKER_QUEUED → DELIVERING → IDLE
READING → IDLE（空 plan 且 after == before；设置下一次轮询时间）
任意未完成阶段 → FAILED / CANCELLED
```

有进展的 plan 只在该表 PlanFinished 回调返回且会话仍有效时释放 outstanding；无进展的空 plan 由 worker 完成通知直接释放 outstanding 并设置下次轮询时间，不发 Heartbeat、不修改 submittedNext。任务提交被拒绝、worker 抛错和取消均必须有明确的终态及资源回收，不能留下永久 outstanding。Coordinator 串行应用这些状态转换；回调返回点只用于本地背压，不命名为 acked/durable。

Coordinator 的状态所有者是 scheduler 线程。另设容量为有效表数的线程安全完成 mailbox：无进展 worker 发布 `PlanNoProgress(table, invocationToken, before)`；PlanFinished 回调检查通过后，由 Delivery Loop 发布 `PlanDelivered(table, invocationToken, after)`。每个 outstanding 只允许二者之一，scheduler 消费后才释放 outstanding；过期 token 丢弃。worker/Delivery 不直接修改表的可调度状态。mailbox 满时仅作可取消的有限等待，首个错误与取消使用独立原子状态唤醒 scheduler，不能依赖向已满 mailbox 插入 STOP。上述消息均不进入 Engine，ReadItem 业务队列仍只容纳 EventItem/PlanFinished。

各进度定义：

| 字段 | 定义 | 谁可以更新 |
|---|---|---|
| `startSnapshot` | 读取周期固定初始点 | 仅显式初始化 |
| `plannedNext` | Paimon plan 后的 checkpoint | 表 worker |
| `submittedNext` | 已发起并返回的完整 plan 边界回调所附位点 | 单一交付线程 |
| `durableNext` | Engine 真正保存的恢复位置 | 本模块没有观察接口，不维护此字段 |

只有候选 next 随交付推进；`startSnapshot` 和 Consumer 内容不变。

### 6.3 plan 与完成标记

1. 同一 scan 对象的 `checkpoint → plan → checkpoint` 串行执行，不允许其他 worker 并发访问。Consumer 续租使用独立管理适配层，不并发修改 scan。
2. 所有 scan 启动前已 restore 正整数，after 不得为 null、不得小于 before。
3. splits 非空时，依次读取全部 split 和全部 iterator；每批 iterator 在 finally 中 release，每个 reader 必须获得实际 close 结果。
4. 转换失败、reader/iterator 失败、取消或队列中断时，不发布 PlanFinished。
5. 全部读取成功且会话未取消后，入队 `PlanFinished(table, before, after)`。
6. splits 为空但 after > before，仍入完成标记；after == before 则无标记，按 poll interval 再调度。
7. 不根据 `plan instanceof SnapshotNotExistPlan` 直接认定无进展；Paimon 可能已跨过若干零 split 快照。[S2]

每次 plan 不再自行遍历和重写 Paimon 的 delta/changelog 选择规则。

### 6.4 Delivery Loop

- EventItem 追加当前 batch；达到 batchSize 或从第一条未提交事件起到达 flush interval 时发起回调，携带尚未完成该 plan 的旧 `submittedNext` Map。
- 收到 PlanFinished 后，构造候选 offset，只推进对应表；其他表携带各自最后已提交边界。
- 有未交付 batch 时，将它与候选 offset 一起提交；必须保证新 offset 对应的全部事件已经在该次或更早的回调中按序出现。
- batch 已为空（精确 batch 边界、零事件 plan）时，提交一个合法的 `HeartbeatEvent().init().referenceTime(...)` 和候选 offset。只使用 PDK 事件，不实例化 Engine 的 TapdataHeartbeatEvent。
- 内部完成标记绝不直接传给 Engine；禁止空 events 回调。
- 每次调用通过 §8 的原子准入门，检查停止/会话 token/首个异常并登记活动调用；调用返回后再次检查。只有检查通过才更新本地 submittedNext 和释放 outstanding。单独的 `if (active)` 后调用不能消除 check-then-act 竞态。
- 回调抛错立即终止整个 source 会话，不投递后续批次；若 Engine 自行吞掉错误，本模块只能通过已有 running/停止信号检测，不能承诺每个失败都同步传播回来。
- 此处 `streamReadFlushIntervalMs` 限制的是尝试提交的等待时间；已有回调被阻塞时不承诺固定端到端延迟。

提交列表不复用，不在回调后清空它；改为新建下一批容器。

## 7. 历史、表模式和事件语义

### 7.1 范围检查

在初始化、restore 前、每次 plan 前，以及 plan 无进展且预期 id 不存在时检查：

- 未解耦表：使用 SnapshotManager 的可用历史边界。
- 解耦表：结合 SnapshotManager 与 ChangelogManager 的实际元数据，允许从 long-lived changelog 继续读取，不能仅用 earliest snapshot 否定合法位点。判断请求 N 的顺序为：snapshot(N) 存在则使用它；否则解耦且 long-lived changelog(N) 存在则使用它；否则仅 N == latestSnapshot+1 可等待，其余缺失为历史不足或缺口。earliest/latest 数值范围不能代替逐个请求点的存在性判定。[S8]
- next == latest+1 是等待新快照；空表只允许 next=1。
- next 低于可读历史或高于 latest+1 明确失败。预期 id 在历史中间不存在且没有对应可用 changelog 时，报告元数据缺口，不无限等待。
- 考虑并发 commit/expire 与存储一致性：确认缺口前允许 3 次重新读取元数据，每次间隔 1 秒；这只是消除短暂可见性问题，不允许更改请求位点。期间不交付更高 offset。
- fixed-start 还检查其 start，不能只检查 next。

复用当前 PDK 已有的 offset-out-of-log 错误类型（如 `TapPdkOffsetOutOfLogEx`，实现前核对依赖签名）向平台报告；其他读取错误保持原始 cause。错误包含表、请求 next、可读范围、是否解耦、Consumer id（如有）和重新初始化建议，不包含凭据。

范围与 location hash 能发现部分重建情况，但同路径重建后又增长到相同 id 范围可能无法识别；不编造 Paimon 不存在的稳定 table incarnation ID。部署验证中要记录这一限制。

### 7.2 支持语义

- 继续使用原生 scan 的表选项与 RowKind；`INSERT/UPDATE_AFTER → TapInsertRecordEvent`，`DELETE/UPDATE_BEFORE → TapDeleteRecordEvent`，本轮不合并成 TapUpdate。
- `changelog-producer=none` 的主键表按变更后的状态同步能力解释，不能宣传完整 before image 或完整原始事务。
- `input` 的完整性取决于写侧实际输入；`lookup/full-compaction` 按原生 changelog 可见性与延迟解释。[S10]
- 不自动设置 `streaming-read-overwrite` 或 `streaming-read-append-overwrite`；遵循 Paimon 表现有选项，测试覆盖默认跳过和显式支持的组合。不把“观察所有 overwrite”列为默认保证。[S11]
- 目标必须支持相应的重复、插入/覆盖、删除语义；Append 目标不能被视为自动具备去重能力。
- 不生成虚假 DDL 事件。本轮不实现运行中 schema 自动演进；执行下述 schema guard，不能按位置静默错配。
- 保持现有 DML referenceTime 的处理，不把它描述为原始事务提交时间；监控日志区分读取时间和 Paimon 快照进度。

### 7.3 Schema guard

Paimon Table/ReadBuilder 绑定创建时的 schema，不会因外部 ALTER 自动刷新。[S13] 初始化时保存该分支 schema id B、完整递归字段 id/name/type/nullability、字段顺序、主键及分区键，与 TapTable 的映射核对后再建 reader。仅比较 schema id 会误拒绝只改注释/非读取选项的版本，因此结构指纹不含注释与一般 options。读取语义相关的 producer、merge-engine、overwrite、branch 等选项，以及由保留策略推导的 changelog 解耦状态另外固定，不能运行中悄悄切换。

每次 plan 前读取持久化最新 schema，结构或读取语义不一致即失败。非空 plan 还通过薄适配层读取 `SnapshotReader.Plan.snapshotId()` 对应 snapshot/changelog 的 schemaId；若大于 B，加载该 schema 再比较，必须在该 plan 第一条 EventItem 入队前完成。计划类型或必要元数据无法核验时明确失败。初始化前的历史 schemaId <= B 交给原生按字段 id 的历史投影，不因旧 data file schema id 不同而一律报错；必须有历史加列/改名实表回归。

本 guard 不会与并发 DDL 建立事务，只保证不把已识别的新不兼容 plan 交付；取消时已经入队的旧 schema 事件可以重放。新增列、删除列、重命名、类型或字段顺序变化均要求重新加载 TapData 元数据后重启；只改注释允许继续。尚未有携带新结构数据的 DDL，在下次检查时检测。

## 8. 停止、失败和资源所有权

停止策略选择“停止交付并允许重放”，不承诺在 Engine 已停止后还能通过排空队列保存最后位点。

1. 一旦 running=false、Service stop 或首个失败，原子取消 source 会话，scheduler 停止派发。
2. 会话取消与操作准入共用短临界区：只有 ACTIVE 且健康时才能登记 reader/回调的活动票据，然后释放锁再执行调用。取消转换为 CANCELLED 后不得再发放票据。取消前已取得票据的调用属于 in-flight，即使底层实际进入稍晚也必须等待或报告退出失败，不能声称能撤回；返回后再次检查会话，不将其当作安全确认。
3. worker 的队列写采用带取消检查的有限等待，取消后退出；Delivery Loop 释放未提交队列内容，不等待一个已经停止消费的满队列自行清空。
4. 已读一部分的 plan 不生成标记；取消前已提交的部分事件可能在恢复时重放。
5. 关闭 iterator/reader、终止 worker/scheduler；最后停止本会话 Consumer 心跳。回调或 I/O 无法及时退出时，受现有 `stopTimeoutSeconds` 总预算约束。
6. 未能证明线程或 reader 关闭时报告失败并保留资源所有权；不得因 Future.cancel、interrupt、join 超时而宣称资源已关闭。
7. 不在持有 scan/Service 全局锁时执行用户回调、等待 worker 或做长时间 I/O。
8. 保留首个失败，后续关闭异常作为 suppressed；不覆盖首因、不在失败路径输出 completed 成功日志。

必须与当前 `PaimonServiceLifecycle`、`PaimonStopController`、`PaimonStopResources` 对接。Service 关闭顺序先取消 source session，再等待生命周期静默，之后才能关闭 Catalog/FileIO；不能只增加一个永不结束的流读 ingress 后继续沿用原先等待顺序。源会话在全量阶段仅持有其保护租约资源，不持有阻塞整个 Service 停止的无限 ingress。

同一进程内会话 token 可阻止旧会话新准入；已经进入 Engine 的回调无法撤回，不能仅凭 token 宣称其副作用已被隔离。跨进程旧 attempt 的 fencing 无法由 Consumer id 代替，必须通过未修改 Engine 的真实重启测试验证。

## 9. 配置与模块结构

### 9.1 新配置

| 名称 | 类型 | 默认 | 校验与意义 |
|---|---|---|---|
| `streamReadPollIntervalMs` | 整数 | 1000 | 100–60000 ms；同表无进展后的轮询间隔。 |
| `streamReadFlushIntervalMs` | 整数 | 1000 | 100–60000 ms；未满事件批次的等待期限。 |
| `streamReadConsumerProtection` | 枚举字符串 | `none` | 仅允许 `none`、`fixed-start`；不是 Paimon 原生 consumer.mode。 |

PaimonConfig.load 和 spec.json 同步校验；UI 中加入简明中文、英文及现有第三语言文案。字段应归属源端高级配置，不能与写入 commit interval 混用。源端表级覆盖暂不支持，检测到上述键的表级覆盖时明确报错。

不新增 TTL 配置，不新增可前移 Consumer 的模式，不新增 legacy 自动猜测配置，不新增固定线程数/队列大小用户配置。保持 Java 8 编译目标，不升级 Paimon/PDK。

### 9.2 实现职责

| 本模块路径 | 职责 |
|---|---|
| `PaimonConnector.java` | 保留 PDK consumer 类型、时间位点入口与生命周期衔接。 |
| `service/PaimonService.java` | 委托源端会话、提前注册初始化保护、与现有 stop 顺序对接。 |
| `read/PaimonStreamReadSession.java` | 每次调用的初始化、交付、取消、错误传播和资源所有权。 |
| `read/PaimonStreamReadCoordinator.java` | 有限任务、公平调度、每表状态和 ReadItem。 |
| `read/PaimonReadOffset.java` | v1 envelope、不可变快照、严格解析与来源绑定。 |
| `read/PaimonReadPositionResolver.java` | timestamp、快照/changelog 可读范围和缺口诊断。 |
| `read/PaimonConsumerLease.java` | 独立固定 start 保护、注册/续租与失败状态。 |
| `config/PaimonConfig.java`、`resources/spec.json` | 三项源配置及语言文案。 |
| `src/test/java/io/tapdata/connector/paimon/read/`（新目录） | 单元、原生 Paimon 实表与恢复测试。 |

不要求 Service/Connector diff 为零；用户限制的是模块范围，并非禁止模块内部入口调整。优先复用现有转换与停止工具，不顺带重构写入链路或复制整个 Service。

命名按现有 Java 风格，状态单一所有者，显式类型取代 Object 队列。规范示意如下，不代表本轮实现：

```java
// nextSnapshot comes from a completely read plan, not merely scan.plan().
// Source: Apache Paimon release-1.3 @ 5c59e6cb01ed0b29563371f56e14fcade4597a2e
// paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java
// DataTableStreamScan#nextPlan, lines 199-242.
// plan() advances next before split consumption; TapData accept is not a durable ack.
// Delivery runs with an admission ticket acquired through the session gate (section 8).
PaimonReadOffset candidate = submittedOffset.withNextSnapshot(tableName, nextSnapshot);
consumer.accept(events, candidate.toMap());
session.checkActive();
submittedOffset = candidate; // Local submission only; never a durable acknowledgement.
```

### 9.3 后续编码的源码注释要求（用户明确要求）

每个依赖 Paimon/PDK/Engine 特定语义的适配方法必须在代码邻近处注释：来源项目与固定 commit 或依赖 artifact、完整源码路径、类/方法及定位行号、复用的契约，以及连接器为何需要该处理。优先附固定 commit 链接；行号只作导航，方法名和版本不可省略。不得只写“参考 Paimon”或只链接首页，也不要求给普通 getter 加无意义注释。

至少覆盖 checkpoint/restore、plan 完成边界、timestamp 相等边界、snapshot/changelog fallback、固定 Consumer/TTL、schema guard、Heartbeat/候选 offset 及停止准入。来源注释必须区分“上游原生行为”和“本连接器政策”；例如 60 秒心跳是本方案政策，不能伪称 Paimon 要求。测试引用对应契约，并用能捕获原缺陷的事件/位点序列断言。

### 9.4 后续实施的本地提交要求（用户明确要求）

每完成一个可验证实现单元，按“实现 → 定向测试与必要回归 → 审查 → 提交”执行，提交到该实现对应的本地分支后才进入下一单元。开始前记录分支和 HEAD；已有指定分支就沿用，需要新分支时使用 `codex/` 前缀，不擅自推送或合并。避免拆成无法编译或没有行为验证的占位提交。

每次提交只包含本模块且属于该单元的确切路径，检查工作区与暂存区差异；用户原有的已暂存设计稿必须保留，不能通过全量 add/commit 混入。报告本地分支、commit SHA、测试结果和未验证边界；测试失败先定位处理，不把未通过的候选声明为已完成。提交说明解释行为与原因，注明来源基线及验证证据。

本轮为 Spec 审查，没有 Java 实现，不生成“实现完成”提交；文档状态与未来实施提交状态分别记录。

## 10. 验证策略与验收

### 10.1 本模块单元及 Paimon 实表测试

| 测试组 | 必须覆盖的场景与判定 |
|---|---|
| Offset | v1 编解码、PDK 编解码往返、未知版本/字段、null/旧 Map 拒绝、整数溢出、缺表、来源不匹配、Map 深复制、额外未请求表保留。 |
| 初始点 | null timestamp 统一捕获 T（初始化间新提交仍可读取）、空表 1、同毫秒多快照取最高 id、时钟回拨、snapshot/changelog 接缝、未来/负数拒绝、历史不足拒绝；连续提交期间不混淆计划起点与返回 envelope。 |
| 恢复 | 实际写入含可识别数据的快照，保存 next=N 后恢复必须读 N，不读 N+1 起；精确记录事件序列，不能只断言总行数。 |
| 完整 plan | 多 split、超过多个 event batch；只有全部读完才有新 offset。reader/转换/队列失败和部分取消不得产生完成标记。 |
| 空进展 | 空表首次 plan 无进展后，在同一 invocation 写入数据仍能读到；无新快照和 streaming-read-delay 等待后能再次派发；完成 mailbox 的 NoProgress/Delivered 各释放一次，旧 token 不释放新会话。仅有进展的零业务事件 plan 提交合法 Heartbeat，覆盖零 split、精确 batch 边界，禁止 empty-list。 |
| 调度 | 注入 worker=1、至少三张表都有新数据，三者均得到读取；多 worker 下慢表不占用 scheduler；持续零 split/跳过快照时验证原生 plan 的长调用限制，不能把外层任务粒度当作内核执行上限。 |
| 时延与背压 | 1 条事件不够 batch 时按可控时钟刷新；回调阻塞时内存队列有界，取消可解除生产者等待。 |
| 范围 | 普通历史过期、解耦 changelog 合法恢复/过期/中间缺失、超大位点、并发可见性重试不跳号。 |
| Consumer none | 全流程无 Consumer 写删、无心跳、无 stateMap 依赖。 |
| Consumer fixed-start | 初始化前注册；全量阶段仍续租；恢复 next 增大但 start 不变；不同 attempt 不覆盖；写失败/读回不符/租约迟到终止；首次注册及后续续租 I/O 分别卡住时 watchdog 仍失败；迟到成功不可恢复 ACTIVE。 |
| 独立 writer | 使用独立 Table/Commit 对象按真实持久化 TTL 做清理；活跃租约保护 start，停租后 writer 可清理；不能复用读侧动态 Table 假装写侧配置生效；覆盖注册与 expire 并发、旧 mtime 清理与续租交错、TTL 配置改变及 manifest/data file 缺失。 |
| 停止 | 满队列、阻塞 read、回调阻塞、半 plan、失败与 stop 并发；取消后无新准入、已准入回调可追踪、无半 plan 标记，无法关闭则失败并保留资源。 |
| Schema | 初始化元数据不匹配；运行时加/删/改名/类型/顺序变化在新 plan 第一条入队前失败；只改注释可继续；历史 schema 投影保持；ALTER 与 plan 并发。 |
| 既有行为 | RowKind 映射、实际支持的 changelog 与 overwrite 组合、batchRead/写入/停止原有回归不出现新增失败。 |

采用已有 JUnit 5、Mockito；原生行为必须使用 Paimon 1.3.2 实表，不能全部由 mock 证明。新增集成测试命名 `*IntegrationTest`，使现有 Surefire 默认发现；不假定 `*IT` 会自动执行。

### 10.2 不修改 Engine 的黑盒适配验证

测试脚本、场景说明、输出记录只能存放本模块；可以使用现有 Engine 运行环境，不修改其源码或新增内核测试文件。单元 fake consumer 只用于连接器逻辑，不能替代以下事实：

1. 实际部署 PDK/Engine 版本、启动模式和 offset 编解码可识别 v1 envelope。
2. 正常 CDC 与全量加增量均通过时间位点入口；首次 offset 的保存和重启行为被观察记录。
3. Heartbeat 在普通、异步写入及多目标任务中不越过未完成 DML，位点最终可保存；未验证的组合不得标为已支持。
4. 注入进程退出：plan 中途、回调后目标未完成、目标完成但位点未保存、位点保存之后；重启后的事件与 offset 无缺口，允许重复。
5. 任务停止/重试期间旧 attempt 不在新 attempt 后继续有效交付；如果现有平台做不到，报告不适配，不能在连接器中假装实现了跨进程 fencing。
6. fixed-start 模式下全量耗时跨越原快照保留窗口，独立 writer 清理仍不破坏受保护的增量起点。

验证失败先判断能否在本模块内修正；若不能，明确限制或不发布该组合。不得把结果改写成“需要修改 Engine 后本 Spec 才成立”。

### 10.3 命令与证据记录

以下为未来实施验证命令，本轮没有执行。仓库根目录运行，使用 JDK 17，模块仍编译为 Java 8；runner/error-code 覆盖沿用本地可用验证依赖，不代表修改生产依赖版本。

```bash
mvn -pl connectors/paimon-plus-connector \
  -Dmaven.gitcommitid.skip=true \
  -Dtapdata.pdk.runner.version=2.8-SNAPSHOT \
  -Dpdk-error-code.version=2.8-SNAPSHOT \
  -DskipTests test-compile

mvn -pl connectors/paimon-plus-connector \
  -Dmaven.gitcommitid.skip=true \
  -Dtapdata.pdk.runner.version=2.8-SNAPSHOT \
  -Dpdk-error-code.version=2.8-SNAPSHOT \
  -Dtest='PaimonReadOffsetTest,PaimonReadPositionResolverTest,PaimonStreamReadCoordinatorTest,PaimonConsumerLeaseTest,PaimonStreamReadIntegrationTest' \
  test

mvn -pl connectors/paimon-plus-connector \
  -Dmaven.gitcommitid.skip=true \
  -Dtapdata.pdk.runner.version=2.8-SNAPSHOT \
  -Dpdk-error-code.version=2.8-SNAPSHOT \
  test

git diff --check -- connectors/paimon-plus-connector/
git diff --name-only
git diff --cached --name-only
```

定向测试的实际类名与执行证据见 `docs/validation/stream-read-delivery.md` 和 `tasks/todo.md`。上面的构建检查使用 `test-compile`，不触发 package：当前模块的 package 阶段会向 `connectors/dist/` 复制产物，不符合本次输出范围。未来如需打包，只能在本模块 pom 内将该复制输出改为本模块目录或增加默认不启用的模块内打包配置，再补充相应命令；不修改父 pom 或其他目录。测试使用临时 catalog 的位置也必须显式指定在本模块 `target/` 内，不对真实业务表执行清理。

本轮不引用旧稿的“540 总计/3 失败”为当前基线。实现开始前重新记录基线，同环境验证候选；新增测试全通过，既有失败逐项比较，不能把“没有新失败”写成“全部通过”。记录 commit、依赖解析版本、执行命令、失败方法及 Engine 场景结果。

### 10.4 完成判定

- 所有修改和新增源文件、测试、脚本、文档都在本模块。
- 新位点不隐式 +1；歧义位点、历史不足和半 plan 均不静默跳过。
- 多表公平、低流量刷新、空表/Heartbeat 和停止用例通过。
- Consumer 内容始终为周期 start；没有 accept/flush/stateMap 驱动的自动前移，也没有自动删除。
- 现有模块写入与有限读取没有新增回归。
- 语义适配代码带固定版本源码位置与解释性注释，每个已完成实现单元均有对应本地 commit。
- 报告明确区分连接器逻辑通过与真实 Engine 场景通过；部署兼容性未验证时不声称端到端恢复已完成。

## 11. 已确认取舍与实施边界

用户已确认按 Spec 全新实现，以下取舍按该授权落地：

| 取舍 | v1 默认方向 | 影响 |
|---|---|---|
| Consumer 是否默认开启 | `none`；`fixed-start` 显式选用 | 避免无意增加历史保留和权限要求；默认依赖既有历史保留。 |
| 无版本号旧 offset | 拒绝，走现有任务重新初始化 | 升级需要安排停机及重放/全量操作。 |
| null stream offset | 拒绝；null timestamp 仍支持 | 依赖正常 Engine 的初始化入口；不兼容直接传 null 的独立调用。 |
| 时间相等边界 | 保留 `<=T` 后继 | 不与 Paimon 原生 `<T` 语义混淆。 |
| 停止尾批 | 取消未完成交付，重启重放 | 不假设停止后的回调仍可持久化。 |
| 固定保护历史增长 | 明确暴露，不自动前移 | 不适合不接受长期历史保留的持续任务。 |

本轮自查已消除旧方案中的架构矛盾：不依赖新增 ack、空批次提交、stateMap 持久化猜测、尚不存在的 read 类或固定旧测试数。仍需通过实施测试解决的不是“是否改内核”，而是当前部署组合是否满足现有 PDK 契约、源端停止接缝是否正确，以及实际存储和 writer 下的历史保护行为。

## 12. 来源索引

以下 Paimon 链接固定到本次核对的 release-1.3 commit；文档仅用于 API 和配置说明，不代替源码行为。

- [S1 StreamTableScan](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/StreamTableScan.java#L26)：checkpoint/restore 契约。
- [S2 DataTableStreamScan](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L199)：扫描推进、空计划、Consumer 写入。
- [S3 ConsumerManager](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L66)：注册、最小进度和 TTL。
- [S4 ExpireSnapshotsImpl](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java#L119)、[ExpireChangelogImpl](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/ExpireChangelogImpl.java#L122)：保护边界。
- [S5 TableCommitImpl](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L364)：写侧维护。
- [S6 AbstractFileStoreTable](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java#L294)：动态 Table 副本与 ConsumerManager。
- [S7 ContinuousFromSnapshotStartingScanner](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/ContinuousFromSnapshotStartingScanner.java#L45)：earliest 钳制。
- [S8 NextSnapshotFetcher](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/NextSnapshotFetcher.java#L52)：缺失与越界。
- [S9 TimeTravelUtil](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java#L156)：原生时间边界。
- [S10 Changelog Producer 官方文档](https://paimon.apache.org/docs/1.3/primary-key-table/changelog-producer/)：不同变更模式的能力。
- [S11 CoreOptions](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L975)：overwrite 选项及限制。
- [S12 Consumer 官方文档](https://paimon.apache.org/docs/1.3/flink/consumer-id/#usage)、[Java Stream Read](https://paimon.apache.org/docs/1.3/program-api/java-api/#stream-read)：使用方式。

- [S13 ReadBuilderImpl](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilderImpl.java#L65)、[SnapshotReader.Plan](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/SnapshotReader.java#L130)：绑定 readType 与计划 snapshotId。
- [S14 SnapshotManager.earlierOrEqualTimeMills](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java#L287)：相等即返回，不能保证同毫秒最高 id。

Engine 本地源码路径供复核，均仅作为只读参考：

- T0：PDK 2.0.8-SNAPSHOT sources.jar 中 `io/tapdata/pdk/apis/consumer/StreamReadConsumer.java:74`、`context/TapConnectorContext.java:33`；普通回调和线程日志上下文。
- [T1 Engine source 回调](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkDataNode.java:1169)：非空判断、入队、内存位点与异常处理。
- [T2 批次与控制事件转换](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkBaseNode.java:1403)：批次最后位点；同文件 1512 行 Heartbeat 转换，1777 行停止时入队行为。
- [T3 Engine 位点保存](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastTargetPdkBaseNode.java:2030)：flushOffset 在保存之前；同文件 1901 行包含不同目标控制事件路径。
- [T4 Engine 初始位点](/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/HazelcastSourcePdkBaseNode.java:744)：初始化与 timestampToStreamOffset。

实施结果与范围核对见 [本地交付记录](../validation/stream-read-delivery.md)；Engine 源码与原工作区暂存设计稿未修改。
