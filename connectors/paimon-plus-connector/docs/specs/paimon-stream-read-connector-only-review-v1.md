# Paimon StreamRead Spec 源码审查记录

- 日期：2026-09-22。
- 对象：`paimon-stream-read-connector-only-spec-v1.md`，v1.0-draft → v1.1-draft。
- 范围：仅连接器内的设计文档；没有实现 Java、修改 TapData 内核或运行实现测试。
- 方法：先核对验收矩阵与 Paimon 原生测试，再沿源码审查正确性、可读性、架构、安全与性能；附独立模型只读复核。
- 基线：Paimon `release-1.3@5c59e6cb01ed0b29563371f56e14fcade4597a2e`；连接器 `hotfix-spill@7c40958fdaf3c4f76ef8eb0312550ad16ee81faf`；PDK 和 Engine 基线沿用 Spec。

## 审查结论

原 v1.0 **Request changes**：存在会让空表停止轮询的状态机缺口，以及时间起点、原生 plan 执行上限和 schema 检测等问题。v1.1 已修订下列设计问题并补入验收条件；这些修订是源码推导，不能替代实现与故障测试。

迁移策略仍待用户答复，Consumer 默认 `none` 仍是草案建议。真实 Engine 的 Heartbeat 顺序、位点保存、启动入口与旧 attempt 退出尚未完成验证。因此本轮不将 Spec 标成“实现已通过”或“可直接发布”，也不作 100% 无缺陷承诺。

## 必须修订的问题与处理

以下“原稿位置”指本次审查前 v1.0 的行号；“修订位置”按章节定位，避免文档编辑后行号误导。

| 编号 / 严重度 | 原稿位置与触发条件 | 影响 | v1.1 处理 |
|---|---|---|---|
| R1 / P1 | §6.2 第 250 行仅完成标记回调后释放 outstanding；§6.3 第 270 行无进展不发标记。空表、没有新提交、read delay 均可命中。 | 该表永远无法再次调度，后续数据不再读取。 | §6.2 增加无进展直接回 IDLE 的通知，保留退避、不推进位点；测试同一 invocation 从空表等到新数据。 |
| R2 / P1 | §4.4 第 150 行将 null timestamp 改为逐表 latest+1。 | 与当前 Connector 一次捕获 T 的入口不同；初始化期间的新提交可能被越过。 | §4.4 统一捕获 T，逐表仍使用同一 T 解析；验收跨初始化并发提交。 |
| R3 / P1 | §6.1/6.3 把一次原生 plan 当作严格有限任务。 | 原生 while(true) 可连续跳过空快照；公平轮询和停止时限不能作绝对保证。 | §2、§6.1 明确调用返回前提；不复制内核扫描规则、不以 Future 超时伪装取消；增加持续空提交场景。 |
| R4 / P2 | §4.4 的“最后一个 <=T”没有约束同毫秒和解耦历史查找实现。 | 直接复用 earlierOrEqualTimeMills 可能命中中间快照；单查 snapshot 会遗漏仍在 changelog 中的历史。 | §4.4 冻结 latest 上界、倒序联合解析元数据，取最高符合 id；不依赖 commit 时间严格单调；缺口失败。 |
| R5 / P1 | §7.2 第 312 行只写“不兼容 schema 失败”，没有检测时点、数据模型或测试。 | Table/ReadBuilder 绑定旧 schema，可能静默漏新增字段或沿用错误字段映射。 | §7.3 增加初始结构绑定、逐 plan 检查与新 schema 的计划元数据复核；历史 schema 由原生投影处理，补加/删/改名/类型及历史投影测试。 |
| R6 / P1 | §8 第 319–320 行依赖停止前后检查，并宣称无新回调。 | 检查与调用之间取消可发生；旧回调不能撤回，token 不等于隔离 Engine 副作用。 | §6.4、§8 使用原子准入票据，明确取消后拒绝新准入、已准入调用必须等待或报告退出失败。 |
| R7 / P2 | §5.4 第 213 行“TTL 风险边界”无定义，续租阻塞可挡住自检；注册后只复查历史元数据。 | 无法确定何时失败，也不能由读回 Consumer 推导保护连续或数据文件完整。 | §5.3/5.4 定义保守 deadline、无 I/O watchdog、不可恢复的失败状态和独立 writer 竞态测试；明确原生删除不具备 CAS。 |
| R8 / P2 | §7.1 仅给历史边界原则。 | 解耦 snapshot/changelog 接缝的中间空洞可能被误当等待，或合法 changelog 恢复被误拒绝。 | §7.1 按请求 id 实际存在性判定，只有 latestSnapshot+1 可等待；增加接缝与中间缺失测试。 |

增量复核发现的两个实现接缝也已补齐：§6.2 明确线程安全完成 mailbox 与单一状态所有者，防止 worker 直接改调度状态；§5.4 为首次注册设置独立初始化 deadline，避免“尚无成功续租时间”使 watchdog 失效。分别增加 mailbox/token 与首次注册卡住的测试要求。

## 内核与架构依据

所有 Paimon 链接固定到本轮源码 commit：

- R1/R3：[DataTableStreamScan.nextPlan](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L199)，199–242 行：缺快照和 delay 返回空 plan；空 split/无需扫描时继续循环；读取前已推进 checkpoint。
- R2：[当前 Connector 时间入口](/Users/SL/javaProject/tapdata-connectors/connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/PaimonConnector.java:501)，先将 null 转成一次捕获的当前时间，再调用 Service。
- R4：[SnapshotManager.earlierOrEqualTimeMills](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java#L287)，309–311 行相等即结束；[TimeTravelUtil](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/TimeTravelUtil.java#L156) 的 from-timestamp 是另一套严格时间边界。
- R5：[ReadBuilderImpl](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/ReadBuilderImpl.java#L65) 绑定 Table；[SnapshotReader.Plan](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/snapshot/SnapshotReader.java#L130) 提供可空 snapshotId；当前 Service 2585–2596 行固定字段列表。
- R6：Engine source 回调、队列停止行为见 Spec T1/T2；这里的准入门是连接器并发设计，不能标成 Paimon 已提供的保证。
- R7：[ConsumerManager](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L70)，写入是 overwrite，95–104 行依照列举的 mtime 删除；[ExpireSnapshotsImpl](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/ExpireSnapshotsImpl.java#L245) 清理 manifest 后才删除 snapshot 元数据，注册与过期没有原子握手。
- R8：[NextSnapshotFetcher](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/utils/NextSnapshotFetcher.java#L52)，优先 snapshot，再 long-lived changelog；解耦历史缺失不完整覆盖 OutOfRange。解耦且无 changelog producer 时，[SnapshotDeletion](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/operation/SnapshotDeletion.java#L68) 对 APPEND 文件和 manifest 有单独保留逻辑，不能用“只有 changelog 文件才可恢复”一概否定该模式。
- 原生测试参考：[StreamTableScanTest](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/test/java/org/apache/paimon/table/source/StreamTableScanTest.java#L50) 的空计划、恢复、bounded 与越界场景；本轮只阅读，未执行。

## 五轴审查结果

| 维度 | 结论与约束 |
|---|---|
| 正确性 | 修订 R1–R8；plan 完成才推进候选 offset、直接 restore(next)、失败不得伪提交。依然需要实表与 Engine 故障测试。 |
| 可读性 | 明确无进展状态与已有/候选/持久化进度，来源注释区分上游契约与本连接器政策；避免仅拆文件却保留共享可变 offset。 |
| 架构 | 保留 TapData capability、回调、生命周期、offset 恢复；Consumer 只作固定历史保护，不作第二恢复真相源；不修改 Engine/PDK。 |
| 安全 | 输入和来源绑定严格校验；不写 Engine 存储、不自动 ALTER 或删 Consumer；日志不得输出凭据。未做第三方依赖漏洞扫描，不声称完成供应链审计。 |
| 性能 | 有界事件队列、每表一次调度；公开原生 plan 无硬上限、元数据倒序查找最坏 O(history)、逐 plan schema I/O 和 fixed-start 长期保留成本。实施时测量，不虚构吞吐结果。 |

## 已纳入的后续执行要求

1. Spec §9.3：语义适配代码必须有固定版本、完整路径、类/方法、位置与解释性注释；源码链接不能只放在 Spec。
2. Spec §9.4：每个实现单元经过测试和审查后提交对应本地分支，报告 branch/SHA/证据；不自动 push/merge。
3. 所有修改仍限定 `connectors/paimon-plus-connector/`；已有暂存旧设计稿保留原样。
4. 用户取舍问题使用 interview-me 逐项澄清。已问旧 offset 是否默认拒绝；未答复前不能写成已确认的升级政策。

## 本轮验证边界

- 已做：固定分支源码核对、独立只读复核、文档约束与引用检查、工作区范围检查。
- 未做：Java 实现、编译、单元/实表测试、真实 Engine 或生产验证。
- 旧稿工作区与暂存区 blob 均保持 `c087661c9c81ab71bf56d24547aed639b9129cf9`；没有将其并入本轮内容。

## 实现期间复核（2026-09-22）

用户已明确“完全基于 Spec 全新实现”，迁移政策和默认 none 已确认。以下为实现阶段新增证据，前文“未实现/待答复”描述的是最初文档审查时点。

- 修正 null RowKind 转换被静默忽略、TapField 顶层和嵌套 nullable 核对；对应定向测试通过。
- 修正 Consumer watchdog 与正常 close 的派发竞争：关闭与提交共用短门禁，正常关闭不产生 sticky protection failure；确定性并发测试覆盖检查与派发之间关闭。
- Consumer TTL 除至少 10 分钟外，要求可安全转换为纳秒期限（不大于 Long.MAX_VALUE/4 纳秒）；防御异常大配置和 Duration 溢出。
- 核实 CoreOptions.changelogLifecycleDecoupled 由 snapshot/changelog 保留策略推导，schema guard 固定这一读取语义，不使用不存在的独立开关。
- 元数据存储访问错误保持原始异常；仅元数据确实缺失且重试耗尽才报告 offset-out-of-log，避免误导平台重新初始化。
- 独立复核曾提出 ended 通知和最大 STOP 秒数问题；回查 Engine listener 和算术后撤回：普通同步入口不把 ENDED 当成功，后续同步失败仍传播；Integer.MAX_VALUE 秒可安全表示为纳秒。

验证：`target/stream-read-validation/review-matrix.log`，20 项通过，0 失败/错误/跳过。包含真实 none/input/lookup/full-compaction、append/主键 overwrite、半 plan 回放、租约、schema 和范围检查。lookup 测试先因 writer 缺 IOManager 失败，按原生 TableWrite.withIOManager 契约补齐测试写侧后通过；未改写连接器源端语义。
