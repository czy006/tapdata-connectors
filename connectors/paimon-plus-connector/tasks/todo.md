# 任务清单：Paimon StreamRead 连接器内重构

> 2026-09-22；状态：已开始本地实施，任务按实际证据更新。唯一执行状态来源为本文件。依据 [Plan](plan.md) 和 [Spec](../docs/specs/paimon-stream-read-connector-only-spec-v1.md)。本轮只编写并提交文档。
> 下列文件路径均相对 `connectors/paimon-plus-connector/`；新增类名为规划，不是已存在文件。B/F/G 的完整命令见 Plan §4。每项在验证及审查后提交到 `codex/paimon-stream-read`，填写证据/提交，再勾选完成。

## S01：建立基线并核验平台入口

- [ ] 完成并本地提交。

**说明：**先记录依赖、现有测试和部署契约，尽早验证正常 CDC/全量加增量的初始化入口及 Heartbeat 行为。

**依赖：**无。

**验收：**

- [ ] 记录当前 HEAD、JDK、实际 Paimon/PDK 依赖及同环境完整基线；失败不隐藏。
- [ ] 取得初始化入口、offset 往返和 Heartbeat 处理的真实环境证据；无环境写明 E01 阻塞与所缺条件。
- [ ] 记录 D01/D02 的待决状态及实施边界，不将提交计划视为产品确认。

**验证：**G；检查 Surefire 报告；授权测试 Engine 上执行场景取证；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `docs/validation/stream-read-baseline.md`
- `docs/validation/stream-read-engine-contract.md`
- `src/test/scripts/stream-read-engine-probe.sh`

**规模：**M，3 个文件。

**证据与本地提交：**本地基线通过，见 docs/validation/stream-read-baseline.md；Engine 源码已核对，真实环境部分保持待验证。

## S02：严格 v1 offset 编解码

- [x] 完成并本地提交。

**说明：**实现不可变 v1 envelope 与 PDK 编解码往返，建立恢复时无需 +1 的明确协议。

**依赖：**S01。

**验收：**

- [x] 合法 offset 往返保留 readEpoch/start/next/额外表条目，回调深复制不受后续修改影响。
- [x] 未知字段/版本、歧义旧格式、null/溢出/start>next、缺表和来源不符明确失败；迁移开关不自行扩展。
- [x] 用实际 PDK 编解码验证普通 Map/string 兼容，不只测 JSON。

**验证：**F PaimonReadOffsetTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonReadOffset.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonReadOffsetTest.java`

**规模：**S，2 个文件。

**证据与本地提交：**PaimonReadOffsetTest：5 项通过；实际 PDK V2 编解码往返、深复制、歧义/溢出拒绝、位置绑定验证；offset.log。

## S03：解析共同时间点及历史范围

- [x] 完成并本地提交。

**说明：**实现统一 T 对应的时间位点解析与 snapshot/changelog 请求点验证，供初始化和恢复复用。

**依赖：**S02。

**验收：**

- [x] 同毫秒取最高符合 id，提交时钟回拨不依赖二分假设；null 在入口捕获的同一 T 可贯穿全部表。
- [x] 空表=1、latest+1 等待；联合历史过期/中间缺口/超大位点/未来与负时间按 Spec 失败，不钳制。
- [x] 并发可见性重试不跳号，冻结上界、有限元数据缓存及取消可测试，实表记录事件与起点。

**验证：**F PaimonReadPositionResolverTest,PaimonReadPositionIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonReadPositionResolver.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonReadPositionResolverTest.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonReadPositionIntegrationTest.java`

**规模：**M，3 个文件。

**证据与本地提交：**Resolver 5 项 + Paimon 1.3.2 实表 1 项通过；position.log。真实 restore(2) 只读取第 2 次提交，checkpoint=3。

### 检查点 C1：S03 之后

- [ ] 本阶段任务的定向测试及相关回归通过，核验真实报告、无新增失败。
- [ ] 依赖、状态所有权与 Spec 保证一致，未引入模块外修改或未授权策略。
- [ ] 所有已完成单元已审查并本地提交，记录尚未关闭的决策/平台验证门槛；不自动开始受阻任务。

## S04：绑定读取 schema 并检测变化

- [ ] 完成并本地提交。

**说明：**加入持久化 schema 与计划 schema 的校验，保留原生历史投影，阻止新结构被旧映射静默读取。

**依赖：**S03。

**验收：**

- [ ] 初始化核验 TapTable 与递归字段 id/name/type/nullability/顺序、主键、分区键和读取语义。
- [ ] 运行中不兼容新 plan 在第一条入队前失败；只改注释继续，计划类型或元数据不可核验时不放行。
- [ ] 历史 schemaId<=初始 B 的加列/改名由真实 Paimon 投影验证，不能一概拒绝旧文件。

**验证：**F PaimonReadSchemaGuardIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonReadSchemaGuard.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonReadSchemaGuardIntegrationTest.java`

**规模：**S，2 个文件。

**证据与本地提交：**待执行。

## S05：完整读取单表 plan

- [ ] 完成并本地提交。

**说明：**在 Coordinator 中实现一次计划读取及 EventItem/PlanFinished 结果边界，先以单表测试驱动，不接生产入口。

**依赖：**S04。

**验收：**

- [ ] 直接 restore(next)，串行 checkpoint/plan/checkpoint；schema/range 检查位于事件入队前。
- [ ] 全部 split/iterator/reader 成功才生成标记；转换、关闭、取消或队列错误不生成。
- [ ] 真实多 split/多 batch、零 split 有进展与无进展区分，reader 复用现有受控资源工具。

**验证：**F PaimonStreamPlanIntegrationTest,PaimonStreamPlanFailureTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadCoordinator.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamPlanIntegrationTest.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamPlanFailureTest.java`

**规模：**M，3 个文件。

**证据与本地提交：**待执行。

## S06：按 plan 边界交付候选位点

- [ ] 完成并本地提交。

**说明：**实现 Session 的单一交付循环，以 fake PDK consumer 验证数据序列、候选 offset、刷新与心跳。

**依赖：**S05。

**验收：**

- [ ] plan 未完整交付前只携带旧 next；完成标记仅推进对应表，事件/Map 不复用修改。
- [ ] 精确 batch 边界及零业务事件进展发合法 Heartbeat，不发空列表；低流量按可控时钟刷新。
- [ ] 回调抛错立即终止；单表真实 Paimon 到 PDK 测试回调贯通，返回不被当 durable ack。

**验证：**F PaimonStreamDeliveryTest,PaimonStreamReadIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadSession.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamDeliveryTest.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamReadIntegrationTest.java`

**规模：**M，3 个文件。

**证据与本地提交：**待执行。

### 检查点 C2：S06 之后

- [ ] 本阶段任务的定向测试及相关回归通过，核验真实报告、无新增失败。
- [ ] 依赖、状态所有权与 Spec 保证一致，未引入模块外修改或未授权策略。
- [ ] 所有已完成单元已审查并本地提交，记录尚未关闭的决策/平台验证门槛；不自动开始受阻任务。

## S07：多表公平调度与无进展重派发

- [ ] 完成并本地提交。

**说明：**为 Coordinator 接入有界 worker 调度和完成 mailbox，消除永久循环与空表 outstanding 泄漏。

**依赖：**S06。

**验收：**

- [ ] worker=1/三表均读取；单表同时一个 outstanding，scheduler 不执行 I/O，不无限提交。
- [ ] NoProgress/Delivered 串行释放一次；空表随后写入、read delay、旧 token、拒绝提交均有明确状态。
- [ ] 队列背压可取消；连续空提交与阻塞 plan 记录原生长调用边界，不虚构绝对公平。

**验证：**F PaimonStreamReadCoordinatorTest,PaimonStreamReadIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadCoordinator.java`
- `src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadSession.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamReadCoordinatorTest.java`

**规模：**M，3 个文件。

**证据与本地提交：**待执行。

## S08：接入现有停止与资源所有权

- [ ] 完成并本地提交。

**说明：**在本模块 Service 增加源会话登记和取消接缝，复用已有控制器/资源账本，先用受控注入验证。

**依赖：**S07。

**验收：**

- [ ] 取消与准入票据原子化；停止先取消 source，再等待 ingress，避免等待自身永久调用。
- [ ] 满队列、半 plan、已准入回调、阻塞 reader 及关闭失败可复现；失败保留资源且不伪造完成。
- [ ] 首错保留、关闭错 suppressed、started/ended 配对，既有有限读与写停止无新增失败。

**验证：**F PaimonStreamStopTest,PaimonServiceCloseTest,PaimonBoundedStopTest,PaimonFiniteReadLifecycleTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadSession.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamStopTest.java`
- `src/test/java/io/tapdata/connector/paimon/service/PaimonServiceCloseTest.java`

**规模：**M，4 个文件。

**证据与本地提交：**待执行。

## S09：新增源端配置校验

- [ ] 完成并本地提交。

**说明：**添加轮询/刷新间隔及 Consumer 模式配置，保证 UI 与配置解析一致并拒绝表级误覆盖。

**依赖：**S08。

**验收：**

- [ ] 三项配置按 Spec 范围、枚举及草案默认值校验，非法值明确失败。
- [ ] 现有三语言文案完整，位于源端高级配置，不混用写 commit interval。
- [ ] 拒绝源选项表级覆盖，不新增 TTL/legacy 猜测/线程数配置；D02 未确认不能视为默认政策定案。

**验证：**F PaimonConfigTest,PaimonSpecTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/config/PaimonConfig.java`
- `src/main/resources/spec.json`
- `src/test/java/io/tapdata/connector/paimon/config/PaimonConfigTest.java`
- `src/test/java/io/tapdata/connector/paimon/PaimonSpecTest.java`

**规模：**M，4 个文件。

**证据与本地提交：**待执行。

### 检查点 C3：S09 之后

- [ ] 本阶段任务的定向测试及相关回归通过，核验真实报告、无新增失败。
- [ ] 依赖、状态所有权与 Spec 保证一致，未引入模块外修改或未授权策略。
- [ ] 所有已完成单元已审查并本地提交，记录尚未关闭的决策/平台验证门槛；不自动开始受阻任务。

## S10：切换 none 模式能力入口

- [ ] 完成并本地提交。

**说明：**同时接通新版时间初始化与流读恢复，保持 PDK consumer 类型，替换旧循环并去掉隐式 +1。

**依赖：**S09；D01/D02 确认；E01 启动路径证据。

**验收：**

- [ ] 正常 capability 从初始化 envelope 到 next=N 恢复读取 N 全链路成立，不允许新旧协议入口混搭。
- [ ] 缺表/元数据失败整体失败；并发 invocation 拒绝；none 不写 Consumer、不启动续租、不依赖 stateMap。
- [ ] 保留 codec/RowKind/回调协议；未接通的 fixed-start 明确报错，不偷偷降为 none；本提交不作为完整发布。

**验证：**F PaimonStreamCapabilityTest,PaimonStreamReadIntegrationTest,PaimonStreamStopTest,PaimonConnectorControlTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/PaimonConnector.java`
- `src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `src/test/java/io/tapdata/connector/paimon/PaimonStreamCapabilityTest.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamReadIntegrationTest.java`

**规模：**M，4 个文件。

**证据与本地提交：**待执行。

## S11：注册固定起点 Consumer

- [ ] 完成并本地提交。

**说明：**独立实现基于 readEpoch/attempt/table 的固定 Consumer 注册与读回，验证真正写侧过期边界。

**依赖：**S03；按顺序在 S10 后执行。

**验收：**

- [ ] 恢复 next 增长但 Consumer 始终=start，attempt 文件不覆盖；不调用 notifyCheckpointComplete 推进。
- [ ] 持久化 TTL/起点范围严格核验，注册前后检查；snapshot/changelog 与 manifest/data 缺失保留原始错误。
- [ ] 独立 writer 的清理与注册交错可复现；失败部分注册文件不删除，不声称原子建立保护。

**验证：**F PaimonConsumerLeaseIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonConsumerLease.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonConsumerLeaseIntegrationTest.java`

**规模：**S，2 个文件。

**证据与本地提交：**待执行。

## S12：实现注册超时和独立续租

- [ ] 完成并本地提交。

**说明：**为租约添加初始化 deadline、无 I/O watchdog、有界续租执行和 sticky failure。

**依赖：**S11。

**验收：**

- [ ] 首次写/读回卡住也有 deadline；续租使用成功操作开始时间保守计算风险期限，迟到成功不复活。
- [ ] 同 Consumer 无并发重入、队列有界；业务背压不挡心跳、存储阻塞不挡 watchdog、重试不越期限。
- [ ] TTL 变化、读回不符、旧 mtime 删除与停租均有测试；正常/失败 stop 不删 Consumer。

**验证：**F PaimonConsumerLeaseTest,PaimonConsumerLeaseIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/read/PaimonConsumerLease.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonConsumerLeaseTest.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonConsumerLeaseIntegrationTest.java`

**规模：**M，3 个文件。

**证据与本地提交：**待执行。

### 检查点 C4：S12 之后

- [ ] 本阶段任务的定向测试及相关回归通过，核验真实报告、无新增失败。
- [ ] 依赖、状态所有权与 Spec 保证一致，未引入模块外修改或未授权策略。
- [ ] 所有已完成单元已审查并本地提交，记录尚未关闭的决策/平台验证门槛；不自动开始受阻任务。

## S13：贯通全量至增量保护生命周期

- [ ] 完成并本地提交。

**说明：**在时间初始化阶段注册租约，持续覆盖全量，streamRead 复用匹配会话并传播后台失败。

**依赖：**S10、S12。

**验收：**

- [ ] 所有请求表保护完成才返回初始化 offset，full 阶段仍续租；不同 Service 使用独立 attempt。
- [ ] 全量阶段 sticky failure 在后续源操作/回调与流读入口可见；不持有阻止 Service stop 的永久 ingress。
- [ ] 独立 writer 清理跨越原保留窗口，固定 start 仍可恢复；租约停止不删文件、不自动重新锚定。

**验证：**F PaimonStreamProtectionIntegrationTest,PaimonFiniteReadLifecycleTest,PaimonStreamStopTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadSession.java`
- `src/main/java/io/tapdata/connector/paimon/read/PaimonConsumerLease.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamProtectionIntegrationTest.java`

**规模：**M，4 个文件。

**证据与本地提交：**待执行。

## S14：验证表模式与恢复语义

- [ ] 完成并本地提交。

**说明：**用 Paimon 1.3.2 实表验证 schema、RowKind、changelog、overwrite 的实际支持矩阵和恢复事件序列。

**依赖：**S13。

**验收：**

- [ ] 验证实际支持的 append/主键、none/input/lookup/full-compaction、overwrite 默认/显式组合，拒绝组合有清晰结果。
- [ ] 历史过期、解耦合法/缺口、schema 改变、半 plan 重放均有逐事件/offset 断言。
- [ ] RowKind 转换和 referenceTime 保持既有行为；不把状态同步描述成完整事务流，不把未测组合列为通过。

**验证：**F PaimonStreamReadIntegrationTest,PaimonReadSchemaGuardIntegrationTest,PaimonReadPositionIntegrationTest；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/test/java/io/tapdata/connector/paimon/read/PaimonStreamReadIntegrationTest.java`
- `src/test/java/io/tapdata/connector/paimon/read/PaimonReadSchemaGuardIntegrationTest.java`
- `docs/validation/stream-read-table-matrix.md`

**规模：**M，3 个文件。

**证据与本地提交：**待执行。

## S15：验证真实 Engine 故障恢复

- [ ] 完成并本地提交。

**说明：**在不修改内核的前提下，执行源/目标真实链路的进程退出、心跳与旧 attempt 退出场景。

**依赖：**S14；授权测试环境可用。

**验收：**

- [ ] 覆盖 plan 中途、回调后目标未完成、目标完成但 offset 未保存、保存后重启，允许重复但不能有连接器引入的缺口。
- [ ] 普通/异步目标/多目标的 Heartbeat 不越过 DML，正常与全量+增量恢复入口成立，旧 attempt 无迟到有效交付。
- [ ] 结果区分本地测试与平台事实；不适配组合明确限制，缺环境保持未完成，打包如需改变先按 Plan §4 单独提交。

**验证：**按脚本说明执行真实 Engine 矩阵；回读持久化 offset 与目标事件，记录版本与日志；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `src/test/scripts/stream-read-engine-probe.sh`
- `docs/validation/stream-read-engine-contract.md`
- `docs/validation/stream-read-recovery-results.md`

**规模：**M，3 个文件。

**证据与本地提交：**待执行。

### 检查点 C5：S15 之后

- [ ] 本阶段任务的定向测试及相关回归通过，核验真实报告、无新增失败。
- [ ] 依赖、状态所有权与 Spec 保证一致，未引入模块外修改或未授权策略。
- [ ] 所有已完成单元已审查并本地提交，记录尚未关闭的决策/平台验证门槛；不自动开始受阻任务。

## S16：最终回归与提交账本复核

- [ ] 完成并本地提交。

**说明：**核对 Spec 全部验收项、测试结果、来源注释和提交范围，整理最终本地交付证据。

**依赖：**S15；全部检查点通过。

**验收：**

- [ ] 本模块完整回归通过，或对既有失败提供同环境证据与明确处理结论；新增测试不得失败或意外跳过。
- [ ] 逐项闭环 Spec 与审查 R1–R8/两项接缝，源码引用完整，D01/D02 和平台支持范围已记录。
- [ ] 实现单元均有本地 commit，变更只在模块，工作树干净；未验证边界明确，不 push/merge。

**验证：**G；git diff --check；git log 与 baseline..HEAD 文件范围核对；首次/必要构建使用 B；人工复核事件/位点证据及来源注释。

**预计文件：**

- `docs/validation/stream-read-delivery.md`
- `docs/specs/paimon-stream-read-connector-only-spec-v1.md`
- `docs/specs/paimon-stream-read-connector-only-review-v1.md`
- `tasks/plan.md`
- `tasks/todo.md`

**规模：**M，5 个文件。

**证据与本地提交：**待执行。

### 检查点 C6：S16 之后

- [ ] 本阶段任务的定向测试及相关回归通过，核验真实报告、无新增失败。
- [ ] 依赖、状态所有权与 Spec 保证一致，未引入模块外修改或未授权策略。
- [ ] 所有已完成单元已审查并本地提交，记录尚未关闭的决策/平台验证门槛；不自动开始受阻任务。

## 提交与验证账本

| 项目 | 状态 | 证据 |
|---|---|---|
| Spec/Review | 已本地提交 | d6c62884；固定 Paimon 1.3 源码审查。 |
| Plan/Todo | 本轮编写 | 提交后以本分支 docs 提交及最终交付消息为准。 |
| Java 实现/测试 | 未开始 | 本轮没有执行 B/F/G 或 Engine 验证。 |
| D01 旧位点迁移 | 已明确方向 | 用户要求按 Spec 全新实现、不保留旧实现；拒绝歧义旧格式。 |
| D02 Consumer 默认值 | 按 Spec 实施 | none 默认；fixed-start 显式开启。 |
| E01 平台适配 | 未验证 | S01/S15 记录实际 Engine/PDK/目标组合。 |
