# Paimon Plus 同机恢复实施任务

对应[实施计划](plan.md)和[设计规范](../../src/doc/specs/SPEC-paimon-local-recovery.md)。勾选项表示已实施并有本地证据。仍涉及真实引擎/Mongo 故障或特定断点组合的验收保持开放。

本轮审查后的上线边界：已有绑定仅恢复目标端已接收的批次，正式节点再次启动会在开放输入前停止。源端真实恢复位置不通过当前目标 PDK 暴露。T01、T12、T15 中的跨重启自动追平仍是待验收的业务目标。已有错误数据需按冻结源位点重建或对账；连接器无法从已经合并的两个键推回旧值。

### 2026-11-19 实现及验收记录

| 范围 | 实现入口和实际证据 |
| --- | --- |
| 写入确认与重放 | `PaimonService.writeRecords` 在批次返回前将原生 prepare 交给 `PreparedMessageStore`。固定桶多表测试使用独立 JVM、停止或强退、静默启动和再次启动，使用普通 batch 扫描核验键值。重复发送相同 CDC 更新和删除后检查全部剩余键。对应 `PaimonStartupRecoveryIntegrationTest`。 |
| 目录和提交身份 | 节点 KV、主机、目录及固定锁文件身份绑定。表清单保存连续 prepare 序号、有序摘要和已确认的世代基线。查 `PreparedMessageStoreTest`、`PreparedMessageCodecTest`、`PaimonCommitStateStoreTest`。含跨 JVM 争锁、陈旧注册表、损坏有效尾、同步故障及确认后 GC 恢复。 |
| 发布、维护与历史 | `PublicationVerifier` 对照 Paimon 1.3.2 原生表提交器的 APPEND/COMPACT 变化和有序索引。已确认的快照通过 manifest 指纹及文件元数据核查。固定桶见 `PaimonStagingRecoveryIntegrationTest`；停止维护线程见 `LocalRecoveryCommitterTest`。 |
| 桶与 spill | `PaimonHashIndexRecoveryIntegrationTest`、`PaimonKeyDynamicRecoveryIntegrationTest` 在子进程准备后强退，移除旧 spill 后恢复并对后继更新、删除逐键核查。哈希动态桶反序对照产生重复键。跨分区键动态桶给索引设置有限 TTL 的负对照留下旧分区记录。 |
| 生命周期及配置 | `PaimonRecoveryAdmissionTest`、`PaimonRecoveryHeartbeatIntegrationTest`、`PaimonRecoveryDdlIntegrationTest` 校验写入前准入、跨表与未知来源心跳、隐式降级，以及截表或世系变化。启用前应核对源锚点、同键顺序、外部独占和仓库保留。 |
| 生产验收 | [TapData 源位点夹具和故障边界](e2e/README.md)以及[固定桶三轮容量对照](e2e/CAPACITY.md)。夹具调用本地引擎方法并替换持久化边界。真实 Mongo 连接在 ping 阶段被当前执行环境拒绝。实际源 CDC 回放、Mongo 持久化、硬件断电和生产形态容量仍需独立取证。 |

**本次构建。** JDK 17、Maven 3.9.16、Paimon 1.3.2。目标模块 1,434 项通过，RustFS 外部集成用例 1 项按环境条件跳过。另有 5 项 S3/HDFS 原有契约用例在单独实跑时因本机 socket 被拒绝而报错。排除这 5 项后运行 `verify` 生成连接器包，上游模块 82 项均通过。受限项为 `PaimonS3ConditionalWriteContractTest` 的 3 项和 `PaimonHadoopFileIoOwnershipTest` 的 2 个 `serviceRestartMustReuse*` 方法。该结果未覆盖生产 Mongo 的真实持久化边界。

**验证约定。** F(类名) 在 worktree 根目录、JDK 17 运行 `mvn -pl connectors/paimon-plus-connector -am -DskipTests=false -Dtest=<逗号分隔类名> -Dsurefire.failIfNoSpecifiedTests=false test`。此参数允许上游模块没有匹配测试，在执行前记下开始时间，检查目标模块每个指定类均有本次新生成的 Surefire XML，tests > 0、failures/errors/skipped 均为 0。计划的新测试名统一用 *IntegrationTest 以匹配仓库 Surefire 默认发现规则。B 为 `mvn -pl connectors/paimon-plus-connector -am -DskipTests=false verify`，逐类对照本次 XML 清单，防止计划测试静默跳过。E 是[计划中的引擎故障用例](plan.md#引擎故障用例)，需保留源基准、Mongo 记录、快照元数据和普通批读结果。生产代码的新增类均放在模块已有包树下。

## T01 复核全量转 CDC 的源端起点

依赖 无。规模 S。预计涉及 隔离 TapData 任务、Mongo 进度导出（测试工件）。

- [ ] A 初始为空或已读完，在全量与首次成功 CDC 心跳之间插入/删除并中断；对普通串行、并发、仅全量、纯 CDC 分开取证。
- [ ] 记录源连接器/version、任务模式、错误策略、初始 P0，以及 source flush 成功/Mongo 写失败的保留行为。同时保存 Mongo 重启前实际值、重启真正传给源连接器的起点及普通查询结果。无法证明覆盖原始变更的组合在准入清单上关闭。
- [ ] 验证：E(anchor)，对照关闭首次心跳或令 Mongo 写入失败，再验证有可重复起点的正例；勿以源时间代替位点；受测方法/版本和初始心跳构造见 Spec §8–9。

## T02 固定桶原生提交反例固化

依赖 无。规模 S。预计涉及 write/PaimonStagingRecoveryIntegrationTest（新增）、独立临时 warehouse fixture。

- [x] 真实 writer 多次 prepare、进程强退，后继 JVM 按顺序恢复、第三次重试；比较普通查询中的更新与删除。
- [x] 构造缺文件/文件原名仍在但截为零、同编号 APPEND 后缺 COMPACT、暂存后外部 truncate 的独立子用例，断言各自原生结果和读表结果。
- [x] 验证：F(PaimonStagingRecoveryIntegrationTest)，此步先固化原生行为，按 Spec §8 实验参数建干净仓库；主键值、K、snapshot kind、原消息、各进程退出点都要有断言。

## T03 判定源、目标和仓库是否具备恢复条件

依赖 T01、T02。规模 M。预计涉及 config/PaimonConfig、spec.json、schema/PaimonWriteSemanticContractResolver、config/PaimonConfigTest。

- [x] 关闭特性时沿用原行为。启用时检查可见能力、表消费方式、保留边界及有效配置，区分可以在连接器验证与需要由任务部署验证的条件。
- [ ] 纯全量/CDC/组合按证据独立准入。无保证的源起点、无法保护准备文件、允许竞态替换的表、POSTPONE 默认批读等情况在承诺生效前拒绝或标为未获准。准入记录绑定引擎/源/连接器版本、源模式、日志及硬删除能力、真实 Mongo 锚点、数据/表跳错开关、owner 与实际 cleaner 配置；变更需重新确认。
- [ ] 验证：F(PaimonConfigTest,PaimonWriteSemanticContractResolverTest)，单项改变配置做拒绝对照。

### C1 检查点

- [ ] 前组三项的用例实际执行并留下新报告。来源/配置表可排除初始位点丢失，记录原生反例而非将负例误标通过。

## T04 绑定任务目录与表世代

依赖 T03。规模 M。预计涉及 PaimonConnector、recovery/LocalRecoveryBinding（新增）、commit/PaimonCommitStateStore、recovery/LocalRecoveryBindingTest（新增）。

- [x] 目标任务以节点身份、宿主/规范路径和目录 UUID 绑定。首次绑定中途崩溃可恢复；目录克隆、错配和双 owner 被拦截，连接测试不触发认领。
- [x] 在调用可写的旧 bind/reconcile 前先核查未决意图、表世代、分支和基线。重用表名或重用数值 snapshot ID 不能继承旧进度。
- [ ] 验证：F(LocalRecoveryBindingTest,PaimonCommitStateStoreTest)，独立进程争用；旧标记复制到第二路径须失败。另测不同节点以不同目录写同一动态表，证明本机目录锁无法提供跨任务排他；部署未提供排他时不启用。

## T05 持久化一份可恢复的原生清单

依赖 T04。规模 M。预计涉及 recovery/PreparedMessageStore（新增）、recovery/PreparedMessageStoreTest（新增）、recovery/PreparedMessageCodecTest（新增）。

- [x] 使用 Paimon 消息版本与整批校验，包含表世代、来源、prepare ID 和连续批号。强制同步文件、原子替换状态、同步父目录后才承认有效尾。
- [x] 新 JVM 能扫描全部登记表。截断有效尾外的临时记录；有效段损坏、长度异常、格式不识别、磁盘满均失败，不能回退已确认水位。
- [ ] 验证：F(PreparedMessageStoreTest,PreparedMessageCodecTest)，分别在 force / 同文件系统原子 rename / 父目录同步前后强退，并注入每一操作失败，断言无 ACK、回调和水位推进。不支持所需原子/同步语义的文件系统拒绝启用；进程强退不冒充断电测试。

## T06 固定桶从 DML 写入到批次确认

依赖 T03、T05。规模 M。预计涉及 service/PaimonService、write/PaimonTableWriteContext、PaimonConnector、service/PaimonPreparedAckTest（新增）。

- [x] 沿用现有类型转换与分桶，在可独立成功的目标调用返回前登记完整有效尾。批内部分写入或 prepare 已消费增量后发生异常，冻结 writer 和该次成功路径。
- [ ] 重复调用同一 Java 批、返回前退出、坏行在更新旧行和新行之间失败，均不把半批计为完成。确认后再也不收到该表输入时仍有可用清单。
- [ ] 验证：F(PaimonPreparedAckTest,PaimonServiceInitialSyncPendingTest)，记录真实回调顺序；移走落盘屏障应使负对照失败。本步通过夹具断言 B 同批重入被隔离；T12 再用真实引擎验收随后的 OVER/H 及 Mongo 落点。

### C2 检查点

- [ ] 前组三项的用例实际执行并留下新报告。固定桶在持久有效尾前不返回成功，损坏和磁盘失败有实际负对照。

## T07 封存不可变的有序提交区间

依赖 T05、T06。规模 M。预计涉及 recovery/PublicationIntent（新增）、write/PaimonTableWriteContext、commit/PaimonCommitStateStore、recovery/PublicationIntentTest（新增）。

- [x] 在首次提交之前固定所有子区间、原消息顺序、摘要、每组 K 及其 predecessor。根据最早 writer 基线校验来源，后续读取时保留 prepare 内部顺序。
- [x] K 在表世代/分支/commitUser 下单调且不能溢出。按原计划逐组推进连续前缀；部分完成后改变 chunk 大小、重启或发现 KV 超前也不能重分组或复用同 K 对应其他消息。
- [ ] 验证：F(PublicationIntentTest,PaimonCommitStateStoreTest)，提交第 1 组后崩溃，修改阈值后对比分组 ID、字节、顺序。

## T08 按真实文件和快照效果确认发布

依赖 T02、T07。规模 M。预计涉及 recovery/PublicationVerifier（新增）、write/PaimonStreamTableCommitter、write/PaimonTableWriteContext、recovery/PublicationVerifierTest（新增）。

- [x] 首发和重试先依据原元数据核对适用数据/索引长度，再使用有文件检查的原生路径并独立核对保留的历史。缺文件或文件截断均须在原生提交前失败，无新 snapshot、无 published、无回调。核对业务 delta、索引有效映像与已公开给普通读端的效果，确认后才可推进 published。
- [x] 覆盖同 K 部分提交、零返回、缺文件、截止基线失效和非空输入的空消息。只有显式验证的维护工作可重建；不能只看 K、行数或文件名。
- [ ] 验证：F(PublicationVerifierTest,PaimonStagingRecoveryIntegrationTest)，将 T02 的原消息送入真正的连接器确认分支和第二 JVM；只用原生函数的对照不抵此验收。移除长度校验或改成仅看编号，反例必须失败。

## T09 同机启动先补交旧前缀

依赖 T07、T08。规模 M。预计涉及 PaimonConnector、service/PaimonService、recovery/PreparedMessageStore、service/PaimonStartupRecoveryIntegrationTest（新增）。

- [x] 所有登记表先处理旧意图，再补交完整前缀，再开放新 writer。原子落本地确认、核对 KV 后才 GC 自己的清单；首次全量重启可以额外发布一次。
- [ ] 测试 Mongo 中 A=OVER、B 未完及后续完全静默；失去提交响应、确认后 KV 失败、GC 中断、无进展的第二次重启均保持逐键结果。
- [ ] 验证：F(PaimonStartupRecoveryIntegrationTest,PaimonServiceInitialSyncPendingTest)，E(quiet)；源已排队时，连接器的目标入口仍须等待旧前缀恢复或明确失败，不能靠控制源的启动顺序兑现。

### C3 检查点

- [ ] 前组三项的用例实际执行并留下新报告。同机恢复在首个新输入前完成；缺文件、半提交和世系分叉均未误清理。

## T10 验证动态哈希桶后继路由

依赖 T06、T08、T09。规模 S。预计涉及 write/bucket/HashDynamicBucketWriterStrategy、write/PaimonHashIndexRecoveryIntegrationTest（新增）。

- [x] 同桶多次更新累计 hash 索引，有序原消息恢复后引导新 assigner，填桶再更新/删除。比较物理桶、唯一键与普通批读。
- [x] 将清单反序作为阴性；首次查询即使通过，后继写入应检出重复或错误。对比每桶有效映像，禁止按文件名排序重放。
- [x] 验证：F(PaimonHashIndexRecoveryIntegrationTest)，先断言至少两份针对同桶的索引更新，负对照实际命中。

## T11 验证跨分区键与删除向量的适用范围

依赖 T06、T08、T09。规模 S。预计涉及 write/bucket/KeyDynamicBucketWriterStrategy、write/PaimonKeyDynamicRecoveryIntegrationTest（新增）。

- [x] 旧进程写入并搬键，恢复发布后重建全局索引，新 writer 再搬迁并删除。断言旧分区查不到、目标只有一个最新主键。
- [x] 无 TTL、DV 关闭是独立基线。首版拒绝有限 TTL；建表拒绝 cross-partition + sequence.field。DV 开启属于独立的可选准入，必须有真实非空 DV、先后快照及跨分区删除对账。
- [x] 验证：F(PaimonKeyDynamicRecoveryIntegrationTest)，记录 bootstrap 基线、分区/桶和普通批读结果；只在申请 DV 开启的组合时要求非空向量，空 DV 不计该组合通过。

## T12 按来源控制 CDC 窗口与心跳

依赖 T01、T03、T06、T08、T09。规模 M。预计涉及 commit/PaimonMicroBatchCoordinator、commit/PaimonAsyncCommitScheduler、service/PaimonService、service/PaimonRecoveryHeartbeatIntegrationTest（新增）。

- [x] 同一来源覆盖所有依赖表；尚未覆盖全部批号时扣住心跳。成功发布后锁外回调。用单调时钟按期发布，有错误先锁存并让心跳处理安全返回。
- [ ] 串行拆批、并发 watermark、只剩心跳、跨来源共享表、错误策略分别取证。首版每次 ACK 前 prepare；可重放性未单独验证前不启用纯 CDC 窗口内延迟 prepare。
- [ ] 验证：F(PaimonRecoveryHeartbeatIntegrationTest,PaimonMicroBatchCoordinatorTest)，E(heartbeat)，用同一引擎组 A 成功、B 失败和 B 重试后依次出现 OVER/H 的时序，分别核对串行/并发、跳错开/关时的 Mongo 与已发布前缀。

### C4 检查点

- [ ] 前组三项的用例实际执行并留下新报告。动态桶的新 writer 已对账，CDC 的 Mongo 落点已验证。无证据的能力继续关闭。

## T13 处理停机、超时与失效 owner

依赖 T06、T08、T09、T12。规模 M。预计涉及 service/PaimonService、write/PaimonTableWriteContext、service/PaimonStopController、service/PaimonRecoveryStopIntegrationTest（新增）。

- [x] 冻结 ingress，保留开放的全量清单。未确认文件的所有权经过真实 close 路径验证，迟到线程不能确认、GC 或触发新 owner 的回调。
- [ ] 在正常 stop、强停、close 中途退出和 dispose 超时时检验最终 Mongo 位点及同机重启，依赖旧 worker 仍活跃时不得释放独占权。
- [ ] 验证：F(PaimonRecoveryStopIntegrationTest,PaimonServiceSyncStopIntegrationTest)，E(stop)，逐项对照 prepare、资源所有者退出及最后一次写位点的顺序。

## T14 隔离 DDL 与外部历史改写

依赖 T04、T08、T09、T12。规模 M。预计涉及 service/PaimonService、recovery/PublicationVerifier、service/PaimonRecoveryDdlIntegrationTest（新增）。

- [x] 覆盖 create/drop/truncate、字段变更与同名复用。截表事件无跨重放身份或结果不可证时，在执行前阻止或从受控新基线恢复。
- [x] 覆盖已确认、清单已 GC、本机无 pending 的重启。外部 truncate/rollback 后数值 snapshot ID、UUID 或 schema 未变时仍能发现基线不符。独占承诺之外的竞态不能通过一次前置查询消除。
- [ ] 验证：F(PaimonRecoveryDdlIntegrationTest,PaimonServiceTableDdlCacheInvalidationTest)，E(ddl)，断言不会把旧清单用到新世代。

## T15 同机故障矩阵与生产准入

依赖 T01、T10、T11、T12、T13、T14。规模 M。预计涉及 隔离的源/Mongo/warehouse 对账 fixture、恢复验收报告、节点配置说明。

- [ ] 冻结一个可重放源位点 P，对照基准逐键比较值、删除及目标多余键。记录每类源、桶、版本、writer 配置、真实 Mongo 落点、断点命中和两次重启结果。
- [ ] 按最早相关文件的 mtime 核对仓库保留至恢复末的窗口，包含清理竞态与已确认历史的保留；验证停机超界时不自动续跑。测量每桶文件数/大小、末尾提交内存、本机峰值和 CDC 延迟。只开放通过组合，记录旧版本排空、对账和回滚演练。
- [ ] 验证：E(acceptance) + B，检查任务集未启用模式回归；与相同硬件/批大小/分布/事件规模的旧版本对照，按部署方给定阈值报告快照/小文件、堆与磁盘峰值、延迟及恢复时间，未给阈值只报告实测，不宣布性能达标。

### C5 交付检查点

- [ ] B 和 E(acceptance) 完成，同机重启、受控回滚与运行配置清单可复核。记录通过与拒绝的部署组合及各项资源上限。
