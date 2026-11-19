# Paimon Plus 同机恢复与 TapData 引擎联动审查

2026-11-19。审查对象为连接器 `spec/paimon-exactly-once@c2753e90c407`、Apache Paimon `release-1.3.2@c05f7d1f1b1e`、TapData `65add935fdee`，在 JDK 17 下复现。**审查结论为 Request changes。** 目标提交清单能跨进程恢复，当前准入仍允许成功确认违反逐主键一致性的数据。建议修复下列 P1 后再开放这项承诺。

本报告记录 `c2753e90c407` 上的原始反例。后续修复通过目标端准入和写前校验挡住主键合并、漏列、默认值改写及 fallback 遮行；真实送达的初始 P0 在所属来源满足提交条件后回调。已绑定节点再次启动时先恢复目标清单，再以 `PAIMON_RECOVERY_SOURCE_POSITION_UNVERIFIED` 拒绝新输入。Manager 旧请求覆盖新位点的现象仍可复现，因此当前目标连接器不能承诺跨重启自动追平。修复后的回归命令和断言见[当前联动测试说明](../../../tasks/local-recovery/e2e/README.md)。

## 判定标准

在同一源位点 P 比较映射后**完整的主键集合、每个键的字段值以及已删除的键**，同时检查重复物理键。为了在任意一次同机中断后仍满足这个条件，需要同时成立：

1. 不同合法源主键可被目标区分，写入后的字段值符合选定映射。
2. 引擎可以跳过一个批次时，目标已可查询，或能仅凭原机持久状态补交该批次。
3. 同一来源的心跳必须覆盖该来源已交付的所有目标表。重启能够从覆盖尚未传给目标的变更的源锚点重读。
4. 用业务的普通 Paimon 批读验收。快照存在、同一批重复读一致、行数相等，都不能替代源目标对账。

## 阻断项

### P1 · 首次全量转 CDC 的起始位点 P0 可能缺失

**并发路径**实际运行了 `PartitionConcurrentProcessor` 的两个 worker 和 watermark consumer。带 `streamOffset=P0` 的 INITIAL 心跳已到 Paimon 的 `processControl`，服务对 INITIAL 直接返回。引擎正常返回后交由连接器决定是否回调。保存空表 A 的 `OVER` 后重启，恢复方法走 `initStreamOffsetFromTime(null)`，没有得到 P0。同一实例收到后续 CDC 心跳 P1 时能够保存并解码 P1。**普通串行路径**中，只有 streamOffset 的 INITIAL 心跳会在到达目标连接器之前被引擎筛掉。如果源从当前位置开始，P0 与重新取点之间未投递的更新、删除有被跳过的风险。本地 MySQL 连接器的 `MysqlConnector.timestampToStreamOffset` 在时间为空时读取当前 binlog 位点。夹具实证到恢复分支，未测量真实源上的漏行数。

在仅升级连接器的约束下，对并发路径识别、保留可证明属于全量起点的 P0，验证来源、心跳顺序、恢复阶段及位点回调载荷。不能将任意 INITIAL 心跳当作安全锚点。对于串行路径，仅在源端有独立、可重复的全量转流起点时开放这项保证；目标端无法还原从未收到的事件。用空表、首次 CDC 心跳前更新或删除、同机重启作为验收用例。[连接器心跳处理](../../main/java/io/tapdata/connector/paimon/service/PaimonService.java) · [真实并发方法探针](../../../tasks/local-recovery/e2e/EngineConcurrentAnchorProbe.java)

### P1 · 已成功保存的首个 CDC 位点可能被旧全量进度覆盖

在真实引擎方法与可控 Manager 持久化边界之间延迟一个较早的请求。旧请求载荷是 A=OVER、流位点为空。随后真实 Paimon 回调保存 P1，调用源恢复方法已经能解码 P1；放行旧请求后，两次保存都成功返回，再恢复却只剩 A=OVER，源重新进入从当前位置取点的分支。另一个对照中，已有流位点的旧 P1 晚于 P2 落地，重启读到 P1，源端 flush 已收到 P2。前者可能留下无法用目标恢复清单补齐的变更缺口，后者在源能从 P1 重读的前提下扩大重放窗口。测试注入了完成顺序，未测量发生频率。

引擎的定时保存与回调保存均可调用 `saveToSnapshot`；复制进度时的锁没有把请求完成顺序串行化。Manager 的 `TaskServiceImpl.updateSyncProgress` 以任务 ID 筛选，逐 edge 对 `attrs.syncProgress.<edge>` 执行无条件 `$set`。生产只升级连接器时，方案需要覆盖整个风险窗口的独立可恢复源起点以及足够的日志保留；缺少这些条件的全量转 CDC 组合应排除出承诺。目标收到过成功回调不足以保证引擎下次重启仍能读到那个位点。[乱序保存探针](../../../tasks/local-recovery/e2e/EngineConcurrentAnchorProbe.java)

### P1 · 主键映射可合并两条源记录，已有物理表还可能漏业务列

- 已有物理表的 `id` 是 FLOAT，TapData 节点模型是 INT。字段都叫 `id`，准入通过。向空表插入 `16777216→first`、`16777217→second`，整个调用成功，快照 1 只剩 `16777216→second`。另把映射中的 `payload` 从已有物理表移除，源写入的 `payload=source-value` 同样被确认，目标没有该列。
- 时间主键的损失发生得更早。经过实际 PDK codec，`12:34:56.123456` 和 `12:34:56.123789` 都变成毫秒 `45296123`。固定桶及动态哈希桶均观察到，含第三个对照键的三次插入只留下两行，删除第一个源键后只有对照键存活。分桶和恢复清单此时收到的已经是相同的键。线上历史任务用整数主键，本项是隔离反例。

TapData 目标节点按目标表名从节点 `TapTableMap` 取得传给 PDK 的模型。连接器需要同时核查该映射与物理 writer 布局，检测有意映射的字段被丢弃，并要求主键转换保持唯一性。对于转换前已携带亚毫秒的主键，需要在精度丢失前拒绝或使用无损编码。普通业务列的降精度策略应单独明示。基线已有这些类型转换；本次变更的问题是强模式准入仍给这些组合提供成功确认。[准入代码](../../main/java/io/tapdata/connector/paimon/schema/PaimonRecoveryAdmission.java) · [映射对账](../../../tasks/local-recovery/e2e/LocalRecoveryImageAudit.java) · [时间主键](../../../tasks/local-recovery/e2e/TemporalPrimaryKeyProbe.java)

### P1 · 首次插入的显式 NULL 会被目标默认值改写

已有目标列 `payload` 可空且默认值为 `DEFAULTED`。源全量发送 `{id:7,payload:null}`，连接器确认写入，快照 1 读回 `DEFAULTED`。对照表移除默认值时读回 NULL；同一默认值表的 CDC UPDATE_AFTER 显式设为 NULL 则在发布前拒绝，原值及已有快照保留。输入缺字段与字段明确为 NULL 的语义需要在 Paimon 默认值生效前区分。

强模式应对 INSERT 使用与一致性承诺匹配的镜像检查，拒绝或无损表示物理写入会改变的显式 NULL。Paimon 的默认值处理与旧写入路径均存在于基线；本次新增准入尚未封住这一情况。[批次镜像校验](../../main/java/io/tapdata/connector/paimon/schema/PaimonDmlImageValidator.java) · [原生 DefaultValueRow](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-common/src/main/java/org/apache/paimon/casting/DefaultValueRow.java)

### P1 · scan.fallback-branch 可以在发布后遮住已对账的旧行

主分支尚未写某个分区，普通批读从静态 fallback 分支读到 `id=1`，与源端初始基线完全一致。向同一分区插入 `id=2`，快照发布并回调 P1。普通读只返回 2；旧行仍存在于 fallback 分支。无 fallback 对照在相同位点读到 1 和 2。另一个独立仓库初始读到 2、3，插入 4 后只读到 4。两组实验均无需外部并发修改。

Paimon 1.3.2 按整个分区选择主分支或 fallback 分支。强模式当前只限定 `startupMode=LATEST_FULL`，建议在建表、接管以及恢复已有表时拒绝有效的 `scan.fallback-branch`。需要支持该选项时，应先物化完整目标基线并验证后续普通批读。[准入代码](../../main/java/io/tapdata/connector/paimon/schema/PaimonRecoveryAdmission.java) · [原生分区选择](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/FallbackReadFileStoreTable.java#L385) · [带 P1 回调的反例](../../../tasks/local-recovery/e2e/FallbackBranchReviewProbe.java)

## 已通过的联动路径和剩余边界

真实引擎目标控制方法、位点回调及源恢复方法与连接器、真实本机 Paimon 仓库在同一测试流程调用。源 DML 和预期键值由夹具给定，节点 KV 与 Manager 进度由可跨进程读写的测试存储提供。P1、P2 是不透明的测试位点。表 A、B 的期望结果独立于仓库读取结果。

| JVM | 故障点、观测与断言 |
| --- | --- |
| 1 | A 已记为 OVER，A/B 已返回全量写入成功，两表均无可见 snapshot，强制退出 42。 |
| 2 | 无新输入时两表各补交至快照 1，四条初始记录完整。之后更新主键、删除、修改 B 并发出心跳。B 待发布期间 P2 被扣住；两表达到快照 2 后已向源 flush P2，模拟 Manager 第二次保存失败，能恢复的位点仍是 P1。强制退出 43。 |
| 3 | 从 P1 重放至 P2，快照分别达到 3。两表普通批读严格等于 A=`{3:moved}`、B=`{7:changed,8:keep}`，旧键和删除键消失。此时进度可以读回 P2。 |

完整运行命令及各进程的断言见[引擎夹具与运行入口](../../../tasks/local-recovery/e2e/README.md)。

**现场任务** `6ad3a33039a815b40b1ea3bc` 的历史日志在 11 月 18 日 15:41 UTC 记录目标正常停止；此前仅有 `default.cdc_mysql_types` 一张表，写缓冲标记为 `spillable=false`。`compaction-terminated`、`spill-closed` 是停机资源事件。11 月 19 日可见 Engine、Manager 进程，现场插件目录所见 11 月 18 日 JAR 内没有本次新增的 `PreparedMessageStore`。本轮直接连接本机 UI、Manager 和 Mongo 被执行环境拒绝；Mongo 版夹具在 ping 阶段退出。没有取得这项任务在新版本下的真实 Mongo 进度、源数据库重放或实时逐行对账。此次没有改动生产实现、部署或现有任务。

## 回归与重复实验

- 目标模块 1,441 项通过，1 项 RustFS 环境用例跳过，上游 82 项通过，Maven `verify` 成功。5 项原有 S3/HDFS socket 用例单独实跑，因 `Operation not permitted` 失败。上述新增反例独立运行，不计入绿色的 Surefire 数字。
- 原生测试覆盖固定桶的 APPEND/COMPACT 不完整提交、截断文件、动态桶重新开 writer 后路由与跨分区删除。哈希动态桶的强退测试实际触发了 spill，移除旧 writer 的 spill 目录后重启并逐键核对。另在隔离副本中让长度校验仅拒绝过长文件，原有 2 个参数化用例中的截断分支立即失败；已恢复隔离副本。另在隔离副本反转微批协调器的世代比较，三进程夹具在 B 尚未提交时捕获到提前的 P2 回调。
- 本轮固定两桶、16 批 × 512 行、关闭自动压实的单轮对照。两种模式的全量都只发布 1 个快照，开启后数据文件由 2 个增至 32 个，总字节由 70,145 增至 104,724，未提交清单 22,128 字节。无新输入时恢复 8,192 键用时 958 ms。这些数据只描述该本地样本。[容量夹具与三轮基线](../../../tasks/local-recovery/e2e/CAPACITY.md)

在模块 `tasks/local-recovery/e2e` 下，设置 JDK 17 的 `JAVA_HOME`、本地引擎路径 `TAPDATA_REPO` 和 Maven 可执行文件 `MAVEN`，并先运行模块 `PaimonStartupRecoveryIntegrationTest` 生成实际 classpath。核心复现命令如下。

```sh
bash run-anchor-probe.sh --coupled
bash run-anchor-probe.sh --concurrent
bash run-review-probe.sh LocalRecoveryImageAudit
bash run-review-probe.sh FallbackBranchReviewProbe
bash run-review-probe.sh TemporalPrimaryKeyProbe fixed
bash run-review-probe.sh TemporalPrimaryKeyProbe dynamic
```

在被审查的旧提交上，前两项的成功退出表示脚本观察到预期的实验时序。映射和 fallback 的对账断言预期失败，代表已确认的数据与源不符。时间主键探针以观察到可复现的碰撞作为成功条件，需结合输出中的源目标键集解读。
