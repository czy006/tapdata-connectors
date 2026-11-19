# Flink CDC 多表写入与故障恢复 · Paimon 1.3.2

本次沿 Apache Paimon 的 MySQL 整库同步入口读取本地 `release-1.3.2` 标签，提交为 `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7`。本地 Paimon 工作目录检出的是另一个提交，下列引用固定到本次审查的源码版本。对照的 TapData 引擎为 `65add935fd`，Paimon Plus 为 `develop@c21910a`。本文记录静态证据和设计约束，未运行 Flink 作业或注入故障。

## 全量阶段的可见性

持续运行的“首次全量 + CDC”任务中，sink 处理 checkpoint barrier 时会准备已接收的数据，checkpoint 完成通知到达后即可发布相应的表快照。若 A 已有数据进入成功的 checkpoint，即使 B 的源端全量仍在读取，业务也可能查到 A，甚至只查到 A 的一部分。可见范围取决于源已发出的记录和成功 checkpoint 的时机。有界输入的 `endInput()` 对应整条输入结束；持续 CDC 从初始快照切到增量并未结束输入。[写入端的两个入口][barrier] · [提交端的完成通知][operator] · [1.3.2 CDC checkpoint 文档][cdc]

| MySQL 整库模式 | 写入与提交拓扑 | 桶模式边界 |
| --- | --- | --- |
| `divided`（默认） | 按表分流，为启动时已知的每张表构建 sink。新增源表需要重启作业才能纳入。 | builder 分别处理 `HASH_FIXED`、`HASH_DYNAMIC`、`POSTPONE_MODE` 和 `BUCKET_UNAWARE`，拒绝 `KEY_DYNAMIC`。 |
| `combined` | 共用多表 writer 算子，每个 writer 按表维护写入对象。提交消息按库表分发到 committer，同一表进入同一提交子任务；新增表可以动态纳入。 | 此版本的 CDC 多表路由显式要求 `HASH_FIXED`。 |

源码见 [同步模式][mode]、[按表建 sink][builder]、[共享 sink 的提交路由][combined]、[固定桶校验][route]。

## 从写入到恢复

1. **准备。** checkpoint N 的 barrier 到达时，writer 为各表调用 `prepareCommit`。输出包含表身份、checkpoint ID 和与文件有关的提交消息。此时业务表还可以没有本次写入的可见 snapshot。[PrepareCommitOperator][barrier] · [多表 writer][writer]
2. **保存待提交信息。** 提交算子按 checkpoint 汇总，再在每个 checkpoint 下按表组织 manifest。序列化内容包括 checkpoint ID、watermark、库表标识和各表的 manifest。提交算子把这份待提交清单写入 Flink operator state，writer 与 committer 使用的提交身份也可从状态恢复。恢复依赖清单引用的数据文件仍然存在。[提交身份][identity] · [提交清单序列化][serializer] · [状态读写][state] · [恢复时的文件检查][files]
3. **逐表发布。** 收到 `notifyCheckpointComplete(N)`，提交算子可处理截至 N 的待提交清单。`StoreMultiCommitter` 按表分组，再调用各表各自的 `StoreCommitter.commit`。checkpoint ID 标识同一轮恢复进度，各表分别推进自己的 snapshot ID。普通跨表查询可能看到 A 已更新、B 尚未更新。[通知处理][operator] · [逐表提交][multi]
4. **处理部分成功。** 假设 N 的状态已持久化，A 提交成功后发生故障，B 尚未提交。从 N 恢复时读取原始清单，按表调用 `filterAndCommit`。在同一提交身份、编号递增且成功提交仍可识别的条件下，A 被过滤，B 用原清单补交。内核按该表该 `commitUser` 的最新可查提交编号判断。若恢复实际补交了数据，组合 sink 使用的状态管理器会主动触发一次重启，使 writer 基于补交后的快照重新初始化。[按表补交][multi] · [内核过滤条件][filter] · [补交后重启][recover]
5. **上游的测试证据。** `testFailIntentionallyAfterRestore` 先保存算子状态，不发 checkpoint 完成通知，断言目标表还没有 snapshot；从该状态恢复会补交，再次恢复仍读到预期行。该用例先后对两张表操作。上面的“同一轮 A 成功、B 未提交”是根据逐表提交和逐表恢复代码推导的故障时序。[上游测试][test]

## 对仅升级 TapData 连接器的含义

本设计选定同机持久盘，允许事件重放，追平后以普通查询逐主键核对。正常全量在阶段完成信号后按表发布，CDC 按时间窗口提交。多来源和重启时的具体可见性见[连接器设计第 5 节](../specs/SPEC-paimon-local-recovery.md)。

新方案优先在确认全量批次前调用原生 prepare，将版本化提交清单保存在本机。数据与索引文件沿用目标 warehouse，待全量完成再发布。固定桶跨进程恢复结果和小文件成本见设计第 8 节。Flink 的 operator state 与源进度随同一 checkpoint 恢复；TapData 的 Mongo 位点与连接器状态独立。上面的 Flink 示例说明的是两张表之间的部分成功。一张表内部也可能以同一编号先后发布 APPEND 和 COMPACT。完整发布与只发布 APPEND 时，原生 `filterAndCommit` 都可能返回 0，实验与恢复判定见[连接器设计第 4、8 节](../specs/SPEC-paimon-local-recovery.md)。

验收可用两张有主键的表。在 TapData 已保存 A 的 `OVER`、B 仍在全量时中断进程，再在只发布了 A、尚未发布 B 时各中断一次。两种恢复都要在同一源位点逐主键核对更新、删除和行数；延迟发布模式还需检查故障前、正常停任务和重启期间是否提前露出部分全量数据。该场景也是区分普通 spill 与可靠恢复记录的直接验证。

[mode]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/action/MultiTablesSinkMode.java#L38
[builder]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc/FlinkCdcSyncDatabaseSinkBuilder.java#L225
[combined]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc/FlinkCdcMultiTableSink.java#L131
[route]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc/CdcMultiplexRecordChannelComputer.java#L83
[barrier]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/PrepareCommitOperator.java#L93
[writer]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-cdc/src/main/java/org/apache/paimon/flink/sink/cdc/CdcRecordStoreMultiWriteOperator.java#L242
[identity]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CommitterOperator.java#L125
[operator]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/CommitterOperator.java#L164
[serializer]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/WrappedManifestCommittableSerializer.java#L53
[state]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/RestoreCommittableStateManager.java#L62
[multi]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/StoreMultiCommitter.java#L151
[recover]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/main/java/org/apache/paimon/flink/sink/RestoreAndFailCommittableStateManager.java#L49
[filter]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L272
[files]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L265
[test]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-flink/paimon-flink-common/src/test/java/org/apache/paimon/flink/sink/StoreMultiCommitterTest.java#L165
[cdc]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/docs/content/cdc-ingestion/overview.md#L143
