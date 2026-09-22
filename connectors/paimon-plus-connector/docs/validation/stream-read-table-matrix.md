# StreamRead 本地实表语义矩阵

基线：Paimon 1.3.2；release-1.3 源码 commit `5c59e6cb01ed0b29563371f56e14fcade4597a2e`。所有表和本地测试数据位于本模块 target 下，PDK 回调由测试提供，未连接实际 Engine 下游。

| 场景 | 已断言的结果 | 测试 |
|---|---|---|
| 空 append 表初始化并随后提交 | start/next=1；新提交交付后 next=2；恢复仅读下一提交 | PaimonStreamEntryIntegrationTest |
| 主键 producer=none | +a、+b、-b；属于状态变更，无完整 before image 承诺 | PaimonStreamReadIntegrationTest.primaryKeyProducerMatrixUsesNativeChangelogs |
| 主键 producer=input | 输入 +a、-a、+b、-b 按原生 changelog 交付 | 同上 |
| 主键 producer=lookup | 由独立测试 writer 提供 IOManager，生成 +a、-a、+b、-b | 同上 |
| 主键 producer=full-compaction | 独立测试 writer 显式全压实，生成 +a、-a、+b、-b；未测试自动压实延迟 SLA | 同上 |
| append overwrite | 默认跳过替换提交；显式 streaming-read-append-overwrite=true 读取替换插入 | appendOverwriteDefaultsToSkipAndOptInEmitsReplacementInserts |
| 主键 overwrite | 默认跳过；显式 streaming-read-overwrite=true 产生删除旧值/插入新值 | primaryKeyOverwriteDefaultsToSkipAndOptInEmitsDeleteInsert |
| 半 plan 回调失败 | 事件已部分提交但 offset 仍为 1，恢复后完整回放 a/b | partialPlanCallbackFailureReplaysFromUnchangedSavedOffset |
| snapshot/changelog 解耦 | snapshot 1 已过期而 changelog 1 存在时可从 1 读取；删除所需 changelog 后明确 out-of-log | expiredSnapshotsRemainReadableThroughDecoupledChangelogAndMissingHistoryFails |
| none 与持久化 consumer-id | 显式 restore 覆盖已有 Consumer 进度；不覆盖该文件 | noneModeIgnoresPersistedConsumerProgressAndDoesNotOverwriteItsFile |
| fixed-start 与独立 writer 清理 | start=1 阻止历史清理；停止保留 Consumer；将测试文件 mtime 调旧后，独立 writer 执行 TTL 清理，旧 start 失败 | PaimonStreamEntryIntegrationTest.independentWriterCannotExpireFixedStartUntilStoppedLeaseExpires |
| schema | 注释改变允许；新增字段、nullable、嵌套 nullable 和解耦语义改变拒绝；历史改名按原生 field-id 投影 | PaimonReadSchemaGuardIntegrationTest |

`+`/`-` 分别表示 TapInsertRecordEvent/TapDeleteRecordEvent。保留原始 RowKind 映射，不合成 TapUpdate，referenceTime 保持连接器读取时刻。Consumer 读回和本地 writer 测试不构成与外部过期线程的事务保护证明；上游没有 CAS 的竞态仍存在。

定向证据：`review-matrix.log`（20 项）、`decoupled-matrix.log`（5 项）。最后补充的持久化 consumer-id 用例及全部回归见 `full-regression.log`。首次 lookup 写侧测试缺 IOManager 的失败已定位并修复测试夹具，没有改变源读取规则。
