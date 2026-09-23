# StreamRead 精简与行为保持验证

2026-09-22；起点 `b3e40b50`；本地分支 `codex/paimon-stream-read`。本轮仅修改本连接器，保留既有测试断言，新增一项异常清理合同测试。

## 已实施的精简

1. `978f06b7`：`PaimonConsumerRegistration` 直接实现已有的 lease `Target` 接口，删除每表匿名转发对象及中间列表。TTL 检查、固定起点写入、读回验证、watchdog 和独立 I/O 执行器保持原样。
2. `5e596943`：将 `readPlan` 内嵌的 split/batch 消费提取为 `readSplit` 与 `deliverBatch`。plan 校验与完成标记留在同一方法，reader 与 batch 的资源所有权分别集中表达。读取循环最大控制嵌套从五层降为三层；未增加逐行对象或存储调用。

新增 `deliveryFailurePreservesCleanupOrderAndBothSuppressedFailures`：交付、batch 释放、reader 关闭同时失败时，交付异常仍为主异常，两个清理异常依次进入 suppressed，且不执行完成位点校验。该测试先在提取前通过，再在提取后运行。已有测试未为适应重构而改写。

## 必须保留的边界

- `RecordReader.forEachRemaining()` 不能直接替代当前循环。固定 Paimon 源码在行循环成功后才调用 `releaseBatch()`；当前合同要求交付/转换失败也释放 batch，并保留异常及清理顺序。继续使用两层 try-with-resources。
- session 保留 coordinator 字段，并新增说明：当工作线程未确认终止时，Service 保留 session 及其资源登记；不能仅因字段的显式访问都在 `run()` 内就删除这条所有权引用。
- 仍保留每次读取与入队的取消检查、完整 plan 才提交候选 offset、Heartbeat、固定起点 Consumer、历史缺口和 schema 校验。没有把回调返回变为持久化确认，也没有改变 JSON 载体或 MySQL 类型策略。

## 源码依据

- 依赖由本模块 pom 核实：Paimon **1.3.2**、PDK API **2.0.8-SNAPSHOT**。
- [Paimon 1.3 Java Stream Read](https://paimon.apache.org/docs/1.3/program-api/java-api/#stream-read)：继续使用 ReadBuilder、StreamTableScan、TableRead。
- 固定 release-1.3 commit `5c59e6cb01ed0b29563371f56e14fcade4597a2e` 的 [RecordReader#forEachRemaining](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-common/src/main/java/org/apache/paimon/reader/RecordReader.java#L129)：核实释放与关闭顺序，并在实现旁标注来源。
- 同一 commit 的 [DataTableStreamScan#nextPlan](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableStreamScan.java#L199)：plan 内部提前推进 checkpoint，外层完成标记仍须等待全部 reader 关闭。
- 同一 commit 的 [ConsumerManager](https://github.com/apache/paimon/blob/5c59e6cb01ed0b29563371f56e14fcade4597a2e/paimon-core/src/main/java/org/apache/paimon/consumer/ConsumerManager.java#L66)：存储操作及其来源注释保持原样，删除的包装对象没有额外语义。

## 测试

环境沿用已验证的本地离线仓库覆盖，Zulu **17.0.20 x86_64**、runner/error-code **2.8-SNAPSHOT**，启用 `mysql-temporal-contract-test`。命令与依赖覆盖说明见 [类型审查报告](stream-read-mysql-type-review.md#本地验证)。没有执行 package、修改全局 Maven 元数据或升级生产依赖。

| 阶段 | 结果 | target/stream-read-validation 下日志 |
|---|---|---|
| 删除 Consumer 转发层 | 6 项通过 | `simplify-consumer.log` |
| 提取前：新增异常合同及实际入口 | 12 项通过 | `simplify-plan-before.log` |
| 提取后：流读、位点、Consumer | 58 项通过 | `simplify-plan-after.log` |
| 最终完整模块，含 MySQL 合同 | **809 项通过，0 failure/error/skip** | `simplify-full-regression.log` |

完整回归耗时 **2m32s**，完成于 **2026-09-22 23:10:39 +08:00**。日志 SHA-256：`8c5fd3065e3a7266e41c47dd1aada8438d2117450f03631d80a21abfae492307`。与上一轮日志相比未新增 Maven WARNING；仓库已有的插件版本和日志绑定提示仍存在。`git diff --check` 通过；模块和父 pom 未配置独立格式检查任务。

## 本地性能抽查

复用测试目录的 `io.tapdata.connector.paimon.service.PaimonStreamReadBenchmark`。同一 JDK、`-Xms512m -Xmx512m`、不挂 JaCoCo，实际本地 append 表、2 列、20,000 行。每场景预热一次后测五次，每次断言交付恰好 20,000 行。Consumer 为 none；下游仅为回调。

四个独立 JVM 顺序为 **前→后→后→前**；前版本为 `978f06b7` 的已编译类，后版本为最终重构类。两者使用相同测试类及依赖 classpath，仅替换模块 classes 路径；没有与 Maven 测试并行运行。每行是该 JVM 的五次扫描耗时中位数，单位 ms：

| 批大小 | 回调延迟 | 前 JVM 1 | 后 JVM 2 | 后 JVM 3 | 前 JVM 4 |
|---:|---:|---:|---:|---:|---:|
| 100 | 0 | 82.74 | 88.67 | 96.41 | 129.83 |
| 100 | 2ms | 552.81 | 546.77 | 562.92 | 548.45 |
| 1,000 | 0 | 48.47 | 38.80 | 51.14 | 36.28 |
| 1,000 | 2ms | 75.88 | 76.96 | 80.28 | 76.07 |
| 10,000 | 0 | 38.13 | 27.03 | 31.31 | 27.46 |
| 10,000 | 2ms | 37.17 | 39.65 | 36.01 | 34.29 |

[120 条原始测量](stream-read-simplification-data.csv) 包含扫描耗时、回调次数及回调 P95。原始 JVM 日志位于模块 target 的 `simplify-stream-{1..4}-{before|after}.log`。

结果存在进程间波动：例如批大小 1,000、2ms 回调时后版本略慢，零延迟场景则没有一致方向。这些样本不足以判定小幅性能差异；本轮不宣称性能提升或生产容量达标，也没有解决此前窄行解码的 CPU 开销问题。

真实 Engine→MySQL 的持久化、崩溃恢复、时区与精度策略仍按既有 [恢复验收矩阵](stream-read-recovery-results.md) 和 [类型审查报告](stream-read-mysql-type-review.md) 单独验证。结构精简不改变这些验收边界。
