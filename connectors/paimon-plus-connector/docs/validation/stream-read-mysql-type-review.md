# StreamRead：MySQL 类型契约与性能审查

日期：2026-09-22。审查起点 `29995888`；分支 `codex/paimon-stream-read`。功能修复本地提交 `466117bd`。所有改动限定在本连接器，未修改 Engine、PDK 或 MySQL 连接器。

## 结论与验收边界

已修复可以在连接器内确认的类型问题，并通过实际 PDK、MySQL codec 和 MySQL JDBC recorder 的本地合同测试。**这不是实际 MySQL 数据库或完整 Engine 的验收；暂不能批准“所有类型无损、生产容量达标”。** MySQL 版本、目标 DDL/字符集、session time_zone、吞吐/延迟/内存指标均未提供。

尚未关闭的门槛：

1. **P1：TIMESTAMP(7–9) 无损写入策略待定。** MySQL DATETIME/TIMESTAMP 至多 6 位小数；真实 MySQL codec 测试确认 `123456789` 输出 `123456`。当前连接器保留 Paimon 原始纳秒并准确声明精度，不能替目标承诺无损。拒绝、允许降精度、转字符串三种策略已向用户提问，本轮不擅自选择。
2. **P1：DECIMAL scale 31–38 与目标范围。** Paimon scale 可到 38，MySQL DECIMAL scale 最大 30；DATE/DATETIME 年份范围也比 Paimon 窄。这些字段不能因为 Java/JDBC 参数保真就视为目标无损，需按同样的拒绝/有损/字符串策略配置目标。
3. **P1：LTZ 与真实目标列/session 时区尚未验证。** 当前源保留 instant 并携带 UTC；MySQL codec 输出 UTC 文本。目标 DATETIME 可采用约定的 UTC 表示；目标 TIMESTAMP 还受服务器会话时区解释影响。必须用真实目标 DDL、session 时区做往返测试。
4. **P2：性能容量与部分 CPU 开销未关闭。** 下述本地数据不代表网络 MySQL 写入。通用解码器在所测 JDK 上的窄行 CPU 微基准有回退；复杂值还承担 JSON 编码/解码成本。本轮是正确性修复，不能称为性能优化完成。
5. **P2：跨 Engine 传输/溢写的复杂类型契约未实测。** 直接 FilterManager→JDBC 路径已验证；不能据此推断所有中间节点、序列化器都保留数组空元素。独立 PDK V2 通用序列化探针发现其 TYPE_LIST 反序列化跳过 null；未证明它是此任务实际事件传输路径，因此不把探针混入 JDBC 合同测试，也不将其直接归因于连接器。须在既有 S15 验证中覆盖 `[1,null,3]`。

## 已确认并修复的问题

| 优先级 | 原行为与影响 | 修复及证据 |
|---|---|---|
| P1 | ARRAY/MAP/ROW 落入 `getString`，原生对象抛 ClassCastException | schema 绑定的 `PaimonSourceRowDecoder` 使用 Paimon 原生访问器；GenericRow/BinaryRow、嵌套、null 测试。`review-complex-red.log` 先复现 |
| P1 | 只声明裸 ARRAY/MAP/ROW，实际 `ARRAY<ROW<...>>` 模型无法匹配 | 修正 spec 参数表达式，用依赖中的真实 `DefaultExpressionMatchingMap` 测试 |
| P1 | 直接返回嵌套 Map 时，PDK 递归包装子 Map；MySQL 将子对象二次编码为字符串 | 事件中的复杂列以标准 JSON String 穿过源 PDK 遍历，再由源 String codec 仅恢复外层 TapMap/TapArray；真实全量/增量 FilterManager + JDBC recorder 均断言嵌套结构和 DECIMAL(38,18) |
| P1 | LocalDateTime 的 UTC 编码被 MySQL 再套目标时区，墙钟偏移 ±8 小时 | 通过既有源 codec 扩展点给 DateTime 编码载体显式 UTC；保留字段无时区语义和 LOCAL_DATETIME origin。UTC/+08/−08 三组合同通过，LTZ 不套用此覆盖 |
| P1 | TIMESTAMP 声明范围仅 1970–2038、精度上限仅 6，与源不符 | 声明为原生 0000–9999、精度 0–9；用 `uuuu` 让 PDK 日期解析接受公历 0000 年 |
| P2 | 二进制可能引用 GenericRow 的可复用 byte[] | 顶层 binary 复制；修改原缓冲后已输出值不变 |
| P2 | 每行重新取得 rowType/字段列表，LTZ 逐行解析 SQL 精度 | 每个读入口/schema 初始化一次 decoder，LTZ 原生访问器读取精度；未改取消门闩、位点语义或调度 |

BIGINT/DECIMAL 的初始疑点已撤回：PDK `ToTapNumberCodec` 本身转 Double，但正常有模型的默认/schema-enforced FilterManager 不进入该 codec。测试确认 Long 极值、`9007199254740993`、DECIMAL(38,18) 与 scale 保持精确；不增加不必要的 Number 覆盖。

## 明确的数据表示

- 标量保留 Boolean、Byte/Short/Integer/Long、Float/Double、BigDecimal、String、byte[]。DATE 为 epoch-day 对应的日期；TIME 按 InternalRow 的 int 毫秒值解码，当前连接器映射声明精度 0–3；Timestamp/LTZ 保留纳秒。固定 Paimon 1.3 源码的 TimeType 声明允许精度 0–9，不能将连接器毫秒路径视为上游类型上限；TIME(4–9) 的端到端精度尚未验证。
- ARRAY 为有序数组，ROW 为字段名对象，MAP 仅接受非空字符串键。非字符串键、空键、MULTISET、VARIANT、JSON 内的 NaN/Infinity 明确失败，避免静默改键或错误解码。
- JSON 内时间用 ISO 文本，嵌套 binary 用标准 Base64。JSON 数字保留十进制精确值；不承诺 MySQL JSON 内部存储的精度和客户端展示方式与 Java BigDecimal 完全一致。
- 批量和流式事件采用复杂列 JSON 载体；只读查询仍返回独立 Java Map/List。复杂列需要正确发现的 TapMap/TapArray 模型；陈旧/手工改写模型不属于本次已验证合同。
- String To-codec 是源转换扩展；目标写入使用 From-codec，二者不得混淆。普通 Unicode 文本和既有 Paimon From-codec 回归均通过。
- JSON 往返是现有 PDK 递归机制下的兼容成本。未采用连接器私有 carrier：PDK 会将其放入 TapValue.originValue，需要额外证明跨插件加载器序列化。不得仅为减少一次转换就引入未验证的私有传输类型。

## 本地验证

环境：Zulu JDK 17.0.20 **x86_64**（本机 ARM 上运行），Paimon artifact 1.3.2，tapdata-api/pdk-api 2.0.8-SNAPSHOT，runner/error-code 2.8-SNAPSHOT；MySQL connector/core 使用本地 1.0-SNAPSHOT artifact。没有连接数据库。

- 完整回归：**806 tests，0 failure/error/skip**，`review-full-regression.log`，2026-09-22 16:15:17 +08，2m46s。
- 后续增加标量元数据边界断言并复跑 MySQL 绑定/模型合同，**5 项通过**，见 `review-final-contract.log`。此项与上面的完整回归分开计数。
- JSON 非有限数拒绝补充后，相关 decoder/MySQL 绑定 **8 项定向测试通过**，`review-final-json.log`；源端遇到嵌套 NaN/Infinity 明确失败，避免 JSON 序列化悄悄改为 null。
- 完整回归日志 SHA-256：`f604f77c8262e7cbd830c467685218b6bd372cf29bd70286c11e971ec85382d9`。
- MySQL profile 共 8 个合同测试，覆盖源→默认/全量 FilterManager→真实 MySQL codec→真实 MysqlWriteRecorder→mock PreparedStatement.setObject；包括 Long、Decimal、二进制、null、Unicode、复杂结构、时间和时区。
- 默认模块测试不依赖 MySQL artifact；`mysql-temporal-contract-test` profile 的两项 MySQL 依赖均 test scope、排除所有传递依赖，不进入生产包。
- 通用 PDK V2 探针另存 `review-wire-binding.log`，失败原因是其数组 null 处理；不能把它当作完整 Engine 传输结论。源码：runner `ObjectSerializableImplV2#toObjectPrivate(TYPE_LIST)`。

在仓库根目录运行：

```sh
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
mvn -o -B -pl connectors/paimon-plus-connector \
  -Pmysql-temporal-contract-test -Dmaven.gitcommitid.skip=true \
  -Dtapdata.pdk.runner.version=2.8-SNAPSHOT -Dpdk-error-code.version=2.8-SNAPSHOT \
  -Djacoco.append=false test
```

本机已有较新远端 SNAPSHOT metadata，但对应 error-code jar 未缓存，离线解析会失败。验证时在本模块 `target/stream-read-validation/offline-repository` 建立本地仓库覆盖：其他目录引用已有缓存，仅复制两个 error-code artifact 目录并忽略远端 snapshot metadata，保留已安装版本；加 `-Dmaven.repo.local=<该绝对路径>`。没有修改全局 Maven 缓存元数据，也没有升级生产依赖。

## 性能证据与限制

原始采样已入库：[解码 CSV](stream-read-mysql-review-data/decode.csv)、[读取 CSV](stream-read-mysql-review-data/stream.csv)。

独立 benchmark main 放在测试目录，不在单元测试中断言耗时。固定 `-Xms512m -Xmx512m`，不挂 JaCoCo。主程序分别是 `PaimonSourceDecodeBenchmark`、`PaimonStreamReadBenchmark`，包为 `io.tapdata.connector.paimon.service`。使用 Surefire 报告的 `java.class.path` 启动；流读取 benchmark 工作目录为本模块，生成的本地表均在 target 下。

解码：每轮 200,000 行，每种宽度预热后交替执行旧/新实现各 6 次；两个独立 JDK 17 JVM。旧方法取自 `29995888` 的相关标量分支，两边都缓存字段列表，保守地不计旧入口重复构造 rowType 的成本。字段交替 BIGINT、DECIMAL、TIMESTAMP、VARCHAR。

| 列数 | JVM 1：旧/新中位数 ms | JVM 2：旧/新中位数 ms | 判断 |
|---|---:|---:|---|
| 8 | 33.26 / 44.75 | 33.63 / 44.39 | 新增约 32–35% 解码 CPU 开销 |
| 32 | 159.37 / 167.76 | 150.95 / 163.07 | 新增约 5–8% |
| 128 | 708.50 / 731.88 | 715.39 / 681.16 | −4.8% 到 +3.3%，无稳定收益 |

这是隔离解码成本；不包含 PDK/MySQL codec、复杂 JSON 或 I/O。早期 JDK 21 ARM 的结果接近持平，不能与 JDK 17 x86 的结果混为一组。保留最终两组原始日志 `review-decode-final-{1,2}.log`，不择优报告。

真实本地读取：一张 append 表、2 列、20,000 行、实际 Paimon 文件→正式 Service→事件回调，每场景预热 1 次、测 5 次；计数断言恰好 20,000。回调 2ms 是模拟下游阻塞，**不是 MySQL 写入**。P95 为五次完整扫描耗时的最近秩估计，样本量有限。

| 批大小 | 每次回调模拟延迟 | 扫描中位数 ms | 扫描 P95 ms | 中位吞吐 行/秒 |
|---:|---:|---:|---:|---:|
| 100 | 0 | 100.28 | 187.48 | 199,433 |
| 100 | 2ms | 567.92 | 613.10 | 35,216 |
| 1,000 | 0 | 49.80 | 62.33 | 401,627 |
| 1,000 | 2ms | 74.10 | 82.36 | 269,910 |
| 10,000 | 0 | 25.77 | 28.24 | 776,089 |
| 10,000 | 2ms | 36.40 | 57.47 | 549,432 |

未由此提高默认批次。更大批次增加内存和尾延迟；队列上限是事件条数 `10 × batchSize`，不是字节预算，宽行/大字段仍需单独压测。当前每次回调复制全表 offset、空闲轮询的元数据请求随表数增长；保留正确性检查，未盲目缓存历史边界或移除取消门闩。

后续性能验收应指定 MySQL 版本/DDL/索引、行宽与复杂列比例、表数、网络、写入批次与提交策略；测试实际 insert/update/delete、持续追赶及慢下游，记录吞吐、P95 延迟、堆/GC、积压和恢复行为。当前数据只能回答本地读取及回调成本。

## 源码与官方边界

- Paimon release-1.3 固定 `5c59e6cb01ed0b29563371f56e14fcade4597a2e`：`paimon-common/src/main/java/org/apache/paimon/data/InternalRow.java#createFieldGetter`、`InternalArray.java#createElementGetter`；`paimon-api/src/main/java/org/apache/paimon/types/{DateType,TimeType,TimestampType,LocalZonedTimestampType}.java`。
- tapdata-api 2.0.8-SNAPSHOT source jar：`TapCodecsFilterManager`、`TapCodecsFilterManagerSchemaEnforced`、`AllLayerMapIterator`、`DateTime(LocalDateTime)`、`TapDateTimeMapping`。依赖源码优先于较新的 common-lib 工作区。
- Engine `ba3533f47fc3c43e9f0dc408f5e871986f1e8626`：`HazelcastSourcePdkBaseNode#transformToTapValue` 区分全量与 CDC；目标使用目标 registry 的 From 转换。本轮未更改此架构。
- connectors 基线 `7c40958f`：`MysqlConnector#registerCapabilities`、`ConnectorBase#formatTapDateTime`、`MysqlWriteRecorder#upsert`。
- MySQL 官方：[日期时间类型与精度](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-type-syntax.html)、[DATETIME/TIMESTAMP 范围与时区](https://dev.mysql.com/doc/refman/8.0/en/datetime.html)、[DECIMAL 的 scale 上限](https://dev.mysql.com/doc/refman/8.0/en/precision-math-decimal-characteristics.html)。

测试使用的 MySQL artifacts SHA-256：

- `mysql-connector`：`bdb941b02304a8f71984e98e247c7e41449264e0677aac1a8f9a4e59e2929601`。
- `mysql-core`：`8091a14a52d382eca9c4d5fbcc843afbb4b364312975c68b61e3b0b33c9c2528`。
