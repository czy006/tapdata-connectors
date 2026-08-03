# Paimon Plus Connector Writer Spill 原生参数统一治理 Spec

## 1. 文档状态

- 状态：Spec 与 Implementation Plan 问题修订已完成；尚未进入代码实施
- 适用模块：`connectors/paimon-plus-connector`
- Paimon 版本：`1.3.2`
- Paimon 源码基线：官方 [`release-1.3.2` tag](https://github.com/apache/paimon/releases/tag/release-1.3.2) 对应 [commit `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7`](https://github.com/apache/paimon/commit/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7)
- 当前阶段：已基于 Paimon 1.3.2 发布源码和 plus Connector 现有实现完成复核，并订正失败语义、运行期并发、旧任务兼容、目录治理与 JDK 17 验证契约

本文定义 Connector 对 Paimon Writer Spill 原生选项的唯一治理规则，并保证新表、已有表、INITIAL 与 CDC 采用同一组持久化配置。

本文中的“当前行为”只描述审查时 `connectors/paimon-plus-connector` 的现状；“必须”“不得”和验收条款描述待实施的目标行为。本阶段只修订 Spec，不代表对应 Java 实现已经完成。

## 2. 已确认需求

### 2.1 用户与问题

主要用户是负责 Tapdata Paimon 任务配置、发布与运行保障的平台工程师和运维工程师。

当前行为存在以下问题：

1. Connector 暴露的 `writeBufferSize`、`diskOverflowWrite`、`diskMaxSize` 与 Paimon 原生选项名称、单位和默认值不一致。
2. Paimon `write-buffer-spillable` 的原生默认值为 `true`；旧参数 `diskOverflowWrite=false` 当前只会导致 Connector 不写表属性，在没有 `tableProperties`、Catalog 默认项或已有表属性另行覆盖时，并不会把 Paimon Spill 关闭。
3. 已有表直接跳过建表属性写入，因此旧任务参数对已有表不生效。
4. 每表 `tableProperties` 在 Connector 生成的属性之后写入，可覆盖 Writer 相关选项，导致同一任务中的表配置不统一。
5. `write-buffer-spill.max-disk-size` 在 Paimon 内核中按单个 External Buffer 的 Spill 文件统计，并且只在特定 `flushMemory` 路径检查，不能作为任务级磁盘配额。
6. Paimon TableWrite/Writer 的选项在 Writer 创建时确定，并持续作用于该 Writer 的整个生命周期，不能只按全量阶段治理。

### 2.2 目标结果

Connector 必须满足以下结果：

- 新任务只配置 Paimon 原生 Writer Spill 选项。
- 旧任务继续可读；旧别名在首次目标端治理时迁移为等价的原生有效值。
- 原生配置与旧别名同时存在时，原生配置优先。
- 新建表显式写入统一后的四个原生选项。
- 已有表逐 key 治理：缺失 key 通过 Paimon Catalog 补齐；已存在 key 与任务值 typed compare 不一致时 WARN 并拒绝任务，绝不覆盖。
- 每表 `tableProperties` 不得声明这四个受管选项。
- 同一任务的所有目标表采用同一组任务级有效值。
- 选项作用于 Writer 的完整生命周期，包括 INITIAL、CDC 以及两者混合运行的任务。
- `diskTmpDir` 保留为 Connector 任务级运行时参数，但不写入 Paimon 表属性。
- `onStart` 不修改业务表；首次 `createTable`、`writeRecord` 或 `afterInitialSync` 目标端回调触发一次任务级治理门禁。
- 纯源端、连接测试和 Schema 浏览不因未使用的 Writer 参数非法而失败，也不得 CREATE/ALTER 业务表。

## 3. 范围与非目标

### 3.1 本次范围

本次只治理以下 Paimon 原生选项：

| 原生选项 | Paimon 类型 | Paimon 1.3.2 默认值 | 本 Spec 中的作用域 |
| --- | --- | --- | --- |
| `write-buffer-size` | `MemorySize` | `256 mb` | 每个物理表的 TableWrite 内存池；任务配置统一 |
| `write-buffer-spillable` | `Boolean` | `true` | 每个物理表 Writer 的 Spill 开关；任务配置统一 |
| `write-buffer-spill.max-disk-size` | `MemorySize` | `Long.MAX_VALUE bytes`，语义为不限制 | 每个相关 External Buffer 的内核检查值；任务配置统一 |
| `write-buffer-for-append` | `Boolean` | `false` | Append-only 写入是否使用写缓冲；任务配置统一 |

运行时目录参数：

| Connector 参数 | 默认值 | 作用域 | 是否写入表属性 |
| --- | --- | --- | --- |
| `diskTmpDir` | `/tmp` | 整个 Tapdata 任务 | 否 |

### 3.2 明确不做

- 不为容量治理扫描全部临时文件、计算任务总占用或实现 Connector 层容量阈值、容量触发 Commit、限流和停止写入状态机；第 7.3 节只按受保护规则清理最终任务级目录下可证明不再被使用的 `paimon-io-*` 遗留目录，不统计或限制目录容量。
- 不在进入 CDC 时自动关闭 Spill，也不在阶段切换时 ALTER 表属性、动态覆盖原生选项或重建 Writer。
- 不把 Paimon 的 `max-disk-size` 包装成任务总磁盘配额或硬上限。
- 不修改 Paimon 1.3.2 内核。
- 不自动把 `write-buffer-for-append` 改为 `true`。
- 不允许每表覆盖四个受管选项，也不允许每表覆盖 `diskTmpDir`。
- 不在任务停止时恢复表属性；成功 ALTER 的结果是持久配置。
- 不把迁移结果写回 Tapdata 任务配置；四个 Writer 选项与 `diskTmpDir` 的迁移和完整归一化都延迟到首次目标端治理门禁，纯源端和连接测试不解析未使用的 Writer 运行时配置。
- 不治理 S3/S3A fast-upload 缓冲目录，不修改或验证 `fs.s3a.buffer.dir`；本 Spec 的 `diskTmpDir` 只描述 Paimon Writer Spill 的本地 IOManager 目录。
- 不自动扫描或删除被任务级配置覆盖、迁移时忽略的旧 per-table 临时目录；这些目录只记录一次运维 WARN，确认相关旧任务全部停止后的清理由运维负责。
- 不承诺跨任务、跨进程的表属性互斥或原子事务。
- 不增加治理 owner 标记、配置指纹表属性、租约或分布式治理锁；两个任务同时首次治理同一未配置表的低概率竞态作为已知限制接受。

## 4. Paimon 1.3.2 内核事实

本节是本 Spec 的技术约束，不得以 Connector 自定义语义替代。

### 4.1 原生默认值

Paimon `CoreOptions` 定义：

- `write-buffer-size` 默认 `256 mb`。
- `write-buffer-spillable` 默认 `true`。
- `write-buffer-spill.max-disk-size` 默认 `MemorySize.MAX_VALUE`。
- `write-buffer-for-append` 默认 `false`，只影响 append-only 写入路径。

源码：[CoreOptions.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L558-L584)

### 4.2 `write-buffer-size` 的真实作用域

`MemoryFileStoreWrite` 为一个 FileStoreWrite/TableWrite 创建默认内存池，该表 Writer 下的 writer 共享并竞争该池。Connector 当前为每个物理目标表创建独立的 `PaimonTableWriteContext`，所以该值不是整个 Tapdata 多表任务共享的一块内存，而是每个物理表 TableWrite 的配置；本 Spec 只保证各表使用相同配置值。

源码：[MemoryFileStoreWrite.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/MemoryFileStoreWrite.java#L120-L128)、[Paimon 写性能文档](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/docs/content/maintenance/write-performance.md#L133-L143)

### 4.3 `max-disk-size` 不是任务磁盘配额

`BinaryExternalSortBuffer#getDiskUsage` 只汇总当前 Buffer 的 Spill Channel 文件；`maxDiskSize` 在 `flushMemory` 的特定路径判断。正常缓冲写满触发的 `spill()` 不等价于全局磁盘配额检查。`ExternalBuffer` 也采用同类局部语义。

因此 Connector 必须原样传递这一内核选项，不得据此推导任务目录总占用、剩余磁盘、跨表总量或硬性磁盘保护结果。

源码：[BinaryExternalSortBuffer.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/sort/BinaryExternalSortBuffer.java#L166-L212)、[ExternalBuffer.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/ExternalBuffer.java#L94-L129)

### 4.4 不同表模式的路径不同

- 主键表 Writer 将 `write-buffer-spillable` 与 `write-buffer-spill.max-disk-size` 传入 MergeTree Writer。
- Append-only Writer 独立读取 `write-buffer-for-append` 与 `write-buffer-spillable`。
- `write-buffer-spillable=false` 不代表所有表模式都完全不需要 IOManager；例如 `KEY_DYNAMIC` Bucket 模式仍有自己的 IOManager 要求。

Connector 不得因为 `write-buffer-spillable=true` 自动打开 `write-buffer-for-append`；但 Paimon 内核自己的 `forceBufferSpill` 路径仍可强制使用 Buffer。验收只能断言 Connector 配置值和对应内核分支正确，不能笼统断言关闭 Spill 后绝无任何临时 IO。

源码：[KeyValueFileStoreWrite.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/KeyValueFileStoreWrite.java#L190-L220)、[BaseAppendFileStoreWrite.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/BaseAppendFileStoreWrite.java#L115-L135)

### 4.5 Connector 与 Paimon Core 只有一套数据 Spill

本 Spec 不新增 Connector 数据 Spill Buffer。Connector 的职责仅为治理表级原生选项、创建和持有 Paimon IOManager、提供任务级临时目录，以及维护 Writer/Commit 生命周期；实际写缓冲、排序、Spill Channel、归并与正式数据文件生成均由 Paimon Core Writer 完成。

因此同时具备 Connector 配置和 Paimon 原生 Spill 能力时，不会把同一批数据在 Connector 层和 Core 层各溢写一次。可能同时发生的是多个 Paimon 内核资源实例的独立 Spill：

- 一个 Tapdata 多表任务为不同物理表创建不同 Writer Context 和 IOManager，各表可以并发 Spill。
- 一个物理表内部的多个分区、Bucket 或 Buffer 可以分别产生临时文件。
- 如果另一个 Flink/Paimon 作业并发写同一张表，它拥有自己的 Writer 和本地 Spill 文件；这不是同一批记录的双层 Spill，但会增加本地磁盘、IOPS、排序 CPU 和提交并发压力。
- `write-buffer-spill.max-disk-size` 仍只按相关 Buffer 的内核路径生效，不能约束上述实例的聚合磁盘使用量。

Spill 文件是提交前的中间数据，不因 Spill 本身产生业务重复。磁盘满、IO 异常或提交结果不确定可能导致任务失败或 at-least-once 重试重复，但这属于运行时失败与提交语义，不是 Connector/Core 双重溢写。

源码：[TableWrite.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWrite.java#L36-L42)、[MergeTreeWriter.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L60-L64)、[SortBufferWriteBuffer.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/SortBufferWriteBuffer.java#L106-L126)、[`PaimonTableWriteContextFactory`](../main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextFactory.java#L124-L164)

### 4.6 四个选项可以通过 Catalog ALTER

Paimon `SchemaManager` 对已有 Snapshot 的表应用 `SetOption` 时会拒绝 `CoreOptions.IMMUTABLE_OPTIONS` 中的键。该集合由 `@Immutable` 注解生成，本 Spec 的四个受管选项均未标记该注解，因此 Paimon 1.3.2 允许通过 `SchemaChange.setOption` 修改。Connector 仍必须传播 Catalog、文件系统和并发导致的实际 ALTER 失败。

源码：[SchemaManager.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java#L270-L312)、[SchemaManager.java immutable check](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/schema/SchemaManager.java#L1078-L1083)、[CoreOptions.java immutable set](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L3474-L3484)

### 4.7 Paimon Spill 目录与 `diskTmpDir`

Paimon Core 没有名为 `diskTmpDir` 的表选项，也不会在本 Connector 已注入 IOManager 后再选择另一套默认 Spill 根目录。Connector 使用归一化后的任务级 `diskTmpDir` 调用 `IOManager.create(tempDirs)`；Paimon 1.3.2 的 `FileChannelManagerImpl` 随后在每个根目录下创建独立的 `paimon-io-<uuid>` 子目录并把 Spill Channel 写入其中。任务级值缺失时，本 Spec 由 Connector 明确补为 `/tmp`，因此最终默认位置是 `/tmp/paimon-io-<uuid>`，而不是 Connector 目录与 Paimon 默认目录二选一。

当前 Connector 还把同名配置用于其他文件系统路径，但这些路径不属于本治理契约。实现不得因为本 Spec 增加 S3/S3A 编码、适配或测试逻辑，也不得把 Paimon 多目录语法推导为其他组件的目录语法。

源码：[IOManager.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManager.java#L47-L52)、[IOManagerImpl.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java#L40-L65)、[FileChannelManagerImpl.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/FileChannelManagerImpl.java#L69-L82)、[`PaimonTableWriteContextFactory`](../main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextFactory.java#L124-L164)

### 4.8 Catalog 默认项的持久化语义

Paimon `AbstractCatalog#createTable` 在真正创建表之前，以 `putIfAbsent` 方式把 Catalog 的 `table-default.*` 复制到传入的 `Schema.options()`。因此：

- 新表显式传入的四个受管值优先，Catalog 默认项不能覆盖它们。
- Catalog 默认项一旦在建表时被复制并持久化，后续从 `TableSchema.options()` 读取时就是已存在的表元数据；治理器无法、也不得把它伪装成 `MISSING`。
- 只有未写入 `TableSchema.options()`、由 `CoreOptions` 在读取时补出的原生默认值才属于“key 缺失但具有运行时有效默认值”。

源码：[AbstractCatalog.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java#L380-L405)、[AbstractCatalog.java `copyTableDefaultOptions`](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/AbstractCatalog.java#L649-L651)、[CatalogUtils.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogUtils.java#L99-L101)

## 5. 配置契约

### 5.1 新任务配置入口

`spec.json` 新增一个任务级 KeyValueEditor 容器 `paimonWriterProperties`。该容器只负责承载键值对，不是 Paimon 表属性，也不产生新的 Writer 语义。

允许的 `propKey` 只有：

```text
write-buffer-size
write-buffer-spillable
write-buffer-spill.max-disk-size
write-buffer-for-append
```

要求：

- 键必须精确匹配，不接受大小写变体、缩写或自定义前缀。
- 同一个键最多出现一次。
- 键和值去除首尾空白后不能为空。
- 未知键、重复键、空键、空值及非法容器形态必须在访问 Catalog 和创建 Writer 前失败。
- 新 UI 不展示 `writeBufferSize`、`diskOverflowWrite`、`diskMaxSize`，新建任务也不得生成这三个键；旧任务编辑回存兼容按第 6.3 节执行。
- `paimonWriterProperties` 不得标记为 `x-perTable`。

原始 JSON/PDK 数据形态必须采用稳定、可测试的分类，不得依赖强制类型转换偶然抛出的异常：

- 容器缺失或值为 `null` 表示未配置，进入旧别名/Paimon 默认值优先级；容器存在且非数组或 `List` 时属于 `CONFIG_CONTAINER_INVALID`。
- 容器元素必须是非 `null` 的 Map；`null` 或非 Map 元素属于 `CONFIG_ENTRY_INVALID`。
- `propKey`、`propValue` 必须真实存在且为 String；缺失、`null` 或非 String 分别按空项或 `CONFIG_ENTRY_INVALID` 处理，不得通过 `String.valueOf` 把任意对象静默转成配置。
- key/value trim 后再判断空值和重复 key；除 `propKey`、`propValue` 外的编辑器展示字段可以忽略，但不得进入归一化结果。
- 三个旧别名若原始 key 存在但值为 `null`，属于 `LEGACY_VALUE_INVALID`，不能伪装为“未配置”；`diskOverflowWrite` 只接受 Boolean，两个旧数值只接受 Number，并把其原始十进制表示拼接声明单位后交给 Paimon `MemorySize` 解析。小数、非有限数、字符串数字及 Paimon 拒绝的值均失败，Connector 不另写数值/单位解析器。

新任务 UI 建议预置：

```text
write-buffer-size = 256 mb
write-buffer-spillable = true
write-buffer-for-append = false
```

`write-buffer-spill.max-disk-size` 默认不预置；缺失时使用 Paimon 的无限制有效默认值。即使用户删除任一预置项，首次目标端治理归一化仍必须使用对应 Paimon 默认值，不能回退到 Connector 的旧 Java 字段默认值。

### 5.2 类型解析与规范值

- 任务输入中的两个布尔选项接受 Connector 对 KeyValueEditor 输入去除首尾空白后、不区分大小写的 `true` 或 `false`，持久化时规范为小写。去除空白后必须通过 Paimon 公共 `Options#get(ConfigOption)` 或 `CoreOptions` 完成类型解析，不得另写 Boolean parser。已有表属性必须保留原始字符串并通过同一 Paimon 公共入口校验；其内部 `OptionsUtils.convertToBoolean` 不去除空白，因此已有值 `" true "` 属于非法元数据，必须拒绝而不能按等价值放行。
- 两个内存选项使用 Paimon `Options#get(ConfigOption)` / `CoreOptions` 或公共 `MemorySize.parse` 解析，比较时使用 `MemorySize#getBytes()`，持久化时使用 `MemorySize#toString()` 生成可被同一解析器重新读取的规范字符串；不得自行编写单位正则、倍率换算或格式化器。
- 未配置 `write-buffer-spill.max-disk-size` 时，有效值必须是 `MemorySize.MAX_VALUE`；持久化到新表时使用 `9223372036854775807 bytes`。
- Connector 不额外要求 `max-disk-size` 必须是有限正数，也不替换 Paimon 对非法或负数的校验。
- 配置比较必须按解析后的类型值进行，不能因为 `256 mb` 与 `268435456 bytes` 字符串不同就触发 ALTER。
- `max-disk-size=0` 保留 Paimon 原生可解析语义，不由 Connector 擅自禁止；当它会进入可 Spill 的写路径时，UI 帮助文本和首次目标端治理日志必须 WARN：该值不等于“禁止磁盘写入”，并可能使内存抢占触发的 `flushMemory` 无法继续 Spill。

`OptionsUtils.convertToBoolean` 不是 Connector 可直接调用的公共方法；`Options#get(ConfigOption)` 会在 Paimon 包内委托该方法。Connector 必须复用公开门面，不得通过反射、把实现复制到 Connector，或在 `org.apache.paimon.*` 包下增加桥接类规避可见性。

解析依据：[Options.java `get/getOptional`](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/Options.java#L101-L121)、[OptionsUtils.java `convertToBoolean`](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/OptionsUtils.java#L197-L213)

语法解析成功不等于目标表 Writer 一定可运行。在任何 Catalog 变更前，目标端治理必须根据每张表最终有效 options 和实际写路径执行上下文预检：

- 使用 Paimon `CoreOptions` 取得 `write-buffer-size`、`page-size` 等有效值，不自行复制内核默认值。
- 对会创建 `SortBufferWriteBuffer` 的路径，每张表最终有效的 `write-buffer-size / page-size` 必须至少提供 3 个完整 page；不足时必须在 Catalog 零变更预检阶段失败。`KEY_DYNAMIC` 的 `GlobalIndexAssigner` 会把最终有效 `write-buffer-size / 2` 交给 RocksDB bootstrap `BinaryExternalSortBuffer`，因此该模式要求该表最终有效的 `write-buffer-size` 至少提供 6 个完整 `page-size`。现有 `HASH_DYNAMIC` 历史污染预检在 marker 缺失或表 UUID 变化时也会打开 `GlobalIndexAssigner`，所以该条件下同样必须在任何 CREATE/ALTER 前按 6 page 边界预检；非法 marker 类型直接在 mutation 前失败，有效 marker 命中时不得凭空增加该路径没有执行的 6 page 限制。`page-size` 不是本 Spec 的受管任务参数，必须从该表合并 Catalog 默认项后的最终 options 读取，不能仅检查任务级 Writer 四键。
- 对会构造 `AbstractMemorySegmentPool` 的路径，`page-size` 字节数必须位于 `1..Integer.MAX_VALUE`，且 `write-buffer-size / page-size` 的完整 page 数必须位于 `1..Integer.MAX_VALUE`；否则 Paimon 当前的 `long -> int` 转换或除法不能可靠表达请求值，必须提前拒绝。

源码：[GlobalIndexAssigner.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/GlobalIndexAssigner.java#L169-L180)、[RocksDBState.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/lookup/rocksdb/RocksDBState.java#L121-L132)、[BinaryInMemorySortBuffer.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/sort/BinaryInMemorySortBuffer.java#L48-L65)
- 只拒绝上述源码可证明的边界和其他由确定性构造条件证明 Writer 必然无法运行的组合，不把经验性容量建议升级成配置合法性规则。
- 该预检只提前暴露 Paimon 1.3.2 已有约束，不新增用户参数，也不得推导未被源码证明的通用限制。
- Plus Connector 已有 `PaimonWriteSemanticContractResolver`、`PaimonDynamicBucketPreflight` 及对应测试。实现必须扩展并前移这些既有入口：新表候选 Schema 调用 `validateNewTable`，已有 `FileStoreTable` 调用 `resolve`，仅已有 HASH_DYNAMIC 表按 marker/UUID 状态复用 `ensureHashDynamicValidated`；新表没有历史数据，只执行 Schema/6-page 确定性预检，并在 CREATE 回读取得 UUID 后按 Writer 前门禁建立验证状态。不得在新的 Reconciler/Gate 中复制 bucket、merge、RowKind、污染扫描或 3/6 page 判断。已有 HASH_DYNAMIC 扫描可能创建临时 IO 并写入验证 marker，但不得修改业务 Catalog；扫描或 marker 校验失败时后续 CREATE/ALTER 次数必须为零。

Paimon 类型与构造约束依据：[MemorySize.java 常量与构造校验](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/MemorySize.java#L51-L88)、[MemorySize.java toString](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/MemorySize.java#L145-L174)、[MemorySize.java parse](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/MemorySize.java#L244-L345)、[CoreOptions `PAGE_SIZE`](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L602-L606)、[CoreOptions `pageSize()` 的 `int` 转换](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2405-L2407)、[`AbstractMemorySegmentPool` 页数转换](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-common/src/main/java/org/apache/paimon/memory/AbstractMemorySegmentPool.java#L33-L37)、[`SortBufferWriteBuffer` 最少 3 page 检查](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/SortBufferWriteBuffer.java#L106-L109)

### 5.3 单一归一化结果

首次目标端治理必须生成不可变的任务级归一化结果，包含四个选项的完整有效值及每个值的来源：

- `NATIVE`：来自 `paimonWriterProperties`。
- `LEGACY`：来自旧别名迁移。
- `PAIMON_DEFAULT`：任务未配置，采用 Paimon 1.3.2 原生默认值。

`PaimonConnector#onStart` 必须在调用 `PaimonConfig.load`、应用 Java 字段默认值或修改 node DataMap 之前，只复制治理所需的原生容器、旧别名、任务级目录和历史每表目录，形成不可变的只读原始快照。快照必须递归防御性复制 Map、List/数组及其中的标量；快照完成后修改原始嵌套 Map/List 不得改变后续归一化结果。不得把 `DataMap.create` 的浅复制当成不可变保证。

传给 `PaimonConfig.load` 的也必须是防御性副本，并移除三个旧 Writer 别名和原生 Writer 容器，避免 Bean 类型转换在非写入上下文提前解析四个受管选项；不得修改框架传入的 connection/node/table DataMap。`diskTmpDir` 原始值可以继续随防御性副本进入既有、非本 Spec 治理的运行时路径，但 Paimon IOManager 不得把该 Bean 原始值或 per-table getter 当成事实源。首次目标端回调必须从不可变快照独立完成四个 Writer 选项和第 7 节目录的完整归一化，并把结果显式提供给治理和 Writer 创建路径。

后续新建表、已有表治理、Writer 创建与日志都必须读取同一个归一化结果，不得在不同路径重复解析并形成不同默认值。

## 6. 旧参数只读迁移

### 6.1 映射规则

| 旧任务键 | 旧单位/类型 | 目标原生键 | 迁移结果 |
| --- | --- | --- | --- |
| `writeBufferSize` | MB 数值 | `write-buffer-size` | `<value> mb` |
| `diskOverflowWrite` | Boolean | `write-buffer-spillable` | `true` 或 `false` |
| `diskMaxSize` | GB 数值 | `write-buffer-spill.max-disk-size` | `<value> gb` |

`write-buffer-for-append` 没有旧别名；缺失时固定采用 Paimon 默认值 `false`。

### 6.2 优先级与存在性

每个原生键独立按以下优先级取值：

1. 新原生配置。
2. 原始任务 DataMap 中真实存在的旧别名。
3. Paimon 1.3.2 原生默认值。

必须从原始任务配置判断旧键是否存在。不得把 `PaimonConfig` Java 字段初始化值当成用户配置过的旧参数，否则所有历史任务都会被误判为显式旧配置。

首次目标端治理发现新旧配置同时存在时：

- 原生值生效。
- 旧值不参与合并。
- 首次目标端治理日志按键输出一次旧值被忽略的警告，但不得失败。

即使旧 `diskOverflowWrite=false`，旧 `diskMaxSize` 仍应被独立归一化；是否被当前 Paimon 写路径消费由内核决定，Connector 不删除该值。

当 `write-buffer-spillable` 的来源是旧 `diskOverflowWrite=false` 时，首次目标端治理摘要必须标记“按旧配置意图关闭 Spill”。已有表该 key 缺失时可补写 `false`，这会把 Paimon 默认有效值从 `true` 显式纠正为 `false`，应列入实际行为变化清单；已有表若已显式配置 `true`，则必须按冲突规则拒绝，不能列为“已纠正”。不得笼统声称所有历史表此前都在 Spill。

### 6.3 迁移边界

- 旧别名继续可读，但不再出现在新建或编辑任务的 UI 中，新建任务不得写出这些键。
- 必须在目标 UI/Engine 环境验证“打开旧任务—不修改旧字段—保存—重新运行”后原始旧键和值仍可供迁移读取。未取得该证据前，`spec.json` 必须保留无默认值、`x-display=hidden` 的 deprecated 兼容属性，防止 schema 投影回存时丢弃旧键；只有证明确认未知属性原样保留后才允许完全移除这些兼容属性。
- Connector 不修改 Tapdata 已保存的任务 JSON。
- 已有表只 ALTER 补齐缺失 key；已存在 key 与任务值 typed compare 不一致时拒绝覆盖；新表始终显式写入四个规范值。
- 迁移日志不得输出整份任务配置或其他无关凭据。

## 7. `diskTmpDir` 运行时契约

`diskTmpDir` 不是 Paimon 表选项，必须与四个受管原生选项分开处理。

### 7.1 新配置

- `diskTmpDir` 只允许任务级配置，不再使用 `x-perTable`。
- 默认值为 `/tmp`。
- 与四个 Writer 选项一样，目录只在首次目标端治理门禁中归一化；`onStart` 只保存递归防御性快照。纯源端、连接测试和 Schema 浏览不得因未使用的目录输入非法而失败。
- 首次目标端治理成功后，该目录配置在任务整个生命周期中保持不变，供 Paimon IOManager 和 Connector Spill 清理使用；本契约不覆盖 S3/S3A 缓冲路径。
- 多个物理表可以各自创建 IOManager，但它们都读取同一个任务级目录配置。
- 目录参数只决定临时文件位置，不提供目录总量配额、跨表统计或磁盘保护承诺。
- 全部纯计划通过并完成第 7.3 节 stale cleanup 后、第一次 CREATE/ALTER 前，如果任一已知写路径因 `write-buffer-spillable` 或 Bucket 模式需要 IOManager，Gate 必须直接调用 `IOManager.create(规范目录数组)` 完成一次任务级 create/close smoke preflight，并复用现有 live-dir 登记/注销与关闭语义。创建或关闭失败时 Catalog mutation 次数为零；该 preflight 允许 Paimon 创建临时 `paimon-io-*` 目录，但不写业务 Catalog，也不能被表述为完全无副作用。

### 7.2 历史每表值迁移

如果任务级 `diskTmpDir` 存在，它始终优先，旧每表值仅记录一次忽略警告。以下迁移在首次目标端门禁读取的不可变快照上完成，不修改任务 JSON 或框架传入的 DataMap。

如果任务级值不存在：

- 没有旧每表值：使用 `/tmp`。
- 所有非空旧每表值在规范化后完全相同：将该唯一值提升为本次任务的内存运行时值，并记录一次迁移警告。
- 存在多个不同旧每表值：在创建任何 Writer 前失败，要求用户收敛为一个任务级值；不得静默选择其中一个。

目录列表必须按以下唯一规则解析、规范化和比较：

1. 分隔符严格复用 Paimon `IOManagerImpl#splitPaths` 的 `,` 或平台 `File.pathSeparator` 语法；Connector 不复制其正则或另写拆分器。
2. 调用 `splitPaths` 前只做原始空片段检查；前导、中间、尾随空片段或最终空值失败，因为 Java `String.split` 会丢弃尾随空片段。
3. 对拆分后的每项 trim；trim 后为空则失败。随后通过 JDK `Path` 转为绝对路径并执行词法 `normalize`，但不得调用 `toRealPath`、解析符号链接或要求目录预先存在。
4. 规范结果是不可变的有序路径列表；不去重、不排序。历史值只有在列表长度和每个位置的规范路径完全相同时才算相同。
5. `IOManager.create` 必须消费该规范列表，不再接收未经规范化的原始字符串。目录顺序和重复项可能影响 Paimon 目录选择，不得被 Connector 改写。

源码：[IOManagerImpl.java `splitPaths`](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManagerImpl.java#L153-L156)

### 7.3 资源关闭与异常退出遗留目录治理

`PaimonTableWriteContext` 是单个物理表 Writer 资源的唯一所有者。正常关闭、构造中途失败和 Gate 的 IOManager smoke 都必须复用现有 Context、Factory 与 Spill Cleaner 的所有权语义，不得再引入一层 Writer 包装器、第二套资源注册表或第二个 Cleaner。

正常关闭契约固定为：

1. 先关闭持有底层 Paimon TableWrite 的 Writer Strategy；没有 Strategy 时关闭 raw Writer。
2. 再关闭对应 Committer。
3. 再关闭该 Context 持有的 IOManager。
4. 无论 IOManager 关闭成功或失败，最后都必须在 `finally` 语义下注销 live-dir 并释放 Connector owner lock。

任一关闭步骤失败都不得阻止后续资源继续关闭。按上述顺序观察到的第一个异常作为主异常抛出，后续关闭异常按发生顺序通过 `Throwable#addSuppressed` 附加；不得吞掉、覆盖主异常或只记录日志。`close()` 必须幂等，同一资源和注册不得重复关闭或重复注销。

Writer Context 构造中途失败时，原始构造异常始终作为主异常，已取得资源按唯一所有权逆序尽力释放：已建成 Strategy 时由 Strategy 关闭其 raw Writer，不得再次直接关闭；Strategy 尚未建成时先关闭已建成的 Committer，再关闭 raw Writer；IOManager 最后关闭，live-dir/owner lock 在 `finally` 中注销。所有释放异常附加到原始构造异常，任何分支都不得泄漏 IOManager、live-dir 注册或 owner lock。

异常退出遗留目录治理固定为：

- 首次目标端 Gate 完成任务级目录归一化且全部 Catalog 零变更纯计划通过后，先对最终规范任务级目录执行一次 stale cleanup，再执行 IOManager create/close smoke、计划要求的 HASH_DYNAMIC Preflight 和任何 CREATE/ALTER。纯源端、`connectionTest` 和 Schema 浏览不触发该清理。
- 清理只扫描最终不可变任务级目录列表中的根目录，不扫描被任务级配置覆盖或迁移时忽略的旧 per-table 根目录，也不把目录扩展到 S3/S3A 或其他文件系统缓冲路径。
- 必须复用现有 `PaimonSpillDirCleaner`：当前 JVM live-dir 永不删除；存在 Connector owner-lock 文件且能够取得跨进程 advisory lock，并超过 stale grace period 的 `paimon-io-*` 目录才允许删除；其他进程仍持锁的目录即使很旧也不得删除。
- 滚动升级期间，没有 Connector owner-lock 文件的旧版本目录不得仅凭年龄自动删除，必须保留并输出一次运维 WARN。这里的目录 owner lock 只保护本地临时目录，不是第 3.2、9.6 和 17 节禁止增加的表属性治理 owner 标记、租约或分布式锁。
- stale cleanup 是 best-effort 运维卫生动作；枚举、加锁或删除失败只输出 WARN，不发布 sticky failure。最终目录是否可用于本任务写入仍由紧随其后的 IOManager create/close smoke 判定，smoke 失败继续按第 7.1 节阻断 Catalog mutation 和 Writer 创建。

现有实现与测试锚点：[`PaimonTableWriteContext#close`](../main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContext.java#L261-L280)、[`PaimonTableWriteContextFactory` 构造失败清理](../main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextFactory.java#L176-L198)、[`PaimonSpillDirCleaner`](../main/java/io/tapdata/connector/paimon/service/PaimonSpillDirCleaner.java#L20-L170)、[`PaimonTableWriteContextTest`](../test/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextTest.java#L195-L228)、[`PaimonSpillDirCleanerTest`](../test/java/io/tapdata/connector/paimon/service/PaimonSpillDirCleanerTest.java#L22-L83)。实现必须保留这些已验证语义，同时把当前初始化阶段直接读取 Bean `diskTmpDir` 的 cleanup 调用迁移到上述首次目标端 Gate 时点。

## 8. 每表配置冲突

任一目标表的 `tableProperties` 中只要出现以下键之一，任务必须在 Catalog 变更和 Writer 创建前失败，即使值与任务级值相同：

```text
write-buffer-size
write-buffer-spillable
write-buffer-spill.max-disk-size
write-buffer-for-append
```

禁止的原因是消除多事实源，而不是只防止值冲突。键匹配在去除首尾空白后按精确名称判断。

`diskTmpDir` 也不得通过每表配置覆盖；历史值只按第 7.2 节迁移。

## 9. 目标表发现与 Writer 前置门禁

### 9.1 核心不变量

对每一个物理目标表，必须在它的首个 `PaimonTableWriteContext`、TableWrite 或 Writer 创建前完成以下步骤：

1. 得到任务级归一化结果。
2. 校验每表冲突。
3. 完成目标表写路径的 Paimon 构造约束预检。
4. 确认表不存在并使用统一选项创建，或确认已有表已完成逐 key 治理。
5. 从 Catalog 重新加载表并验证四个 key 已显式存在且有效值一致。
6. 只有验证成功后才允许创建 Writer。

如果相关 Writer 已经存在，再尝试配置治理属于生命周期不变量破坏，任务必须失败，不能热替换 Writer 内的选项。

### 9.2 首次目标端回调与任务级门禁

PDK `TapConnectionContext` / `TapConnectorContext` 不提供可靠的当前任务读写方向标志，而本 Connector 同时注册 Source 和 Target 能力。因此 `onStart` 不得根据 `instanceof TapConnectorContext`、`tableMap != null` 或 Connector 支持写能力推断当前是目标端，也不得在此阶段解析失败或修改业务表。

首次进入以下任一目标端专用回调时，必须原子触发一次任务级治理门禁：

- `createTable`；
- `writeRecord`；
- `afterInitialSync`。

门禁必须满足：

1. 同一任务的并发目标端回调只能有一个执行治理，其他回调等待同一结果；不得重复 ALTER。
2. 遍历 `TapConnectorContext#getTableMap()` 中可知的全部目标表，解析最终物理 Catalog Identifier。
3. 在第一次 Catalog 变更前，完成全部可知目标表的原始配置冲突校验、逐 key 表属性分类、既有 `PaimonWriteSemanticContractResolver`、Paimon 构造约束预检和差异计划；仅标记哪些已有 HASH_DYNAMIC 表需要执行有临时 IO/marker 副作用的动态 Bucket Preflight，不得在纯计划阶段提前执行。
4. 任一表存在已配置但不一致的受管 key、非法元数据或其他预检失败时，整个门禁失败，所有表的 Catalog 变更次数必须为零。
5. 全部纯计划通过后，按第 7.3 节只对最终任务级目录执行一次 best-effort stale cleanup；随后按第 7.1 节执行 IOManager smoke，再执行第 5.2 节计划标记的 HASH_DYNAMIC Preflight。smoke 或 Preflight 失败时所有表的 Catalog 变更次数必须为零；cleanup 失败只 WARN 并继续 smoke。
6. 上述阶段通过后，只对已存在表顺序执行必要的 ALTER、invalidate、reload 和回读验证；对尚不存在的表记录不可变的 `PLANNED_CREATE` 治理计划，不得为了治理而提前创建业务表。
7. 门禁成功结果可供后续目标端回调复用；失败结果形成任务可见的粘滞失败，不能由后续回调绕过。

任务级门禁成功不替代 PDK 的正常建表生命周期。`createTable` 只创建本次回调请求的目标表，并消费其 `PLANNED_CREATE` 计划；`writeRecord` 若发现目标表仍不存在，必须沿用现有生命周期错误处理，不得由治理器擅自补建其他业务表。每次实际 CREATE 后仍必须执行缓存失效、回读和四键验证。

`PLANNED_CREATE` 不得保存可变 `TapTable`、DataMap 或 options Map 引用。它必须绑定：

- 物理 Identifier 和任务级四键规范值；
- 从本次请求生成并递归复制的最终 Paimon `Schema`：有序 `DataField`（包含 id、名称、完整 DataType/nullable、description、defaultValue）、有序 partition keys、primary keys、comment，以及合并 Catalog defaults 后的完整 options；options 只忽略 Map 迭代顺序，不能忽略键值差异；
- 预检时的目录规范列表和 Paimon 1.3.2 写语义结果摘要。

Paimon `Schema` 的公开值对象字段和 `equals` 已覆盖 fields、partition keys、primary keys、options 与 comment，可作为生成摘要的原生事实源；Connector 只负责防御性持有和补充 Identifier/目录/治理来源，不得另定义一套会漏字段的 TapTable 比较协议。源码：[Schema.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/schema/Schema.java#L64-L125)、[`Schema#equals`](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/schema/Schema.java#L231-L249)

实际 `createTable` 回调必须重新生成同一摘要；与计划不等价时，必须在 CREATE 前重新执行该表的 Catalog 零变更预检，不能使用陈旧计划跳过构造约束或每表冲突检查。CREATE 前再次发现表已存在，或 CREATE 返回并发已存在时，必须 reload 并转入第 11 节逐 key 治理；已有显式冲突仍然拒绝，不能把并发建表当成成功而直接创建 Writer。

如果写任务的目标表集合应当存在但无法枚举、迭代器不受运行时实现支持，或表映射结构不完整，任务必须在 Writer 创建前失败，不能只治理首张表后继续运行。

纯源端任务、`connectionTest`、Schema 浏览等非写入上下文也会调用 `onStart`。此类上下文只复制 Writer 原始配置快照并初始化 Catalog：

- 不因为目标表集合为空而失败。
- 不因未使用的 Writer 参数未知、重复、空值或类型非法而失败。
- 不 CREATE 或 ALTER 业务表。
- 不创建数据 Writer。

进入任一目标端回调即构成明确的写入意图；此时仍无法取得应有目标集合或完整解析 Writer 配置时必须 fail-fast。

### 9.3 运行期新发现的目标表

对于首次任务级门禁时不存在或运行期才解析出的物理目标表，必须在该表首个 Writer 创建前复用相同的逐表门禁：

- 表不存在：创建时显式写入四个规范值。
- 表已经由其他进程创建：转入已有表逐 key 治理流程。

这不是按表自定义配置；所有表仍使用同一任务级归一化结果。运行期逐表门禁还必须满足：

- 以物理 Identifier 建立 single-flight 状态，至少区分 `UNSEEN`、`PLANNING`、`MUTATING`、`VERIFIED`、`FAILED`；同表并发回调共享同一个完成结果，不重复 CREATE/ALTER/HASH_DYNAMIC 扫描。
- 任一运行期表治理失败都调用 `PaimonService` 现有 `recordStickyFailure`/`PaimonServiceLifecycle#fail`，使整个写任务进入统一 FAILED 状态；Gate 可以缓存完成结果用于唤醒等待者，但不得成为第二套独立 sticky failure 所有者。
- `VERIFIED` 必须绑定物理 Identifier 和 `FileStoreTable#uuid()`。UUID 缺失时不得跨 reload 缓存 VERIFIED；UUID 变化表示同名表已经重建，必须丢弃旧结果并重新治理。
- `dropTable`、`clearTable` 及其他经过 `runTableDdl` 的操作在现有派生缓存失效点同步移除该 Identifier 的 `VERIFIED`、`PLANNED_CREATE` 和失败前中间状态；DDL 后再次写入必须 reload 并重新治理。
- 锁顺序固定为 `PaimonServiceLifecycle.Ingress` → 任务级 gate 等待/发布 → per-Identifier single-flight → 现有 `commitLocks[tableKey]`。不得持有 `commitLocks` 等待任务级 gate；应用陈旧计划前必须在 `commitLocks` 内再次核对表存在性、UUID 和受管 options。DDL 只在持有现有 `commitLocks` 时变更表并做非阻塞 registry 失效，不能反向等待 gate。

## 10. 新建表契约

新建表的 Schema options 必须先写入四个受管原生选项，再处理其他非受管表属性。

要求：

- 四个键全部显式写入，包括任务未配置时的 Paimon 默认有效值。
- `tableProperties` 已在前置校验中禁止受管键，因此后续合并不能覆盖它们。
- Catalog 的 table default options 不得覆盖受管值；显式 options 应优先于 Catalog 默认项。
- 创建后必须 invalidate/reload，并按类型验证实际有效值。
- 创建后的实际值不一致时，任务失败，不得创建 Writer。
- `Catalog#createTable` 抛出异常时不能直接推断“未创建”，必须按第 11.3 节 reload 并判定实际结果；只有等价 Schema 和受管值通过验证后才可继续。

## 11. 已有表持久化治理

### 11.1 逐 key 分类

对每张已有表：

1. 读取当前 `TableSchema.options()` 中四个受管 key 的原始存在性和值。未持久化、仅由 `CoreOptions` 补出的运行时默认值不得伪装成“key 已显式存在”；Catalog `table-default.*` 若已在建表时复制进 Schema，则按已存在元数据处理，不能归类为 `MISSING`。
2. 对每个 key 独立分类：
   - `MISSING`：Schema options 中不存在，计划使用规范值 `setOption` 补齐。
   - `PRESENT_EQUAL`：已存在且解析后的 Boolean/字节数等于任务目标值，保留原值，不 ALTER。
   - `PRESENT_CONFLICT`：已存在且 typed value 不同，WARN 并拒绝整个任务，不得覆盖。
   - `PRESENT_INVALID`：已存在但无法按 Paimon 类型解析，ERROR 并拒绝整个任务，不得用目标值掩盖元数据损坏。
3. 只有所有已存在 key 都是 `PRESENT_EQUAL`，该表才允许补齐 `MISSING` key。
4. 四个 key 全部显式存在且 typed value 一致后，该表才合规；即使缺失 key 的 Paimon 默认有效值恰好相同，也必须持久化补齐。

该规则把“已存在的显式配置”视为其他任务或管理员已经做出的表级决定。Connector 不尝试识别 owner，也不增加治理标记；只要值不同就拒绝覆盖。

### 11.2 ALTER 与回读

存在 `MISSING` key 时必须：

1. 只为 `MISSING` key 使用 Paimon `SchemaChange.setOption` 生成变更，不重写 `PRESENT_EQUAL` key。
2. 调用 Catalog `alterTable` 持久化。
3. 显式 invalidate 该表缓存。
4. 重新加载表。
5. 回读验证四个 key 全部显式存在，且 typed value 与任务值一致。

回读不一致、表并发删除、ALTER 异常或 Catalog 缓存无法刷新时，任务必须失败，且不得创建该表 Writer。

Paimon `CachingCatalog#alterTable` 当前会在委托 ALTER 成功后自动 invalidate；但 `Catalog#invalidateTable` 的接口默认实现是 no-op。Connector 仍应在协调边界显式调用 invalidate 并以 reload 后的四键回读作为最终判据，不能只依赖某个 Catalog 实现的自动失效行为。

Paimon Catalog 依据：[SchemaChange.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/schema/SchemaChange.java#L82-L90)、[Catalog.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java#L297-L319)、[CachingCatalog.java](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/CachingCatalog.java#L208-L213)

### 11.3 CREATE/ALTER 异常后的实际结果判定

Catalog API 没有承诺“调用抛异常即服务端未提交”。任何 CREATE/ALTER 抛出异常后，都必须在不创建 Writer 的前提下 invalidate/reload，并记录以下稳定分类：

- `NOT_APPLIED`：ALTER 的原始 raw key/value 全部仍在，或 CREATE 后确认表仍不存在。传播原始 mutation 异常，不执行补偿。
- `APPLIED_EQUIVALENT`：ALTER 的目标受管键全部已显式持久化且 typed equal；或 CREATE 后表的完整不可变 Schema 摘要和四键验证均与计划等价。CREATE 计划本身没有预先可知的 UUID；等价回读成功后必须读取实际 UUID 并绑定新的 VERIFIED。记录一次 WARN，把该操作计入已生效列表并继续；后续若任务失败，ALTER 纳入条件补偿，CREATE 按第 17 节保留并报告残留，不自动 DROP。
- `CONCURRENT_EXISTING`：CREATE 后出现非同一计划生成、但可完整读取的同名表。必须重新运行已有表逐 key 分类、写语义和 Schema 兼容预检；只有完整兼容才可作为已有表继续，不能只因四键相同就放行。
- `PARTIAL_OR_DIVERGED`：ALTER 只应用部分 key、出现非目标值，或 CREATE 后 Schema 不兼容。任务进入 sticky failure；对能够证明仍等于本任务写入值的 key 执行条件补偿，但不得覆盖不同值。
- `UNKNOWN`：reload 本身失败、无法确定表存在性/UUID/Schema 或结果在重试窗口持续变化。任务进入 sticky failure，并报告 Catalog、数据库、表、mutation 类型、原始异常和最后一次可观察状态；不得宣称零变更。

原始 mutation 异常作为诊断主因保留；reload、分类和补偿异常作为附加异常。实现不得只维护“方法正常返回的成功列表”，所有 `APPLIED_EQUIVALENT` 和可证明的部分应用都必须进入后续残留状态报告。

### 11.4 多表失败与补偿

Paimon Catalog 不提供跨表 ALTER 事务。Connector 必须采用以下确定性策略：

- 在第一次 ALTER 前完成所有可知目标表的解析、冲突校验和差异计算，减少中途失败。
- 顺序 ALTER，并保存本次任务看到的原始键存在性和原始值。
- 任一表失败后立即停止后续变更，任务启动失败。
- 对本次已成功 ALTER 的表执行尽力而为的条件补偿：只有当回读值仍等于本任务刚写入的值时，才恢复原值或移除原本不存在的键。
- 如果值已被其他进程再次修改，跳过该键补偿并记录明确错误，降低覆盖并发写入的概率。
- 原始失败始终作为主异常；补偿失败作为附加异常和日志信息保留。

回读判断与补偿 ALTER 之间没有 CAS，仍存在 TOCTOU 窗口。补偿只能降低单任务部分变更风险，不能保证绝不覆盖并发修改，也不能形成跨任务原子性保证；测试和日志不得作出更强承诺。

失败保证必须按 mutation 边界表述：第一次 CREATE/ALTER 之前的配置、Schema、写语义、目录、全表冲突和构造预检失败，强保证 Catalog 零变更、Writer 零创建；mutation 发出后的任何失败只保证 Writer 零创建，并执行本节条件补偿。补偿后仍需 reload 形成逐表 `RESTORED`、`RESIDUAL` 或 `UNKNOWN` 报告，不能把“发起过补偿”表述为“已经零变更”。

### 11.5 并发限制

顺序启动时，后启动任务看到任一已存在但不同的受管 key 必须 WARN 并拒绝，不能执行 ALTER 覆盖。日志至少包含 Catalog、数据库、物理表、冲突 key、当前规范值与请求规范值；这些受管值不包含凭据，但不得输出整份表 options。

Paimon 1.3.2 的 Catalog ALTER 没有按旧值比较的 CAS 契约。本期不增加 owner 标记、租约或分布式锁；两个任务同时读取到 `MISSING` 后以不同值首次治理同一张表时，仍可能出现后写覆盖先写。该并发首次治理场景已被明确接受为低概率限制，Connector 只承诺立即回读验证，不宣称消除验证前后或补偿阶段的并发窗口。

## 12. 生命周期语义

四个受管选项在 Writer 创建前确定。Connector 不按 INITIAL 与 CDC 分别计算或切换配置。

| 任务形态 | 必须使用的配置 |
| --- | --- |
| 仅 INITIAL | 任务级归一化结果 |
| 仅 CDC | 任务级归一化结果 |
| INITIAL 后进入 CDC | 同一归一化结果，阶段切换不改变表属性和 Writer 语义 |
| INITIAL 与 CDC 混合 | 同一归一化结果 |

任务阶段只影响数据流，不改变 Spill 参数作用域。若框架因其他原因重建某张表的 Writer，新 Writer 仍必须先通过表配置门禁并使用同一归一化结果。

任务停止、Writer 重建失败和部分资源构造失败的关闭行为统一遵守第 7.3 节，不得由 INITIAL、CDC、DDL 或重试路径各自定义不同关闭顺序、异常覆盖或 live-dir 注销规则。

## 13. 错误、日志与可观测性

### 13.1 失败类别

实现必须能稳定区分以下错误类别，具体异常类名可在实现设计中确定：

| 类别 | 触发条件 | 必须包含的上下文 |
| --- | --- | --- |
| 配置非法 | 目标端归一化发现未知键、重复键、类型解析失败 | 键名、来源；不得输出整份配置 |
| 配置形态非法 | 原生容器、元素、propKey/propValue 或旧别名的运行时类型不符合第 5.1 节 | 稳定错误码、字段路径、实际类型；不得输出完整值对象 |
| 每表冲突 | `tableProperties` 声明受管键 | 逻辑表名、物理表名、冲突键 |
| 临时目录冲突 | 历史每表目录不一致或目录值非法 | 表名与规范化目录摘要 |
| 目标表枚举失败 | 无法得到完整目标表集合 | 任务标识、运行时映射类型 |
| 已有表属性冲突 | 已存在受管 key 与任务 typed value 不一致 | Catalog、数据库、物理表、冲突 key、当前/请求规范值；WARN 后拒绝 |
| Writer 构造约束非法 | 参数可解析但确定无法满足目标表 Paimon Writer 构造条件 | 物理表、写路径、受影响 key、Paimon 约束摘要 |
| 表治理失败 | ALTER、invalidate、reload 或回读失败 | Catalog、数据库、表、差异键、阶段 |
| Catalog 结果不确定 | CREATE/ALTER 异常后无法分类、出现部分应用或 Schema 分歧 | mutation 类型、结果分类、原始异常、最后可观察状态、逐键补偿/残留摘要 |
| 生命周期违规 | Writer 已创建后才尝试治理 | 物理表名、Writer 状态 |
| Writer 资源关闭失败 | 正常关闭或构造失败回收出现一个或多个异常 | 物理表、资源阶段、主异常、suppressed 数量；不得丢失异常链 |

### 13.2 首次目标端治理日志

只有首次目标端治理成功进入配置解析后，每个写任务输出一次归一化和治理摘要；纯源端、连接测试与 Schema 浏览不输出 Writer 参数解析结果：

- 四个原生键的规范有效值和来源类型。
- 是否使用了旧别名，以及被原生值覆盖的旧键名。
- 旧 `diskOverflowWrite=false` 是否触发语义纠正，以及因 key 缺失从默认 `true` 补写为 `false` 的目标表清单或数量。
- `diskTmpDir` 的来源类型。
- 可知目标表总数、已完整一致数、待补齐数、拒绝数、新建时治理数。
- 顺序启动检测到显式配置冲突时，必须先输出一次 WARN 再让任务失败；不得执行任何目标表 ALTER。
- `max-disk-size=0` 且目标表进入可 Spill 路径时输出一次 WARN，说明它不等价于禁止临时磁盘写入。
- ALTER、回读和补偿的逐表结果。
- mutation 异常后的 `NOT_APPLIED`、`APPLIED_EQUIVALENT`、`CONCURRENT_EXISTING`、`PARTIAL_OR_DIVERGED` 或 `UNKNOWN` 分类，以及最终 `RESTORED`、`RESIDUAL`、`UNKNOWN` 状态。

不得按数据行输出配置日志，不得输出 S3 密钥、Catalog 凭据、整份 Connection/Node 配置、整份 `TableSchema.options()`，也不得用 Gson/`toString` 记录包含完整 options 的 `Schema`/`finalSchema`。建表成功日志只允许表 Identifier、字段/主键/分区数量和受管四键的脱敏规范摘要。

## 14. Connector 代码边界

实现应保持以下职责边界，避免再次出现多处默认值：

| 职责 | 输入 | 输出/副作用 |
| --- | --- | --- |
| Writer 选项归一化 | 原始 node DataMap、旧别名 | 不可变的四键有效值与来源；无 Catalog 副作用 |
| 临时目录归一化 | 任务级值、历史每表值 | 单一任务级运行时目录；无表属性副作用 |
| 每表冲突校验 | 全部目标表配置 | 通过或明确失败；无 Catalog 副作用 |
| 目标端治理门禁 | 首次目标端回调、原始配置快照、完整 tableMap | 一次性成功，或通过现有 `PaimonServiceLifecycle` 发布粘滞失败；协调全部预检和 Catalog 变更 |
| 表选项协调 | 归一化结果、Catalog、目标表 | 逐 key 分类；只补齐缺失项；冲突拒绝；CREATE/ALTER、invalidate、reload、验证 |
| Writer Context 创建 | 已验证表、任务级目录 | `PaimonTableWriteContext`；不得修改表配置 |

### 14.1 Plus Connector 链路订正

本 Spec 只约束 `connectors/paimon-plus-connector`。审查时的旧链路与目标链路如下，后续类、测试和 Maven 命令都必须落在该 plus 模块：

| 阶段 | 审查时旧链路 | 本 Spec 目标链路 |
| --- | --- | --- |
| 启动配置 | `onStart` 直接 `load` connection/node，且会修改框架传入的 node DataMap | `onStart` 先保存递归防御性原始快照，再使用防御性副本加载 Bean；不解析四个 Writer 键或 Paimon IOManager 目录，不修改框架 DataMap，Bean 中可能保留的 raw 目录不作为本治理事实源 |
| 参数事实源 | `PaimonConfig` 的三个 Java 旧字段默认值会掩盖“用户是否配置” | 首次目标端门禁从原始快照按“原生键、真实存在的旧别名、Paimon 默认值”生成唯一不可变结果 |
| 新表 | 只按旧字段有条件写入部分 Writer options | 在合并其他表属性前显式写入完整四键，并在 CREATE 后 reload、typed verify |
| 已有表 | `createTable` 发现表存在后直接返回 | Writer 创建前逐 key 分类；仅补齐 `MISSING`，显式相等保留，显式冲突或非法值拒绝 |
| 每表覆盖 | `tableProperties` 后写，可覆盖旧链路生成的 Writer options | 在任何 Catalog 变更前拒绝四个受管键；`diskTmpDir` 只允许任务级治理 |
| Writer 创建 | 直接读取当时加载到的表 options，并允许每表读取 `diskTmpDir` | 只能消费治理成功并回读验证后的表以及同一个任务级目录，不得在 Writer 工厂内再次计算默认值或修改表属性 |

目标调用顺序固定为：`PaimonConnector` 保存原始快照 → 首次目标端回调触发任务级门禁 → 归一化四键和目录 → 复用 `PaimonWriteSemanticContractResolver` 完成全表 Catalog 零变更纯计划并标记 HASH_DYNAMIC Preflight → 对最终任务级目录执行 best-effort stale cleanup → IOManager smoke → 执行计划标记的 `PaimonDynamicBucketPreflight` → CREATE/ALTER → invalidate/reload/typed verify → `PaimonTableWriteContextFactory` 创建 Writer。任一失败不得通过旧字段路径或 `tableProperties` 旁路继续创建 Writer；只有 stale cleanup 失败按第 7.3 节 WARN 后继续，smoke 及其后的失败遵守各阶段阻断语义。

现有代码审查锚点：

- [`PaimonConnector#onStart`](../main/java/io/tapdata/connector/paimon/PaimonConnector.java)：原始 connection/node/table DataMap 的读取顺序、只读快照时机，以及 `connectionTest`/纯源端共用启动路径的边界。
- [`PaimonConfig`](../main/java/io/tapdata/connector/paimon/config/PaimonConfig.java)：继承的 Bean `load` 映射及 Java 默认字段导致的“存在性丢失”风险。
- [`PaimonService#createTable`](../main/java/io/tapdata/connector/paimon/service/PaimonService.java)：当前只对新表写属性、已有表直接返回，以及 `tableProperties` 后覆盖问题。
- [`PaimonTableWriteContextFactory`](../main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextFactory.java)：按物理表创建 Context、读取表最终 options 和 `diskTmpDir`。
- [`PaimonTableWriteContext`](../main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContext.java)：Writer Strategy、Committer、IOManager 的唯一所有权、关闭顺序、幂等和 suppressed 异常语义，必须保留。
- [`PaimonSpillDirCleaner`](../main/java/io/tapdata/connector/paimon/service/PaimonSpillDirCleaner.java)：JVM live-dir、跨进程 owner lock、stale grace 和滚动升级保护，必须复用；只移动 cleanup 调用时点和事实目录来源，不另写 Cleaner。
- [`PaimonBucketWriterStrategyFactory#requiresIoManager`](../main/java/io/tapdata/connector/paimon/service/PaimonBucketWriterStrategyFactory.java)：Bucket 模式对 IOManager 的独立需求。
- [`PaimonWriteSemanticContractResolver`](../main/java/io/tapdata/connector/paimon/service/PaimonWriteSemanticContractResolver.java)：已有新表/已有表确定性写语义校验，必须扩展并前移，不能在治理器重复实现。
- [`PaimonDynamicBucketPreflight`](../main/java/io/tapdata/connector/paimon/service/PaimonDynamicBucketPreflight.java)：已有 HASH_DYNAMIC UUID marker、污染扫描和 `GlobalIndexAssigner` 生命周期，必须在 Catalog mutation 前复用。
- [`PaimonServiceLifecycle`](../main/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycle.java)：已有任务级 sticky failure、ingress/stop 并发所有者；治理 Gate 必须接入而不是建立第二套终态。

### 14.2 Paimon 原生能力复用门禁

实现必须按“Paimon 公共 API 直接复用 → Connector 最小领域适配 → 有证据的例外”顺序决策。复用优先级是强制评审门禁，不是编码风格建议：

1. Paimon 已提供公共常量、类型、解析器、工厂或 Catalog 操作时，必须直接使用；Connector 不得建立第二套默认值、类型系统或等价工具类。
2. Connector 适配只允许处理 PDK/KeyValueEditor 数据形态、配置来源优先级、原始存在性、跨表编排、Catalog 零变更前置校验、错误上下文和日志等 Connector 特有职责；HASH_DYNAMIC marker/临时 IO 是第 5.2 节明确记录的预检副作用，不得被误称为完全无副作用。
3. Paimon 能力不可直接调用时，优先寻找公开门面间接复用。不得通过反射、包名伪装、复制粘贴 Paimon 源码或访问非公开成员建立脆弱耦合。
4. 新增 `*Utils`、`*Helper`、`*Parser`、`*Converter` 或职责等价的通用工具类，以及任何自定义解析、单位换算、默认值或路径拆分逻辑，默认视为不通过评审；只有满足第 15.5 节例外证据要求才可接受。
5. 允许存在 Writer 选项归一化器、表协调器等领域组件，但它们只能编排来源、状态与副作用，底层类型解析和 Paimon 语义必须委托本节矩阵中的原生入口。

| 能力 | Paimon 1.3.2 原生入口 | 必须复用的部分 | 允许的 Connector 最小适配 | 禁止事项 |
| --- | --- | --- | --- | --- |
| 受管键、默认值与 typed read | `CoreOptions.WRITE_BUFFER_SIZE`、`WRITE_BUFFER_SPILLABLE`、`WRITE_BUFFER_MAX_DISK_SIZE`、`WRITE_BUFFER_FOR_APPEND`，以及 `CoreOptions.fromMap`/typed getters | 通过常量 `.key()` 建立 allowlist；通过 `CoreOptions`/`Options` 取得默认值和类型值 | 保留原始 key 存在性、来源与任务级优先级 | 在 Java 业务实现中以字符串或自建常量重新声明 key/default；用 Java 字段默认值替代 Paimon 默认值 |
| Boolean 解析 | `Options#get(ConfigOption)`，内部委托 `OptionsUtils.convertToBoolean` | 大小写、非法值和异常语义 | 仅对任务 UI 输入先做 trim；为异常补充来源和 key 上下文 | `Boolean.parseBoolean`、自定义真假值集合、反射调用或复制包私有转换方法 |
| MemorySize 解析与规范化 | `MemorySize.parse/getBytes/toString`、`Options#get(ConfigOption)` | 单位、溢出、负数、字节比较和规范字符串 | 为旧 MB/GB 数字别名拼接其声明单位后交给 Paimon 解析 | 正则解析单位、自行维护倍率、手写 MAX_VALUE 字符串格式化逻辑 |
| Catalog 默认项 | `CatalogUtils.tableDefaultOptions`；`AbstractCatalog` 的 `putIfAbsent` 语义 | 计算新表最终 options 时保持显式值优先 | 在 CREATE 前合并副本用于无副作用预检 | 自定义不同优先级；把已持久化 Catalog 默认项标记为 `MISSING` |
| 表属性变更 | `SchemaChange.setOption/removeOption`、`Catalog#alterTable` | ALTER 表达和持久化 | 逐 key 差异计划、顺序执行和条件补偿编排 | 绕过 Catalog 直接修改 Schema 文件；重写 `PRESENT_EQUAL` |
| 缓存失效与回读 | `Catalog#invalidateTable`、`Catalog#getTable`；`CachingCatalog` 自动失效行为 | 使用 Catalog 生命周期入口 | 显式 invalidate、reload、typed verify 和错误包装 | 直接操作 Catalog 内部缓存；把 invalidate 调用成功等同于回读一致 |
| 临时目录与 IOManager | `IOManagerImpl.splitPaths`、`IOManager.create`，现有 `PaimonSpillDirCleaner` | Paimon 支持的分隔语法和 IOManager 创建；现有 live-dir/owner-lock/stale-grace 保护 | 在 `splitPaths` 前检查原始空片段；任务级目录迁移、首次 Gate cleanup 与生命周期持有 | 复制 `splitPaths`、自研 Spill Channel/目录选择轮询器、第二套 IOManager/注册表/Cleaner，或扫描被忽略的旧 per-table 根目录 |
| Writer Spill 接入 | `TableWrite#withIOManager`、Paimon TableWrite/Writer，现有 `PaimonTableWriteContext`/Factory | 将治理后的 options 和 IOManager 交给 Paimon Writer；保留现有唯一所有权 | 按 `Writer/Strategy → Committer → IOManager → unregister` 关闭；主异常与 suppressed、构造失败逆序释放和幂等 | Connector 再实现数据 Buffer、排序、Spill 或归并；增加第二层 Writer 包装或吞掉关闭异常 |
| Writer 写语义预检 | 现有 `PaimonWriteSemanticContractResolver`、Paimon `Schema`/`FileStoreTable` 公共入口 | bucket、merge、RowKind、Schema 与格式等现有确定性规则 | 将调用时点前移到 Catalog mutation 前，并把版本锚点订正为 1.3.2 | 在 Reconciler/Gate 重复实现同一语义或绕过现有测试 |
| HASH_DYNAMIC 历史预检 | 现有 `PaimonDynamicBucketPreflight`、Paimon `GlobalIndexAssigner`/`IndexBootstrap` | UUID marker、历史污染扫描和资源关闭 | 在 Catalog mutation 前按 marker 状态调用，并提前覆盖 6 page 确定性边界 | 另写扫描器、marker 或 GlobalIndex 生命周期 |
| Writer 构造约束预检 | `CoreOptions` typed values、现有 Resolver/Preflight 与 Paimon 构造器确定性条件 | 有公开读取入口的有效值和已有校验 | 仅把必然失败的构造条件前移到 Catalog 零变更阶段 | 把经验建议包装成合法性规则；创建通用“Paimon 校验框架”复制内核校验 |
| 服务失败与停止并发 | 现有 `PaimonServiceLifecycle`、`recordStickyFailure`、`commitLocks` | sticky failure、ingress、stop quiescence 和 per-table 序列化 | Gate single-flight 状态发布、registry 失效和固定锁顺序 | 第二套任务终态、持有 table lock 等待全局 gate、吞掉首次失败 |

矩阵源码依据：[CoreOptions typed getters](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/CoreOptions.java#L2354-L2368)、[Options public facade](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/Options.java#L77-L121)、[MemorySize](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/options/MemorySize.java#L145-L174)、[CatalogUtils](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/CatalogUtils.java#L99-L101)、[SchemaChange](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-api/src/main/java/org/apache/paimon/schema/SchemaChange.java#L82-L90)、[Catalog](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/catalog/Catalog.java#L297-L319)、[IOManager](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/disk/IOManager.java#L47-L52)、[TableWrite](https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableWrite.java#L37-L44)

plus 模块当前 POM 对编译插件存在重复声明，Maven 有效配置以靠后的 `source/target=11` 为准；本 Spec 不在审查阶段修改 POM。后续实现的编译、单元测试和模块构建统一使用 JDK 17 运行，并必须显式确认有效 `source/target=11`。单独使用 `source/target=11` 只能约束语法和 class 版本，不能阻止误用 JDK 17 API；验证命令还必须在 JDK 17 上通过 `release=11` 编译门禁，不能仅以普通 JDK 17 构建成功替代 Java 11 API 兼容性证明。

## 15. 验证要求

### 15.1 配置归一化测试

至少覆盖：

- 四个原生键全部缺失时得到 Paimon 1.3.2 默认值。
- 每个原生键单独配置和全部配置。
- MemorySize 等价字符串按字节比较。
- max-disk-size 缺失时得到 `MemorySize.MAX_VALUE`，不要求有限值。
- 未知键、重复键、空键、空值、非法布尔和非法 MemorySize 失败。
- 原生容器非 List/数组、null/非 Map 元素、缺失或非 String `propKey/propValue` 产生第 5.1 节稳定错误类别；额外展示字段被忽略。
- 旧键显式 null、旧 Boolean 非 Boolean、旧数值为小数/非有限数/字符串时产生 `LEGACY_VALUE_INVALID`，不能降级为缺失或被 `String.valueOf` 接受。
- 受管 Boolean 和 MemorySize 的成功值、失败值与异常分类通过 `Options#get(ConfigOption)` / `CoreOptions` 契约测试锁定；测试不得只覆盖 Connector 自定义返回值。
- 旧别名逐项迁移及单位正确。
- 新旧同键同时存在时原生值优先。
- 只把原始 DataMap 中真实存在的旧键视为旧配置。
- `write-buffer-for-append` 不因 Spill 开启而自动变为 `true`。
- 旧 `diskOverflowWrite=false` 的归一化结果为原生 `false`；已有表 key 缺失时报告默认 `true` 到显式 `false` 的实际变化，显式 `true` 时报告冲突并拒绝。
- `max-disk-size=0` 保持可解析，在可 Spill 写路径产生一次风险 WARN，不被描述为零磁盘硬限制。
- 原始嵌套 Map/List 在快照后被修改不会改变归一化结果；传入的 connection/node/table DataMap 深度不变。
- 目标 UI/Engine 完成“打开旧任务—保存—重新运行”回归；在未知字段保留能力未被证明前，隐藏且无默认值的 deprecated 字段可读旧值，但新建任务不生成旧键。

### 15.2 临时目录测试

至少覆盖：

- 任务级值优先。
- 无配置时使用 `/tmp`。
- 多表历史值相同可提升。
- 多表历史值不同失败。
- 每表目录不再影响单表 Writer 的最终目录选择。
- Paimon 支持的多目录分隔语法可传递，空片段失败。
- 非空合法目录先由 `IOManagerImpl.splitPaths` 拆分，再逐项 trim、绝对化和词法 normalize；空白项失败。
- `/tmp` 与 `/tmp/`、相对/绝对等价路径按规范列表比较；符号链接不解析，重复项与目录顺序保留且参与比较。
- 纯源端、连接测试和 Schema 浏览不解析非法目录；首次目标回调在 Writer/Catalog mutation 前失败。
- 任一已知写路径需要 IOManager 时，首次 mutation 前的 `IOManager.create`/close smoke 成功；无效、不可创建或不可关闭目录使 Catalog mutation 与 Writer 次数为零，并验证 live-dir 登记无泄漏。
- stale cleanup 在最终任务级目录归一化和全部纯计划通过后执行，严格早于 IOManager smoke、HASH_DYNAMIC Preflight 和首次 mutation；纯源端、连接测试、Schema 浏览以及被忽略的旧 per-table 根目录均不会触发扫描。
- 当前 JVM live-dir、其他进程持有 owner lock 的目录永不删除；满足 owner-lock 可取得且超过 grace period 的本版本遗留目录可删除；无 owner-lock 的旧版本目录保留并 WARN。清理枚举/加锁/删除异常只 WARN，后续 IOManager smoke 仍独立决定是否阻断任务。
- 不增加 S3/S3A 目录编码或 `fs.s3a.buffer.dir` 测试。

### 15.3 Catalog 协调测试

至少覆盖：

- 新表显式包含四个规范值，Catalog table defaults 无法覆盖。
- 已有表四键均显式存在且 typed value 一致时不 ALTER。
- 已有表四键均缺失时 ALTER 补齐全部四键，包括默认有效值相同的 key。
- 已有表部分 key 已存在且一致时只补齐缺失 key，不重写已有 key。
- 已有表任一已存在 key typed value 不一致时 WARN 并拒绝，所有已知表 ALTER 次数为零。
- 已有表包含非法受管值时失败而不是覆盖。
- 每表 `tableProperties` 含任一受管键时在 Catalog 变更前失败。
- ALTER 后回读不一致时 Writer 不创建。
- ALTER/CREATE 抛异常后 reload，分别覆盖 `NOT_APPLIED`、`APPLIED_EQUIVALENT`、`CONCURRENT_EXISTING`、`PARTIAL_OR_DIVERGED`、`UNKNOWN`；只有可证明等价的结果可继续。
- 多表后续 ALTER 失败时，对已变更表执行条件补偿。
- 并发修改导致当前值不再等于本任务写入值时跳过补偿。
- mutation 后失败的逐表最终状态必须区分 `RESTORED`、`RESIDUAL`、`UNKNOWN`，不得断言绝对零变更。
- 运行期新发现表在首个 Writer 前执行相同门禁。
- 同一运行期 Identifier 的并发首写只执行一次 plan/mutation/verify；任一逐表失败通过现有 Service Lifecycle 使整个任务 sticky failure。
- DROP/CLEAR 后治理 registry 失效；同名表 UUID 变化或 UUID 不可用时不能复用旧 VERIFIED，必须重新治理。
- 首次 `createTable`、`writeRecord`、`afterInitialSync` 任一路径都只能触发一次任务级门禁，并发调用不重复 ALTER。
- 门禁不得提前 CREATE 其他尚不存在的表；只有对应 `createTable` 回调消费 `PLANNED_CREATE` 计划，创建后回读验证。
- 实际建表请求与 `PLANNED_CREATE` 摘要不同会重新预检；并发建表转入已有表逐 key 治理，显式冲突不得被“表已存在”分支吞掉。
- 纯源端、`connectionTest` 和其他非写入上下文即使包含非法 Writer 参数，也不因缺少 tableMap 或未使用参数失败，不修改业务表；传入的 connection/node DataMap 保持不变。
- 写任务缺少或无法完整枚举预期 tableMap 时，在 Writer 创建前失败。

### 15.4 Writer 与阶段集成测试

使用 Paimon `1.3.2` 覆盖：

- 旧 `diskOverflowWrite=false` 对新表以及该 key 缺失的已有表产生 `write-buffer-spillable=false`；已有表若已显式为 `true`，则 WARN 并拒绝，不能覆盖。
- 原生配置覆盖旧别名，并进入最终 `CoreOptions`。
- 普通主键表 `write-buffer-size` 少于 3 个有效 `page-size` 时，在 Catalog 零变更状态下失败；恰好 3 page 可通过构造约束预检。`KEY_DYNAMIC` 因 bootstrap sorter 只获得一半缓冲，少于 6 page 时失败，恰好 6 page 可通过。HASH_DYNAMIC 在 marker 缺失或 UUID 变化、需要执行历史污染预检时覆盖相同 6 page 边界；有效 marker 命中路径不误加限制。
- 构造内存池的写路径对 `page-size=0`、`page-size > Integer.MAX_VALUE bytes` 或完整 page 数超过 `Integer.MAX_VALUE` 的组合在 Catalog 零变更状态下失败。
- INITIAL、CDC 和阶段切换读取同一组有效值。
- 主键表在 `write-buffer-spillable=true` 且 IOManager 可用时进入 Paimon Core External Sort Buffer；该验证不得描述为 Connector 自研 Spill。
- CDC 在提交条件检查前写入当前批次，测试必须允许当前批次先触发 Paimon Core Spill；不能断言 CDC 周期提交等于 CDC 不 Spill。
- `write-buffer-for-append=false` 时 Connector 不自行打开 append write buffer。
- `KEY_DYNAMIC` 等模式仍可按内核要求创建 IOManager，测试不把 `spillable=false` 错判为“绝无临时 IO”。
- 正常停止按 `Writer Strategy → Committer → IOManager → live-dir/owner-lock 注销` 执行，`close()` 幂等；任一步失败仍继续关闭后续资源，第一个异常保持主因，后续异常按发生顺序成为 suppressed。
- Writer/Committer/Strategy/IOManager 任一构造阶段失败时，已取得资源按唯一所有权逆序释放，原始构造异常保持主因，清理异常成为 suppressed，且不得重复关闭 raw Writer 或泄漏 live-dir/owner lock。
- `max-disk-size` 通过 CREATE/reload 后 raw option 与公共 `FileStoreTable#coreOptions().writeBufferSpillDiskSize()` 验证原生值传递，并在可稳定构造时验证公开可观察的 Spill 行为；不得用反射读取 `BinaryExternalSortBuffer` 私有字段，也不编写 Connector 目录总量阈值测试。若没有稳定公开行为观察点，测试必须明确止于公共 options 契约，不得声称已检查“内核对象配置”。
- 不存在 CDC 自动关闭、阶段 ALTER、动态选项覆盖或阶段切换重建 Writer 的行为。

### 15.5 原生能力复用审查

实现 PR 或配套 Implementation Plan 必须提交“Paimon 原生能力复用审计”，至少包含：

1. 列出本次新增或修改的所有参数解析、类型转换、单位换算、默认值、路径处理、Catalog 操作和 Writer 构造逻辑。
2. 对照第 14.2 节矩阵标明实际调用的 Paimon 类、常量和方法；直接复用时给出代码位置，不能只写“已复用”。
3. 列出所有新增的 `*Utils`、`*Helper`、`*Parser`、`*Converter` 或职责等价类。没有时明确写“无”；存在时逐项说明公开 Paimon API 为什么不能满足、尝试过的原生入口、最小职责边界和调用方。
4. 每个获准的自定义适配必须给出固定到 Paimon 1.3.2 的源码依据，以及与原生成功值、边界值和失败值对照的契约测试。
5. Paimon 版本升级时必须重新执行复用审计和契约测试；不能假定包私有实现、默认值、异常类型或字符串格式跨版本稳定。

以下任一情况必须阻断评审：

- 用 `Boolean.parseBoolean`、自定义正则或 switch 替代受管选项的 Paimon typed parsing。
- 在 Java 业务实现中以字符串或自建常量重新声明四个 key、Paimon 默认值、MemorySize 单位倍率或 `splitPaths` 分隔表达式。`spec.json`、本文档和直接对照 Paimon 的契约测试输入不受该字面量限制。
- 通过反射、包名伪装或复制 Paimon 源码调用非公开能力。
- 仅以“便于 mock”“调用更方便”或“以后可能复用”为理由增加通用工具类。
- 自定义适配只有 Connector 自测，没有与 Paimon 1.3.2 公共入口的对照测试。

允许的例外必须同时满足：Paimon 无可用公共入口、需求属于 Connector 特有边界、实现保持最小且不复制内核算法、源码依据明确、契约测试完整。任一条件不满足即不得合入。

### 15.6 建议验证命令

所有 Java 编译与测试使用 JDK 17；模块主源码仍按 Maven 有效 Java 11 目标验证：

```bash
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "${REPO_ROOT}"

PAIMON_JAVA17_HOME=$(/usr/libexec/java_home -v 17)
"${PAIMON_JAVA17_HOME}/bin/java" -version 2>&1 \
  | grep -Eq 'version "17\.' \
  || { echo "JDK 17 is required, but java_home resolved a different version" >&2; exit 1; }

JAVA_HOME="${PAIMON_JAVA17_HOME}" \
  mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 help:effective-pom \
  -Doutput=target/effective-pom.xml

COMPILER_EFFECTIVE_POM=connectors/paimon-plus-connector/target/effective-pom.xml
COMPILER_SOURCE="$(xmllint --xpath 'string(/*[local-name()="project"]/*[local-name()="build"]/*[local-name()="plugins"]/*[local-name()="plugin"][*[local-name()="artifactId"]="maven-compiler-plugin"]/*[local-name()="configuration"]/*[local-name()="source"])' "${COMPILER_EFFECTIVE_POM}")"
COMPILER_TARGET="$(xmllint --xpath 'string(/*[local-name()="project"]/*[local-name()="build"]/*[local-name()="plugins"]/*[local-name()="plugin"][*[local-name()="artifactId"]="maven-compiler-plugin"]/*[local-name()="configuration"]/*[local-name()="target"])' "${COMPILER_EFFECTIVE_POM}")"
test "${COMPILER_SOURCE}" = 11 && test "${COMPILER_TARGET}" = 11 \
  || { echo "Effective build compiler source/target must both be 11" >&2; exit 1; }

JAVA_HOME="${PAIMON_JAVA17_HOME}" \
  mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -DskipTests package

JAVA_HOME="${PAIMON_JAVA17_HOME}" \
  mvn -pl connectors/paimon-plus-connector \
  -Dmaven.compiler.release=11 \
  -Dtest=PaimonWriterOptionsNormalizerTest,PaimonTableOptionsReconcilerTest,PaimonTableWriteContextIntegrationTest \
  test

JAVA_HOME="${PAIMON_JAVA17_HOME}" \
  mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 test

jq empty connectors/paimon-plus-connector/src/main/resources/spec.json

git diff --check
while IFS= read -r file; do
  test -f "${file}" || continue
  if LC_ALL=C grep -nE '[[:blank:]]+$' "${file}"; then
    echo "Trailing whitespace in untracked file: ${file}" >&2
    exit 1
  fi
done < <(git ls-files --others --exclude-standard -- \
  connectors/paimon-plus-connector tasks)
```

若私有 SNAPSHOT 依赖或仓库访问阻断 Maven，必须把它报告为环境阻断，不能据此声称测试通过或实现失败。实现阶段若调整测试类名，必须同步更新为可直接执行的真实命令。

## 16. 验收标准

以下条件必须全部满足：

1. 新 UI 不展示且新任务不生成三个旧别名，只提供受限的 Paimon 原生 Writer 属性入口和任务级 `diskTmpDir`；旧任务编辑回存通过目标 UI/Engine 回归，必要时保留无默认值的 hidden/deprecated 兼容属性。
2. 归一化结果始终包含四个原生选项的完整有效值，并可追踪来源。
3. 旧任务无需改写任务 JSON 即可迁移；原生值与旧值冲突时原生值生效。
4. 旧 `diskOverflowWrite=false` 在已有表 key 缺失时显式持久化 `write-buffer-spillable=false`；已有表已显式配置不同值时 WARN 并拒绝覆盖。
5. 新建表显式保存四个规范值，Catalog 默认属性不能改变结果。
6. 已有表在首个 Writer 创建前完成逐 key typed compare：只补齐缺失项，已有不同值拒绝；最终四键全部显式持久化并完成缓存失效和回读验证。
7. 任一每表配置不能覆盖四个受管选项或任务级 `diskTmpDir`。
8. 所有物理目标表使用同一任务级配置值，同时承认 Paimon 实际资源对象仍是每表/每 Buffer 作用域。
9. Connector 不实现独立的数据 Spill Buffer；配置、IOManager 和提交节奏只驱动 Paimon Core Spill，不形成 Connector/Core 双重溢写。
10. INITIAL 与 CDC 不存在两套 Spill 配置、自动关闭或阶段切换逻辑；CDC 周期提交不得被解释为 CDC 不会 Spill。
11. Connector 不实现磁盘容量比例状态机，也不把 `max-disk-size` 描述为任务硬配额。
12. `write-buffer-for-append` 默认保持 `false`，不会因为 `write-buffer-spillable=true` 被自动打开。
13. `onStart` 只保存递归防御性的 Writer/目录原始配置快照并初始化必要连接资源；纯源端和其他非写入上下文不解析未使用参数、不修改业务表，首次目标端回调通过一次性任务级门禁完成四键与目录治理。
14. 首次任务级或运行期逐表门禁在任何 CREATE/ALTER 发出前发现枚举不完整、配置/Schema/写语义冲突或构造约束失败时，保证 Catalog 零变更和 Writer 零创建；mutation 发出后的失败只保证 Writer 零创建，必须按第 11.3–11.4 节回读分类、条件补偿并报告残留或未知状态，不作绝对零表变更承诺。
15. 使用 JDK 17 完成相关单元测试、集成测试和模块构建，并确认主源码仍以 Java 11 为编译目标；任何未执行或被环境阻断的验证均明确披露。
16. 实现通过第 14.2、15.5 节的 Paimon 原生能力复用审计：不存在无证据的新工具类、重复解析器、重复默认值或内核逻辑副本；所有获准的最小适配都有 Paimon 1.3.2 源码依据和对照测试。
17. 初始任务 Gate 与运行期 per-Identifier single-flight 都复用现有 `PaimonServiceLifecycle` 发布唯一 sticky failure；并发等待者看到同一结果，DDL/UUID 变化使治理 registry 失效，锁顺序测试证明不会与 stop/DDL/Writer 生命周期形成反向等待。
18. Writer Context 正常关闭和构造失败均满足第 7.3 节唯一所有权契约：关闭顺序固定、关闭幂等、主异常不被覆盖、后续异常使用 suppressed，IOManager、live-dir 和 owner lock 在所有路径无泄漏。
19. stale cleanup 只在首次目标端 Gate 使用最终规范任务级目录，并位于 IOManager smoke 与任何 Catalog mutation 之前；其他进程活跃目录和无 owner-lock 的旧版本目录不被删除，清理失败只 WARN，被忽略的旧 per-table 根目录不扫描。

## 17. 已知限制

- Paimon 1.3.2 的 Catalog ALTER 不提供跨表事务或按旧值 CAS；条件补偿不能消除全部并发窗口。
- CREATE/ALTER 请求可能在服务端已提交但客户端仍收到异常；reload 也可能失败，所以 mutation 发出后只能报告可观察状态和条件补偿结果，不能保证绝对零 Catalog 变更。
- 本期不自动 DROP mutation 异常后新出现的表；Catalog 没有可证明“由本任务创建且尚未被他人使用”的 CAS/owner 契约，自动删除可能造成数据损失。该表必须作为 `RESIDUAL` 报告并阻止 Writer，除非完整等价验证允许继续。
- 本期不使用 owner 标记、租约或分布式锁；两个任务同时首次治理同一未配置表时仍可能相互覆盖，顺序启动的不同配置则必须被拒绝。
- 表属性治理只能保证 Writer 创建时读取到一致值，不能控制外部进程随后再次 ALTER 同一张表。
- `write-buffer-spill.max-disk-size` 的内核检查范围和触发路径由 Paimon 决定，本 Connector 不增强为全局限额。
- 多表任务配置值一致不代表物理资源全局共享；每个表 Context、Writer、Buffer 仍按 Paimon 内核独立实例化。
- 多表 Writer、同表多个底层 Buffer 或外部 Flink/Paimon Writer 可以同时使用各自的 Paimon Core Spill；总磁盘和 IO 压力必须在部署层容量规划，不能从单个 Buffer 的 `max-disk-size` 推导。
- `write-buffer-spillable=false` 只约束对应 write-buffer 分支，不承诺所有 Bucket、compaction、lookup 或 append/postpone 路径都不使用临时 IO。
- stale cleanup 是 best-effort，不能保证异常退出遗留目录一定被删除；其他进程持锁、旧版本无 owner-lock、文件系统不支持可靠 advisory lock、枚举/加锁/删除失败时都会保留目录并要求运维确认。
- 被任务级目录覆盖或迁移时忽略的旧 per-table 根目录不自动扫描；这避免扩大误删范围，但历史遗留文件需要在所有相关旧任务停止后由运维清理。
- 目标 PDK API 的 `KVReadOnlyMap#iterator()` 默认可抛 `UnsupportedOperationException`；源码审查基线 `tapdata/iengine@e4e49ee6e60af72292624832619a88879aaf12b1` 的 `iengine-common/.../PdkTableMap.java` 已覆盖 iterator 并委托 `TapTableMap`，但实现仍必须以目标端运行时集成测试证明完整目标表枚举，不能只依赖接口类型或该历史 commit。
- S3/S3A fast-upload 缓冲目录不属于本 Spec；Paimon `diskTmpDir` 多目录规则不得被解释为对 `fs.s3a.buffer.dir` 的兼容保证。
