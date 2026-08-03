# Implementation Plan: Paimon Plus Writer Spill 原生参数统一治理

## 1. Overview

本计划实现 [`paimon-writer-spill-options-governance-spec.md`](../connectors/paimon-plus-connector/src/doc/paimon-writer-spill-options-governance-spec.md) 的修订后契约。目标是在 `connectors/paimon-plus-connector` 内建立唯一的任务级 Writer 配置事实源，在任何 Writer 创建前完成配置、目录、Schema、既有写语义和 Catalog 治理，并诚实处理 CREATE/ALTER 已发出后的不确定结果。

本计划只描述后续实现顺序；当前规划阶段不修改 Java、测试、`spec.json` 或 POM。所有实现和测试使用 JDK 17 运行，并以 `maven.compiler.release=11` 作为 Java 11 API 兼容门禁。S3/S3A fast-upload 缓冲目录明确不在本计划范围内。

## 2. Architecture Decisions

- `onStart` 只递归防御性复制治理所需的 Writer/目录原始输入；四键与 Paimon IOManager 所用目录都延迟到首次目标端回调解析，框架传入的 connection/node/table DataMap 一律不修改。目录原始值可继续进入防御性 Bean load 供 out-of-scope 旧路径使用，但不能成为 Paimon IOManager 事实源。
- 原生容器、条目和旧别名采用 Spec 第 5.1 节的稳定形态错误分类；嵌套 Map/List 必须深复制，不能把 `DataMap.create` 的浅复制当作不可变快照。
- typed parsing、默认值、规范化和 key 常量统一复用 Paimon 1.3.2 的 `CoreOptions`、`Options`、`MemorySize`、`Schema`、`CatalogUtils`、`SchemaChange`、`IOManagerImpl.splitPaths` 和 `IOManager.create`。
- 必须扩展并前移现有 `PaimonWriteSemanticContractResolver` 与 `PaimonDynamicBucketPreflight`；新的 Reconciler/Gate 不复制 bucket、merge、RowKind、HASH_DYNAMIC marker/扫描或 3/6 page 规则。
- Catalog 使用“全部可知表先做纯计划 → 使用最终任务级目录执行一次 best-effort stale cleanup → 执行 IOManager create/close smoke 与计划标记的 HASH_DYNAMIC 只读/marker 预检 → 顺序 mutation”的分段模式；mutation 前各段不得修改业务 Catalog。cleanup 只扫描最终目录，失败只 WARN；mutation 抛异常后必须 reload，分类为 `NOT_APPLIED`、`APPLIED_EQUIVALENT`、`CONCURRENT_EXISTING`、`PARTIAL_OR_DIVERGED` 或 `UNKNOWN`。
- 初始任务 gate 与运行期 per-Identifier single-flight 复用现有 `PaimonServiceLifecycle`、`recordStickyFailure` 和 `commitLocks`。Gate 缓存并发完成结果，但不拥有第二套 sticky failure 终态。
- `PLANNED_CREATE` 使用 Paimon `Schema` 完整值语义并递归防御性持有，不保存可变 `TapTable`/DataMap/options 引用。`VERIFIED` 绑定 Identifier 与表 UUID；DDL 或 UUID 变化使 registry 失效。
- 旧 UI 字段不展示且新任务不生成。目标 UI/Engine 未证明未知字段可原样回存前，保留无默认值、`x-display=hidden` 的 deprecated 兼容属性。
- 计划中的新领域类是当前最小实现假设，不是数量指标。每个新增类编码前都必须证明现有 Paimon API 和 Connector 类不能承载该职责；不得为了满足固定类数量形成 God object，也不得无证据增加通用工具类。
- 现有 `PaimonTableWriteContext`/Factory 是 Writer Strategy、Committer、IOManager 与 live-dir/owner-lock 的唯一所有权边界；正常关闭顺序、构造失败逆序释放、幂等和主异常/suppressed 语义必须保留，不增加第二层 Writer 包装、资源注册表或 Cleaner。
- 不修改 Paimon 内核、不增加依赖、不修改 POM；受影响源码中的 Paimon `1.3.1` 注释/链接随代码修改统一校正到 `1.3.2`。

## 3. Dependency Graph

```text
Task 1 UI/backward-compat contract ─────┐
                                        ├──▶ Task 3 deep startup snapshot
Task 2 native typed normalization ──────┘        + delayed temp directory
                                                 │
                                                 ▼
Task 4 reuse existing semantic/dynamic preflight
                                                 │
                                                 ▼
Task 5 pure per-table governance plan
                                                 │
                                                 ▼
Task 6 ALTER/reload/outcome/compensation
                                                 │
                                                 ▼
Task 7 task + per-Identifier single-flight gate
                         │                       │
                         ├──▶ Task 8 CREATE path │
                         │                       ▼
                         └──────────────▶ Task 9 existing/writer/DDL invalidation
                                                 │
                                                 ▼
Task 10 task directory + stale cleanup
                         │
                         ▼
Task 11 Writer/Committer/IOManager ownership ──▶ Task 12 errors/logging
                                                 │
                                                 ▼
                                      Task 13 full release gate
                                                 │
                                                 ▼
                                      Task 14 audit/document sync
```

## 4. Paimon/Connector 原生能力复用审计（计划态）

| 实现能力 | 必须直接复用的入口 | Connector 最小适配 | 禁止事项 |
| --- | --- | --- | --- |
| 受管 key/default/typed read | `CoreOptions` 四个 `ConfigOption`、`CoreOptions.fromMap`、typed getters | 原始存在性、来源优先级、稳定错误上下文 | 字符串 key 常量、重复默认值、自建类型系统 |
| Boolean | `Options#get(ConfigOption)` | 任务 UI String 先 trim；已有表 raw value 不 trim | `Boolean.parseBoolean`、真假值集合、反射/源码复制 |
| MemorySize | `MemorySize.parse/getBytes/toString`、`Options#get` | 旧 Number 的十进制表示拼接 MB/GB | 单位正则、倍率、MAX_VALUE 格式化器 |
| Schema 摘要 | Paimon `Schema` 字段、复制/`equals` 语义 | 防御性持有 Identifier、目录、来源 | 自建遗漏字段的 TapTable 比较协议 |
| Catalog defaults | `CatalogUtils.tableDefaultOptions`、Paimon `putIfAbsent` 语义 | CREATE 前在副本中计算最终 options | 改变优先级、把持久化 default 当作 missing |
| 既有写语义 | `PaimonWriteSemanticContractResolver` | 前移调用时点、补齐 1.3.2 锚点 | 在 Reconciler/Gate 复制 bucket/merge/RowKind 规则 |
| HASH_DYNAMIC 预检 | `PaimonDynamicBucketPreflight`、`GlobalIndexAssigner`、`IndexBootstrap` | mutation 前按 marker/UUID 状态调用 | 新扫描器、第二套 marker、重复 GlobalIndex 生命周期 |
| ALTER/补偿 | `SchemaChange.setOption/removeOption`、`Catalog#alterTable` | 差异计划、结果分类、条件补偿与残留报告 | 绕过 Catalog、重写 `PRESENT_EQUAL` |
| 缓存/回读 | `Catalog#invalidateTable/getTable` | raw presence + typed verify、异常分类 | 直接操作 Catalog 内部缓存 |
| 目录/IOManager | `IOManagerImpl.splitPaths`、`IOManager.create`、JDK `Path.normalize`、现有 `PaimonSpillDirCleaner` | 空片段检查、有序绝对词法规范列表；首次 Gate best-effort stale cleanup | 复制 split 正则、排序/去重、自研 Spill、第二套 Cleaner/注册表、扫描被忽略的旧 per-table 根目录 |
| 服务终态/锁 | `PaimonServiceLifecycle`、`recordStickyFailure`、`commitLocks` | gate future、registry、固定锁顺序 | 第二套 sticky state、持锁等待全局 gate |
| Writer Spill | `TableWrite#withIOManager`、Paimon Writer、现有 `PaimonTableWriteContext`/Factory | 唯一所有权；固定关闭顺序；构造失败逆序释放；主异常/suppressed；幂等 | Connector 数据 Buffer/排序/Spill/归并、第二层 Writer 包装、吞掉关闭异常 |

计划新增领域类：

- `PaimonWriterOptionsNormalizer`：把已深复制的 PDK 原始形态编排为不可变任务级结果；底层解析全部委托 Paimon。
- `PaimonTableOptionsReconciler`：建立并执行单表 Catalog 差异计划及结果分类；既有写语义委托 Resolver/Preflight。
- `PaimonWriterGovernanceGate`：协调初始任务 gate、per-Identifier single-flight、`PLANNED_CREATE`/`VERIFIED` registry，并把失败发布给现有 Service Lifecycle。

实现者不得依据这份清单机械新建类。若现有类扩展后不再需要某个计划类，应优先减少；若确需其他类，必须先更新本审计，给出职责、调用方、Paimon/Connector 既有入口不足的源码证据和契约测试。

## 5. Task List

### Phase 1: Configuration Foundation

## Task 1: 发布 UI 契约并锁定旧任务回存

**Description:** 将新配置入口调整为任务级 `paimonWriterProperties` 与 `diskTmpDir`，同时用保守 hidden/deprecated 兼容方案保护旧任务，直到目标 UI/Engine 证明未知字段能够原样回存。

**Acceptance criteria:**
- [ ] 新 UI 只展示受限四键 KeyValueEditor 和任务级 `diskTmpDir`；新任务 JSON 不生成三个旧键。
- [ ] 未证明未知字段回存能力前，三个旧属性保留为无默认值、`x-display=hidden` 的 deprecated schema 属性，不参与新任务默认值。
- [ ] 本地 schema/序列化测试证明 hidden 字段无默认值且旧 JSON 值可读取；目标环境证据延迟到 Task 13，未取得前不得删除兼容属性或宣称 UI 迁移完成。

**Verification:**
- [ ] `jq empty connectors/paimon-plus-connector/src/main/resources/spec.json`
- [ ] JDK 17 targeted test: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonSpecTest test`
- [ ] If a target UI/Engine is already available, record optional characterization evidence; definitive open-save-run remains Task 13.

**Dependencies:** None

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/resources/spec.json`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/PaimonSpecTest.java`

**Estimated scope:** S (2 files)

## Task 2: 建立 Paimon 原生 typed 归一化和深快照契约

**Description:** 先用测试锁定四键、旧别名、原始容器形态和递归防御性快照，再实现领域归一化；所有 Boolean、MemorySize、默认值和规范字符串由 Paimon 公共 API 提供。

**Acceptance criteria:**
- [ ] 四键结果完整、不可变、带 `NATIVE`/`LEGACY`/`PAIMON_DEFAULT` 来源，新值逐 key 优先。
- [ ] 非容器、null/非 Map 条目、非 String key/value、旧值 null/错类型/小数/字符串产生 Spec 规定的稳定类别。
- [ ] 快照后修改原始嵌套 Map/List 不改变结果；不存在自定义 parser、单位倍率、默认值或受管 key 常量。

**Verification:**
- [ ] JDK 17 targeted test: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonWriterOptionsNormalizerTest test`
- [ ] Contract tests compare success、boundary、failure behavior directly with Paimon 1.3.2 `Options/CoreOptions/MemorySize`.

**Dependencies:** None; may run in parallel with Task 1 after plan approval.

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/config/PaimonWriterOptionsNormalizer.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/config/PaimonWriterOptionsNormalizerTest.java`

**Estimated scope:** S (2 files)

## Task 3: 修正启动快照并延迟任务级目录治理

**Description:** 在 `PaimonConfig.load` 前捕获治理相关深快照并用防御性副本加载配置；四键和 Paimon IOManager 目录在首次目标回调独立解析，纯源端与连接测试不消费未使用的 Writer 输入。

**Acceptance criteria:**
- [ ] connection/node/table 原对象及嵌套内容启动后保持不变；Writer 容器/旧别名不进入 Bean load，目录即使供既有 out-of-scope 路径加载也不能被 Paimon治理读取为事实源。
- [ ] 首次目标门禁按 Paimon split 结果执行 trim、绝对化、词法 normalize 和有序比较；不解析符号链接、不去重、不排序。
- [ ] 纯源端、connection test、Schema 浏览即使 Writer/目录输入非法也不失败；进入目标回调后在 Catalog mutation/Writer 前失败。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonConnectorWriterConfigTest,PaimonConfigTest,PaimonConnectorControlTest test`
- [ ] Mutation tests change nested source values after snapshot and assert normalized output remains stable.

**Dependencies:** Tasks 1 and 2

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/PaimonConnector.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/config/PaimonConfig.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/config/PaimonWriterOptionsNormalizer.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/PaimonConnectorWriterConfigTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/PaimonConfigTest.java`

**Estimated scope:** M (5 files)

### Checkpoint A: Configuration Foundation

- [ ] Tasks 1–3 targeted tests pass under JDK 17 with `release=11`.
- [ ] 原始输入深度不变，旧任务回存策略有证据，纯源端延迟解析边界通过。
- [ ] Human review approves configuration foundation before Catalog work.

### Phase 2: Source-backed Preflight and Catalog Reconciliation

## Task 4: 前移并扩展既有写语义与动态 Bucket 预检

**Description:** 不新建通用校验框架；扩展现有 Resolver/Preflight，使新表 `validateNewTable`、已有表 `resolve` 和已有 HASH_DYNAMIC 表的 marker/污染扫描都在首次 Catalog mutation 前完成；新 HASH_DYNAMIC 表只做 Schema/6-page 预检，CREATE 回读 UUID 后再建立 Writer 前验证状态。受影响源码锚点统一到 Paimon 1.3.2。

**Acceptance criteria:**
- [ ] 普通 SortBuffer 3 page、KEY_DYNAMIC 6 page，以及 HASH_DYNAMIC marker 缺失/UUID 变化时的 6 page 边界在 Catalog 零变更阶段验证。
- [ ] HASH_DYNAMIC 有效 marker 不误加未执行路径的限制，非法 marker/污染扫描失败时 CREATE/ALTER/Writer 次数均为零。
- [ ] bucket、merge、RowKind、Schema/format 等规则只存在于已有 Resolver/Preflight，不在后续 Reconciler/Gate 重复。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonWriteSemanticContractResolverTest,PaimonDynamicBucketPreflightTest test`
- [ ] Review confirms modified source comments and links target Paimon 1.3.2 commit `c05f7d1f...`.

**Dependencies:** Task 3

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonWriteSemanticContractResolver.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonDynamicBucketPreflight.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriteSemanticContractResolverTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonDynamicBucketPreflightTest.java`

**Estimated scope:** M (4 files)

## Task 5: 生成 Catalog 零变更的单表治理计划

**Description:** 建立纯计划 Reconciler，调用 Task 4 的 Resolver，并仅为已有 HASH_DYNAMIC 表标记是否需要后续历史 Preflight；完成受管 tableProperties 冲突、Catalog defaults 合并、逐 key typed classification 和完整 Paimon `Schema` 摘要。本任务不得执行 marker/污染扫描或 CREATE/ALTER。

**Acceptance criteria:**
- [ ] 已有表精确分类 `MISSING`、`PRESENT_EQUAL`、`PRESENT_CONFLICT`、`PRESENT_INVALID`，持久化 Catalog defaults 不被当成缺失。
- [ ] 新表最终 options 保持任务受管值优先，`PLANNED_CREATE` 防御性绑定完整 Paimon Schema、Identifier、目录和写语义摘要。
- [ ] 实际请求的任意字段/type/nullability/default/PK/partition/comment/options 变化都使摘要不等价并触发重新预检。

**Verification:**
- [ ] JDK 17 targeted test: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonTableOptionsReconcilerTest test`
- [ ] Test double proves plan phase calls Resolver、只生成 DynamicPreflight requirement，不写 state marker、不创建 IOManager，也不调用 `Catalog#createTable/alterTable`.

**Dependencies:** Task 4

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonTableOptionsReconciler.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonTableOptionsReconcilerTest.java`

**Estimated scope:** S (2 files)

## Task 6: 执行 ALTER、回读分类和条件补偿

**Description:** 在纯计划通过后只 ALTER 缺失 key，显式 invalidate/reload/typed verify；mutation 抛异常时 reload 判定实际结果，并对已证明写入的值执行条件补偿和残留报告。

**Acceptance criteria:**
- [ ] 只 ALTER `MISSING`，不重写 `PRESENT_EQUAL`；回读四键必须 raw present 且 typed equal。
- [ ] ALTER 异常覆盖 `NOT_APPLIED`、`APPLIED_EQUIVALENT`、`PARTIAL_OR_DIVERGED`、`UNKNOWN`，不以“方法抛异常”推断未提交。
- [ ] 补偿仅在当前值仍等于本任务写入值时恢复/移除，最终报告 `RESTORED`/`RESIDUAL`/`UNKNOWN`，原始异常保持主因。

**Verification:**
- [ ] JDK 17 targeted test: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonTableOptionsReconcilerTest test`
- [ ] Local Catalog fault injection covers “服务端已应用但客户端抛异常”、partial visibility、reload failure and concurrent-value skip.

**Dependencies:** Task 5

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonTableOptionsReconciler.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonTableOptionsReconcilerTest.java`

**Estimated scope:** S (2 files)

### Checkpoint B: Preflight and Catalog Safety

- [ ] Tasks 4–6 targeted tests pass.
- [ ] mutation 前的确定性失败证明 Catalog 零变更；mutation 后的测试只声明可证明的分类和补偿结果。
- [ ] Human review approves Paimon API reuse and Catalog side effects.

### Phase 3: Gate and Production Paths

## Task 7: 建立任务级与 per-Identifier single-flight Gate

**Description:** Gate 原子执行首次全表治理并为运行期表维护 per-Identifier 状态；全部纯计划通过后按固定顺序调用 cleanup hook、任务级 Paimon IOManager create/close smoke 和计划标记的 HASH_DYNAMIC Preflight，全部通过才进入 Catalog mutation。Task 10 接入现有 Cleaner；并发等待者共享完成结果，除 best-effort cleanup WARN 外的失败通过现有 Service Lifecycle 发布。

**Acceptance criteria:**
- [ ] 并发目标回调只执行一次任务治理；运行期同一 Identifier 只执行一次 plan/preflight/mutation/verify，等待者得到同一结果。
- [ ] Gate 失败调用 `recordStickyFailure`/`PaimonServiceLifecycle#fail`，后续所有写入口被同一首次失败拒绝；Gate 不维护独立终态。
- [ ] cleanup hook → IOManager smoke → HASH_DYNAMIC 扫描的顺序固定且全部早于 mutation；cleanup hook 失败只 WARN，smoke/扫描失败发布 sticky failure，所有路径 live-dir 无泄漏；测试同时锁定 lifecycle → gate → per-Identifier → `commitLocks` 顺序。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonWriterGovernanceGateTest,PaimonServiceLifecycleTest test`
- [ ] Barrier/latch tests assert exactly-once operations, same failure publication and stop quiescence.

**Dependencies:** Task 6

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGate.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycle.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGateTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceLifecycleTest.java`

**Estimated scope:** M (5 files)

## Task 8: 接通新表创建和 CREATE 不确定结果

**Description:** 将 Gate/`PLANNED_CREATE` 接入 plus `createTable`。重新生成 Schema 摘要后才可 CREATE；异常后 reload，等价表可继续，并发表按已有表治理，分歧/未知状态 sticky failure 且不自动 DROP。

**Acceptance criteria:**
- [ ] 新表显式持久化四键，Catalog defaults/tableProperties 不能覆盖；计划摘要变化在 CREATE 前重新预检。
- [ ] CREATE 异常完整覆盖 `NOT_APPLIED`、`APPLIED_EQUIVALENT`、`CONCURRENT_EXISTING`、`PARTIAL_OR_DIVERGED`、`UNKNOWN`。
- [ ] 非等价或未知表不创建 Writer、不自动 DROP，并报告 `RESIDUAL/UNKNOWN`；等价/兼容表须 reload、typed verify，新 HASH_DYNAMIC 表还须绑定实际 UUID 并完成 Writer 前 marker 状态。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonServiceCreateTableValidationTest,PaimonWriterGovernanceIntegrationTest test`
- [ ] Fault-injecting Catalog simulates create committed before exception and concurrent non-equivalent creation.

**Dependencies:** Task 7

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/PaimonConnector.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGate.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceCreateTableValidationTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceIntegrationTest.java`

**Estimated scope:** M (5 files)

## Task 9: 接通已有表、Writer 门禁和 DDL registry 失效

**Description:** 将同一 Gate 接入 `writeRecord`、`afterInitialSync`、Writer context 与 `runTableDdl`；VERIFIED 绑定 UUID，DDL/UUID 变化后重新治理。

**Acceptance criteria:**
- [ ] 未 VERIFIED 表不能进入 Writer factory；INITIAL、CDC、阶段切换和 Writer 重建共用任务结果，不执行阶段 ALTER。
- [ ] 运行期表失败使整个 Service sticky failure；同名表 UUID 变化或 UUID 缺失时不能复用旧 VERIFIED。
- [ ] DROP/CLEAR/其他 DDL 在现有派生缓存失效点清除治理 registry，DDL 后首写 reload 并重新治理。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonWriterGovernanceIntegrationTest,PaimonTableWriteContextIntegrationTest,PaimonServiceInitialSyncPendingTest test`
- [ ] Tests cover drop/recreate same Identifier, UUID change, UUID unavailable, concurrent DDL/write and zero Writer on failure.

**Dependencies:** Task 8

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGate.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceIntegrationTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextIntegrationTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceInitialSyncPendingTest.java`

**Estimated scope:** M (5 files)

### Checkpoint C: End-to-End Governance

- [ ] Tasks 7–9 targeted tests pass.
- [ ] 新表、已有表、运行期表、并发回调、DDL/recreate 和 stop 都使用同一生命周期与失败结果。
- [ ] Human review approves production path and lock/state-machine behavior.

### Phase 4: Runtime Resource and Observability

## Task 10: 统一任务级目录并迁移 stale cleanup 时点

**Description:** 将首次目标门禁产生的有序规范目录列表作为 cleanup 和后续 Writer 资源的唯一事实源；全部纯计划通过后、IOManager smoke/HASH_DYNAMIC Preflight/任何 mutation 前执行一次现有 Cleaner 的 best-effort stale cleanup，不处理 S3/S3A。

**Acceptance criteria:**
- [ ] 最终目录按 split → trim → absolute lexical normalize 形成有序不可变列表；cleanup 和后续 Writer 只消费该列表，顺序/重复项保留，被覆盖或忽略的旧 per-table 根目录只 WARN、不扫描。
- [ ] cleanup 严格位于全部纯计划之后、IOManager smoke/HASH_DYNAMIC Preflight/首次 mutation 之前；纯源端、connection test 和 Schema 浏览不触发，枚举/加锁/删除失败只 WARN 且不发布 sticky failure。
- [ ] 复用现有 JVM live-dir、跨进程 owner lock、stale grace 与滚动升级保护：活跃锁目录不删除、无 owner-lock 的旧版本目录保留；不新增 Cleaner/注册表，不修改 S3/S3A 路径。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonWriterGovernanceGateTest,PaimonSpillDirCleanerTest test`
- [ ] Order/fault-injection tests prove cleanup-before-smoke-before-preflight-before-mutation, best-effort failure continuation, active-lock protection, stale deletion, lockless legacy preservation and ignored-root non-observation.

**Dependencies:** Task 9

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGate.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonSpillDirCleaner.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGateTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonSpillDirCleanerTest.java`

**Estimated scope:** M (5 files)

## Task 11: 固化 Writer、Committer 与 IOManager 唯一所有权

**Description:** 将最终任务级目录接入所有需要 IOManager 的 Writer/Bucket 路径，并用现有 `PaimonTableWriteContext`/Factory 固化正常关闭和构造失败的资源所有权；不恢复旧 `ManagedIOStreamTableWrite` 包装链路。

**Acceptance criteria:**
- [ ] 所有物理表 Context 只消费 Gate 的最终任务级目录；`spillable=false` 不阻断 HASH_DYNAMIC/KEY_DYNAMIC 的独立 IOManager 需求，Writer 路径不再调用 per-table 目录 getter。
- [ ] 正常停止按 `Writer Strategy → Committer → IOManager → live-dir/owner-lock 注销` 执行且幂等；任一步失败仍继续，首个异常为主异常，后续异常按顺序成为 suppressed。
- [ ] 任一构造阶段失败时按唯一所有权逆序释放；Strategy 已拥有 raw Writer 时不重复关闭，原始构造异常保持主因，IOManager、live-dir 和 owner lock 无泄漏。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonTableWriteContextFactoryTest,PaimonTableWriteContextTest test`
- [ ] Failure-injection tests cover every allocation boundary, exact close order, idempotence, suppressed ordering, no double-close and no live-dir/owner-lock leak; two-table test proves same ordered roots with independent `paimon-io-*` subdirectories.

**Dependencies:** Task 10

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContext.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextFactory.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonTableWriteContextFactoryTest.java`

**Estimated scope:** M (4 files)

## Task 12: 完成错误分类与安全日志

**Description:** 实现稳定输入形态、冲突、mutation 结果和残留状态错误；移除/替换当前记录完整 `finalSchema` 的日志，只记录最小安全摘要。

**Acceptance criteria:**
- [ ] Spec 错误类别包含最小上下文，原始 mutation 异常为主因，reload/补偿异常和残留状态可诊断。
- [ ] 不记录完整 DataMap、`TableSchema.options()`、`Schema/finalSchema`、凭据或每行配置；建表日志只含 Identifier、计数和受管摘要。
- [ ] 每任务一次归一化摘要；纯源端无 Writer 摘要；显式冲突先 WARN 后失败且 mutation 次数为零。

**Verification:**
- [ ] JDK 17 targeted tests: `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -Dtest=PaimonServiceWriteErrorLogTest,PaimonWriterGovernanceGateTest,PaimonWriterGovernanceIntegrationTest test`
- [ ] Manual/automated capture asserts full `finalSchema` and unrelated options never appear in logs.

**Dependencies:** Task 11

**Files likely touched:**
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonService.java`
- `connectors/paimon-plus-connector/src/main/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceGate.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonServiceWriteErrorLogTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceIntegrationTest.java`

**Estimated scope:** M (4 files)

### Checkpoint D: Runtime and Operational Safety

- [ ] Tasks 10–12 targeted tests pass.
- [ ] stale cleanup、Paimon IOManager/Writer/Committer 唯一所有权、目录语义、失败分类和日志脱敏符合 Spec。
- [ ] Human review approves full release gate to begin.

### Phase 5: Release and Documentation Closure

## Task 13: 执行完整 JDK 17/Java 11 与目标环境门禁

**Description:** 执行 Spec 全矩阵、Plus 模块回归、JDK 17 构建、Java 11 release 校验、目标 Engine tableMap 枚举和旧任务 UI 回存。未执行验证保持未完成。

**Acceptance criteria:**
- [ ] Spec 第 15–16 节每条要求映射到真实测试/人工证据，类名和命令可直接执行。
- [ ] JDK 17 下 effective build compiler source/target 为 11，`release=11` package 和全量测试成功。
- [ ] 目标 Engine `PdkTableMap` 完整枚举、并发/多表治理及旧任务 open-save-run 通过；环境不可用时发布门禁保持阻断。

**Verification:**
- [ ] `REPO_ROOT="$(git rev-parse --show-toplevel)"; cd "${REPO_ROOT}"; PAIMON_JAVA17_HOME=$(/usr/libexec/java_home -v 17); "${PAIMON_JAVA17_HOME}/bin/java" -version`
- [ ] `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 -DskipTests package`
- [ ] `env JAVA_HOME="$(/usr/libexec/java_home -v 17)" mvn -pl connectors/paimon-plus-connector -Dmaven.compiler.release=11 test`
- [ ] Run Spec §15.6 exact effective-POM、JSON、tracked/untracked whitespace commands.

**Dependencies:** Task 12

**Files likely touched:**
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/service/PaimonWriterGovernanceIntegrationTest.java`
- `connectors/paimon-plus-connector/src/test/java/io/tapdata/connector/paimon/PaimonConnectorWriterConfigTest.java`

**Estimated scope:** S (2 files)

## Task 14: 更新实际复用审计并同步文档状态

**Description:** 用真实类、方法、测试和命令结果替换计划态假设，同步 Spec、Plan、Todo 状态；任何偏离、未运行测试或外部阻断必须保留，不能把文档提前标成完成。

**Acceptance criteria:**
- [ ] 实际复用审计列出所有解析、转换、默认值、路径、Catalog、预检和 Writer 变更及其 Paimon/Connector 原生入口。
- [ ] 所有新增类逐项说明职责和复用证据；无通用工具类，或例外已获人工批准且有 Paimon 1.3.2 对照测试。
- [ ] Spec/Plan/Todo 的状态、源码链接、测试类名、结果、阻断和已知限制与最终实现一致。

**Verification:**
- [ ] `git diff --check` plus Spec §15.6 untracked-file whitespace loop.
- [ ] Review searches for duplicated keys/defaults/parsers/split logic、reflection、Paimon 1.3.1 stale links and unsupported completion claims.
- [ ] Human review explicitly approves implementation and documentation before merge.

**Dependencies:** Task 13

**Files likely touched:**
- `connectors/paimon-plus-connector/src/doc/paimon-writer-spill-options-governance-spec.md`
- `tasks/plan.md`
- `tasks/todo.md`

**Estimated scope:** M (3 files)

### Checkpoint E: Complete

- [ ] All task acceptance criteria and project Definition of Done pass.
- [ ] Full Plus tests、JDK 17/Java 11 release build、Engine/UI external gates pass.
- [ ] Actual Paimon reuse audit has no unexplained exception.
- [ ] Human review explicitly approves merge readiness.

## 6. Spec Traceability

| Spec section | Plan coverage | Completion evidence |
| --- | --- | --- |
| 5–6 配置、形态、深快照、旧迁移 | Tasks 1–3、13 | UI schema、typed/raw-shape tests、nested mutation、open-save-run |
| 7 `diskTmpDir`、资源关闭与 stale cleanup | Tasks 3、7、10–11 | delayed parse、canonical ordered paths、cleanup ordering/protection、unique ownership、suppressed/no-leak tests |
| 8 每表冲突 | Tasks 5、8 | Catalog-zero plan、new-table final options |
| 9 目标表/Gate/registry | Tasks 5、7–9、13 | complete enumeration、single-flight、UUID/DDL invalidation、Engine integration |
| 10 新建表 | Tasks 5、8 | immutable Schema summary、CREATE outcome classes、reload verify |
| 11 已有表/补偿 | Tasks 5–9 | classification、ALTER outcomes、conditional compensation、residual report |
| 12 生命周期 | Tasks 7、9、11 | existing lifecycle integration、INITIAL/CDC shared result、stop/DDL locks、resource close contract |
| 13 错误/日志 | Task 12 | stable taxonomy、safe summaries、full-schema log removal |
| 14 Plus 边界/原生复用 | Tasks 2、4–12、14 | existing Resolver/Preflight/Lifecycle/Context/Cleaner reuse、actual audit |
| 15 验证 | Tasks 1–14 | targeted tests、public observability、full commands |
| 16 验收 | Tasks 13–14 | evidence matrix、external gates、human approval |
| 17 已知限制 | Tasks 6–10、12–14 | no transaction/CAS、unknown outcome、no auto-DROP、best-effort cleanup、ignored legacy roots、runtime evidence |

## 7. Parallelization Opportunities

- Tasks 1 and 2 may run in parallel after human plan approval; Task 3 waits for both.
- Tasks 4–9 are sequential because each consumes the prior preflight/plan/apply/gate contract and several edit `PaimonService` or shared tests.
- Task 10 is kept after Task 9 because cleanup ordering consumes the final Gate directory fact source and shares `PaimonService`/Gate tests.
- Tasks 10–14 are sequential because cleanup ordering establishes the directory fact source consumed by Context ownership, followed by logging, release and audit gates.

## 8. Risks and Mitigations

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Catalog mutation committed before client exception | High | reload classification; equivalent may continue, divergent/unknown sticky failure; conditional ALTER compensation and residual report |
| 多表 ALTER 无事务/CAS | High | all-table preflight, stable order, conditional compensation, no zero-change claim after mutation |
| HASH_DYNAMIC preflight opens half-buffer sorter | High | reuse marker-aware existing preflight before mutation; test missing/stale/valid marker 6-page paths |
| Gate 与 lifecycle/DDL lock inversion | High | one sticky owner; fixed lock order; no commit-lock wait on global gate; barrier tests |
| Drop/recreate reuses stale VERIFIED | High | UUID binding, UUID-unavailable no-cache rule, DDL invalidation hook |
| UI schema projection drops legacy aliases | High | hidden/no-default fallback until target open-save-run proves preservation |
| `KVReadOnlyMap#iterator()` default unsupported | High | fail-fast and target Engine `PdkTableMap` runtime gate fixed to reviewed commit evidence |
| Paimon private state unobservable | Medium | assert raw persisted options + public `CoreOptions`; no reflection or unsupported “kernel object verified” claim |
| Shallow DataMap snapshot aliases nested input | Medium | recursive domain snapshot and post-capture mutation tests |
| stale cleanup deletes another process's active directory | High | reuse live-dir + advisory owner lock + grace; never age-delete locked or lockless legacy dirs; scan only final task roots |
| 关闭/构造失败掩盖主异常或泄漏 IOManager | High | unique Context ownership, fixed close order, suppressed failures, idempotence and allocation-boundary fault injection |
| best-effort cleanup leaves historical disk residue | Medium | WARN with minimal path context; IOManager smoke guards current writes; ignored/lockless legacy roots require operator cleanup |
| POM compiler plugin duplicated | Medium | no POM change; JDK 17 + `release=11`; XPath-check active build plugin source/target |
| Private SNAPSHOT/Engine/UI environment unavailable | Medium | report environment blocker; Tasks 13–14 and merge readiness remain incomplete |

## 9. Open Questions / External Gates

- Task 13 requires a target Engine `PdkTableMap` integration environment or approved equivalent fixture. Reviewed source baseline: `tapdata/iengine@e4e49ee6e60af72292624832619a88879aaf12b1`.
- Task 1/12 require target UI/Engine old-task open-save-run evidence. Until available, hidden/deprecated aliases must remain and final release cannot claim complete UI migration proof.
- If runtime cannot expose a stable public behavior for `write-buffer-spill.max-disk-size`, acceptance stops at persisted raw value plus `FileStoreTable#coreOptions().writeBufferSpillDiskSize()`; no reflection and no stronger claim.

## 10. Project-wide Definition of Done

每个任务除自身 acceptance criteria 外，还必须满足：

- [ ] 新行为具有先失败后通过的测试，错误路径和边界值均覆盖。
- [ ] targeted tests、既有回归和真实运行验证通过；未执行项不算完成。
- [ ] 无重复业务逻辑、无调试输出、无无关重构；tracked/untracked 文本格式检查通过。
- [ ] 向后兼容、Catalog 副作用、不确定结果、补偿/残留和 DDL 失效均有证据。
- [ ] Writer/Committer/IOManager 关闭顺序、构造失败、suppressed、幂等以及 stale cleanup 的锁/时点/扫描边界均有故障注入证据，资源注册无泄漏。
- [ ] 用户配置、Spec、Plan、Todo 与源码事实固定到 Paimon 1.3.2。
- [ ] 日志不泄露凭据、完整配置/options/Schema，关键失败可诊断。
- [ ] 人工评审批准后才能合入。

## 11. Plan Review

审查结论：修订后的计划已覆盖上一轮确认的 P1/P2/P3 问题，并将高风险工作前移为独立、可验证的任务：纯计划复用现有 Resolver 并标记 HASH_DYNAMIC 扫描；Gate 使用最终目录先执行受保护的 best-effort stale cleanup，再执行 IOManager smoke 与标记的 Preflight；mutation 不确定结果先于生产链路；Gate 复用现有 Lifecycle；DDL/UUID 失效和资源唯一所有权分别验证。14 个任务均为 S/M，每个任务预计不超过 5 个文件，依赖和验证命令不再引用尚未创建的后续测试。

文档层审查通过不等于实现完成。当前保留两个明确外部门禁：目标 Engine tableMap 完整枚举，以及旧任务 UI open-save-run。它们不阻断前置实现，但阻断 Task 13、Task 14 和最终完成声明。
