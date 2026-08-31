# Implementation Plan：Paimon Spill 与异步资源生命周期根治（Spec V5）

## 1. 计划状态

- 阶段：Spec-driven Development Phase 2；当前仅修订 Spec 与实施计划，不授权修改生产代码。
- 事实源：`connectors/paimon-plus-connector/src/doc/paimon-spill-compaction-lifecycle-root-fix-spec.md`（DRAFT v5）。
- Connector 基线：当前 `develop`；Paimon 基线：`1.3.2` / upstream commit `c05f7d1f1b1e5d37e64edab0f2978124d90b64f7`；Hadoop 基线：`3.3.6`。
- 现有工作区：保留用户已有 `pom.xml`、`.run/` 和 Connector 文档变更，不覆盖、不清理。
- 执行约束：严格按 `tasks/todo.md` 的依赖实施；每项最多修改 5 个文件，RED/GREEN 与证据未完成时不得越过门禁。

## 2. 目标与完成定义

根治同一故障族：父操作在 lazy/static child、compaction、maintenance、reader、spill 文件或 Hadoop raw FileSystem 仍可能运行/被借用时提前返回，上层随后关闭 IO 或删除目录，形成 use-after-close / delete-before-use。

完成必须同时满足：

1. Paimon lazy并发API返回显式`AutoCloseable` structured iterator/operation；read scope是唯一owner，所有success/failure/early-stop/interrupt路径执行`closeAndDrain()`。
2. `FAILED_DRAINED`只证明runnable已退出，不代表业务成功；仅maintenance SUCCESS且无`maintainError`、writer/committer均成功时允许关闭IO/spill/lease。
3. active termination wait与terminal retained严格分离；前者继续同一operation join，后者不在同进程重试未知部分close。
4. Factory、STOP、DDL、read和write使用同一proof/lease模型，无逆序close旁路、自等待或伪造proof。
5. Hadoop owned mode在Catalog创建/首次probe前启用；probe无遗失handle，first access single-flight，close/create线性化，禁止production反射与broad cache close。
6. patched Maven制品形成可解析的effective-POM闭包，不混装upstream/patched API/Common/Core。
7. deterministic real-spill、五种BucketMode、read/DDL/STOP并发、安全删除、性能与发布证据全部通过。

## 3. 源码证据基线

| 事实 | Paimon/Hadoop 位置 | 对 Plan 的约束 |
|---|---|---|
| lazy helper返回普通Iterator/Iterable | `paimon-api/.../ThreadPoolUtils.java:78-168` | 必须增加显式structured handle并审计全部consumer |
| manifest scan使用lazy helper | `paimon-core/.../operation/AbstractFileStoreScan.java:335-409` | read scope必须拥有handle并`closeAndDrain()` |
| deletion自有`allOf().get()` | `paimon-core/.../operation/FileDeletionBase.java:456-470` | 普通failure已等all-of；修interrupt与multi-error aggregation |
| maintenance先close commit、再`shutdownNow` | `paimon-core/.../table/sink/TableCommitImpl.java:348-411` | 改为graceful lifecycle outcome；`maintainError`非空即retained |
| bootstrap必经`ParallelExecution` | `paimon-core/.../crosspartition/IndexBootstrap.java:72-125` | 生产调用count=0；使用version-locked sequential adapter |
| async ORC reader使用static pool | `paimon-core/.../io/KeyValueFileReaderFactory.java:98-104,268-279`、`.../utils/AsyncRecordReader.java:37-110` | false时本次factory不构造AsyncRecordReader、不提交Future |
| access probe创建后丢失实例 | `paimon-common/.../fs/FileIO.java:609-619` | 复用validated FileIO或exact rollback，失败fail-fast |
| HadoopFileIO `get/create/put`非原子 | `paimon-common/.../fs/hadoop/HadoopFileIO.java:178-201` | `OwnedFileSystemEntry` single-flight与close/create状态机 |
| `newInstance`仍用unique cache key | Hadoop `FileSystem.java:585-611,3666-3743` | 不遍历cache；exact close只处理自己的unique entry |
| Core POM对多模块使用`${project.version}` | 本地`paimon-core-1.3.2.pom:38-112` | 归档effective POM并显式闭合unforked ecosystem版本 |

## 4. 硬门禁

| 门禁 | 通过条件 | 阻断范围 |
|---|---|---|
| G0：DDL产品决策 | 人工裁决Spec Q4：cumulative action-admission deadline及数值，或completion-driven | 仅阻断DDL Task 31及其下游；不阻断Paimon和Connector基础设施 |
| G1a：Paimon源码基线 | 可写fork、固定commit、模块与source JAR一致、RED fixture可复现 | 阻断Paimon补丁实施；不依赖G0 |
| G1b：Paimon制品闭包 | patched API/Common/Core、sources、effective POM、dependency tree、SHA-256与capability marker全部可追溯 | 阻断Connector依赖切换和集成 |
| G2：安全中间态 | 不存在旧Connector搭配语义改变但无gate的新内核，或新Connector搭配未修复内核 | 阻断合并、部署与回滚候选 |
| G3：生命周期证明 | 只有完整success barrier释放IO；WAITING和terminal retained均保持强引用并阻断global cleanup | 阻断STOP/DDL/global cleanup完成 |
| G4：测试真实执行 | 精确Surefire XML中关键测试`tests > 0`，全模块无filter测试通过 | 阻断发布 |

## 5. 不可回退的语义

### 5.1 Structured iterator/operation

- API返回`Iterator<T> + AutoCloseable`语义的显式handle，并提供幂等`closeAndDrain()`；handle持有本invocation全部tickets。
- ticket只能由child wrapper `finally`完成；Future done/cancelled不是termination proof。
- read scope在handle逃逸前登记，按“停止消费 → closeAndDrain → batch release → reader close”释放；消费0/1条同样执行。
- 无法暴露handle的调用边界必须eager drain；禁止回退普通lazy iterator。

### 5.2 状态与释放判定

```text
ACTIVE WAITING / CLOSE_DEFERRED_TERMINATION
  = 未证明终止；同一个 operation 继续 join；不发布 CLOSED

SUCCESS
  = parent + child runnable 已退出，maintainError == null

FAILED_DRAINED
  = parent + child runnable 已退出，但存在业务失败
  = dependency retained；最多继续独立且 exactly-once 的 committer close；禁止 IO/spill/lease release

DEPENDENCY_CLOSE_FAILED_RETAINED / IO_CLOSE_FAILED_RETAINED
  = terminal；保留 strong handle + sticky failure；同进程不重试未知部分 close
```

合法释放条件是：`compaction TERMINATED && writer close success && maintenance SUCCESS && maintainError == null && committer close success`。`FAILED_DRAINED`不得出现在允许IO close的判定中。

### 5.3 Factory与写路径

- proof reason仅`STOP | DDL | FACTORY_ROLLBACK`；fatal write只设置sticky fence与`failureOrigin=WRITE_PATH`，不能启动或冒充STOP teardown。
- Factory fixed safety order：proof → compaction TERMINATED → writer → maintenance closeAndDrain → committer → IO → spill unregister → lease release。
- GlobalIndex创建后立即staged-own；只有`endBoostrap`成功才transfer，外部注入对象是`IOManager`且不得被assigner close。
- Context/generation持有一个全生命周期WRITER physical lease；每次write/commit只获取operation admission并在表锁后revalidate，不重复申请physical lease。

### 5.4 DDL

- 有Context时继续持有其WRITER lease；无Context时申请DDL_ONLY。DDL等待其他foreground/read borrower，绝不等待自己持有的lease。
- 先fence目标表read，再request/join child；deadline先于action时保持active deferred，只允许内部STOP join，restart后用户显式重试。
- 成功或失败都保持当前finally cache/guard cleanup语义；action failure原子转移WRITER/DDL_ONLY到`RetainedDdlActionLease`，callback count=0。

### 5.5 Hadoop owned FileSystem

- owner mode进入CatalogContext/options并早于`CatalogFactory.createCatalog`和`FileIO.checkAccess`。
- access check复用同一validated FileIO；无法复用时exact rollback provisional，rollback失败fail-fast。
- `OwnedFileSystemEntry`分离operational wrapper与exact raw owner，状态`OPEN -> CLOSING -> CLOSED_SUCCESS | CLOSE_FAILED_RETAINED`。
- create在锁外，reservation/publish/close在线性化短锁；close join in-flight creator，losing/late raw exact-close。
- HDFS/S3A验证raw identity；`file://`验证LocalFileIO/stream隔离；native`s3://`不由S3A证明代替。
- production cleanup/capability禁止反射；只读诊断反射不得unwrap/close/clear或通过gate。

### 5.6 Spill安全

- manager必须是stable real approved root的direct child，basename严格`paimon-io-<UUID>`。
- root/manager/marker/traversal全部NOFOLLOW，删除前revalidate parent、basename、fileKey；identity缺失即fail-closed。
- 优先`SecureDirectoryStream` descriptor-relative deletion；平台无等价能力时自动递归cleanup fail-closed。

## 6. 架构与依赖图

```mermaid
flowchart TD
    T0[0 Baseline] --> T2[2 Structured API]
    T0 --> T4[4 FileDeletion drain]
    T0 --> T6[6 Async reader option]
    T0 --> T7[7 GlobalIndex cleanup]
    T0 --> T8[8 Secured wrapper non-owning]
    T2 --> T3A[3A Manifest lazy consumers]
    T3A --> T3B[3B Remaining lazy consumers]
    T2 --> T5[5 Maintenance lifecycle]
    T3B --> T5
    T8 --> T9[9 FileIO access probe]
    T8 --> T10[10 Hadoop owned entry]
    T9 --> T10
    T2 --> T11[11 Capability ABI]
    T5 --> T11
    T6 --> T11
    T7 --> T11
    T10 --> T11
    T11 --> T12[12 Maven closure]
    T12 --> T13[13 Publish artifacts / G1b]
    T4 --> T13

    T13 --> T14[14 Connector dependency gate]
    T14 --> T15[15 Coordinator admission]
    T14 --> T16[16 Lease/proof state]
    T14 --> T17[17 Spill path capability]
    T15 --> T17
    T16 --> T17
    T14 --> T18[18 Compaction runtime]
    T14 --> T20[20 Runtime table]
    T14 --> T30[30 Read scope core]
    T18 --> T19[19 Prepared writer]
    T2 --> T21[21 Sequential bootstrap]
    T3B --> T21
    T6 --> T21
    T20 --> T21
    T7 --> T22[22 HASH preflight / staged index]
    T17 --> T22
    T20 --> T22
    T21 --> T22
    T5 --> T23[23 Maintenance adapter]
    T16 --> T24[24 Write lifecycle]
    T18 --> T24
    T19 --> T24
    T23 --> T24
    T17 --> T25[25 Factory rollback]
    T20 --> T25
    T22 --> T25
    T24 --> T25
    T25 --> T26[26 Context activation]
    T26 --> T27[27 Real spill test]
    T15 --> T28[28 Service coordinator adoption]
    T16 --> T28
    T26 --> T28
    T24 --> T29[29 STOP/write-failure]
    T28 --> T29
    T2 --> T30
    T3B --> T30
    T15 --> T30
    T16 --> T30
    G0{{G0 Q4}} --> T31[31 DDL orchestration]
    T16 --> T31
    T29 --> T31
    T30 --> T31
    T20 --> T32[32 Read integration]
    T21 --> T32
    T28 --> T32
    T30 --> T32
    T31 --> T32
    T15 --> T33[33 Global barrier]
    T16 --> T33
    T29 --> T33
    T30 --> T33
    T32 --> T33
    T9 --> T34[34 Hadoop integration]
    T10 --> T34
    T14 --> T34
    T33 --> T34
    T22 --> T35[35 Bucket/E2E]
    T27 --> T35
    T31 --> T35
    T32 --> T35
    T34 --> T35
    T35 --> T36[36 Test execution gate]
    T36 --> T37[37 Performance/deployment]
    T37 --> T38[38 Release evidence]
```

## 7. 实施阶段与 Checkpoint

### Phase A：Paimon结构化并发生命周期（Task 0、2–7）

- 输出：structured API、全lazy consumer审计、FileDeletion interrupt/multi-error、maintenance outcome、async-reader显式disable、GlobalIndex自有状态清理。
- Checkpoint A：0/1-item early-stop latch证明`closeAndDrain()`在sibling返回前不结束；`FAILED_DRAINED`使IO close count=0。

### Phase B：Hadoop ownership与制品闭包（Task 8–13）

- 输出：non-owning wrapper、access-probe复用、single-flight owned entry、actual-instance capability、effective-POM闭包及immutable artifacts。
- Checkpoint B：S3A probe只有一个live raw；create/close竞态无lost handle；dependency tree无patched/upstream stack混装。

### Phase C：Connector写生命周期（Task 14–27）

- 输出：startup gate、coordinator、proof/lease、spill security、runtime table、sequential bootstrap、fixed-order Factory rollback与deterministic real-spill测试。
- Checkpoint C：Factory只返回`SAFE_ROLLBACK | RETAINED_RESTART_REQUIRED`；真实spill在worker阻塞时目录存在且IO/action close count=0。

### Phase D：Service、read、STOP、DDL与global barrier（Task 28–34）

- 输出：Service adoption、STOP/write failure拆分、read-scope structured ownership、DDL exact lease、global barrier、Hadoop实际实例收口。
- Checkpoint D：read未`closeAndDrain()`时DDL action=0且global close=0；fatal write不触发teardown；production reflection helper彻底删除。

### Phase E：回归、性能与发布（Task 35–38）

- 输出：五BucketMode、Surefire XML、W1-W4、部署清理调查、release manifest。
- Checkpoint E：所有hard threshold通过，关键测试`tests > 0`，failure matrix每行都有test/event/outcome证据。

## 8. 测试执行门禁

Paimon fork定向测试应按模块运行，避免跨reactor指定不存在测试造成假失败；如果必须单命令，使用`-Dsurefire.failIfNoSpecifiedTests=false`并检查XML：

```bash
mvn -pl paimon-api -Dtest=ThreadPoolUtilsTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl paimon-common -Dtest=HadoopFileIOTest,HadoopSecuredFileSystemTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl paimon-core -Dtest=FileDeletionStructuredDrainTest,TableCommitMaintenanceLifecycleTest,KeyValueFileReaderFactoryTest,GlobalIndexAssignerTest -Dsurefire.failIfNoSpecifiedTests=false test
mvn -pl paimon-api,paimon-common,paimon-core -am -DskipITs test
```

Connector精确测试使用Surefire `*Test`，不得以`*IT`配合`-DskipITs`冒充执行：

```bash
mvn -pl connectors/paimon-plus-connector -am \
  -Dtest=PaimonServiceResourceCoordinatorTest,PaimonWriteResourceLifecycleTest,PaimonRuntimeTableFactoryTest,PaimonSequentialIndexBootstrapTest,PaimonReadResourceScopeTest,PaimonTableReadBorrowTest,PaimonHadoopFileSystemOwnershipTest,PaimonSpillDirCleanerSecurityTest,PaimonCompactionSpillLifecycleTest,KeyDynamicBucketWriterStrategyTest \
  -Dsurefire.failIfNoSpecifiedTests=false test

mvn -pl connectors/paimon-plus-connector -am test
```

CI必须保存Surefire XML并断言至少`PaimonCompactionSpillLifecycleTest`、`PaimonHadoopFileSystemOwnershipTest`、`PaimonSequentialIndexBootstrapTest`、`KeyDynamicBucketWriterStrategyTest`各自`tests > 0`。

## 9. Definition of Done

- [ ] Spec V5 Required语义无回退；G0仅阻断DDL，G1a/G1b无自依赖。
- [ ] structured iterator/operation由read scope `closeAndDrain()`；0/1-item early-stop、error、interrupt均有latch证据。
- [ ] `FAILED_DRAINED`、active WAITING、terminal retained在代码、测试、metrics和Failure Matrix完全一致。
- [ ] Factory fixed safety order、GlobalIndex staged ownership、write/commit admission与physical lease合同通过。
- [ ] Hadoop probe/single-flight/close-create、HDFS/S3A/file差异、unique cache membership与禁止reflection通过。
- [ ] patched Maven effective-POM闭包、coordinates、sources、SHA-256、dependency tree与capability marker齐全。
- [ ] deterministic real-spill阈值、事件偏序、>=3 runs/readers、无`.channel ENOENT`通过。
- [ ] 五BucketMode和W1-W4 hard gate通过；默认全模块测试通过且XML证明关键测试实际执行。
- [ ] 未修改Paimon snapshot/offset/callback/exactly-once边界，未引入`prepareCommit(true)`。
- [ ] 人工评审批准后才进入编码阶段。

## 10. 尚需人工裁决

仅保留Spec Q4：DDL是否启用cumulative action-admission deadline及默认值。该决策不改变任何安全不变量；未裁决时DDL implementation task保持blocked，其他任务可继续。
