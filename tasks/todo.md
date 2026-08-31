# Tasks：Paimon Spill 与异步资源生命周期根治（Spec V5）

> 状态：待人工评审。当前任务清单不授权修改生产代码。每项最多修改 5 个文件；若源码审计发现超出范围，必须先拆任务并更新 Plan，不能扩大当前任务。

## Gate 与基线

### Task 0：固定双仓库源码与RED基线

- **依赖**：无。
- **工作**：固定Connector/Paimon/Hadoop commit、Java/Maven版本；用`rg`生成`ThreadPoolUtils`全部调用点清单；复现旧路径`.channel ENOENT`、lazy 0/1-item abandon、S3A probe leak和Hadoop first-access race。
- **文件（≤2）**：仅测试证据/审计清单，不改生产文件。
- **验收**：1) commit与调用点清单可复查；2) 四类RED fixture稳定复现；3) 不修改生产行为。

### Task 1：裁决G0 DDL action-admission deadline

- **依赖**：无；只阻断Task 31及其下游。
- **工作**：由产品/运维owner选择cumulative deadline及值，或completion-driven；记录“不interrupt已开始action、timeout不构成termination proof”。
- **文件（≤3）**：Spec、`tasks/plan.md`、`tasks/todo.md`。
- **验收**：1) Q4有签字结论；2) failure matrix/metrics/测试期望同步；3) 未裁决不阻断Task 2–30。

## Paimon Fork：structured concurrency

### Task 2：定义显式AutoCloseable structured operation API

- **依赖**：Task 0 / G1a。
- **工作**：新增`StructuredIterator<T>`或等价API；`ThreadPoolUtils`返回handle，持有全部tickets；`closeAndDrain()`幂等、first failure + suppressed、中断后恢复flag；ticket仅由wrapper finally完成。
- **文件（≤4）**：`paimon-api/.../StructuredIterator.java`、`ThreadPoolUtils.java`、`ThreadPoolUtilsTest.java`、API兼容说明。
- **验收**：1) 消费0/1/all均需显式close；2) sibling latch释放前close不返回；3) error/interrupt聚合与flag符合Spec。

### Task 3A：迁移manifest lazy主调用链

- **依赖**：Task 2。
- **工作**：让Manifest wrapper、`FileEntry`、`AbstractFileStoreScan`传播structured handle；不能用`.iterator()`丢失owner；无法暴露的边界eager drain。
- **文件（≤5）**：`ManifestReadThreadPool.java`、`FileEntry.java`、`AbstractFileStoreScan.java`、对应structured scan test、调用点审计清单。
- **验收**：1) 0/1-item scan early-stop可closeAndDrain；2) parent资源在sibling返回前close count=0；3)本组无普通lazy handle逃逸。

### Task 3B：迁移其余lazy helper consumer

- **依赖**：Task 2、Task 3A。
- **工作**：迁移`IncrementalDeltaStartingScanner`、`IcebergCommitCallback`、`ListUnexistingFiles`、`LocalOrphanFilesClean`；逐个决定向上暴露handle或边界eager drain。
- **文件（≤5）**：上述4个生产类、一个参数化structured-consumer test。
- **验收**：1) Task 0清单除TableCommit专属路径外全部闭合；2) early-stop/error/interrupt无dangling ticket；3)无fixed sleep/thread-name polling。

### Task 4：统一FileDeletion interrupt与多错误drain

- **依赖**：Task 2。
- **工作**：保留`allOf`普通child failure会等全部child的事实，只把caller interrupt和multi-error aggregation迁移到canonical drain helper；不无依据修改FileOperationThreadPool。
- **文件（≤4）**：`FileDeletionBase.java`、`FileDeletionStructuredDrainTest.java`、`FileOperationThreadPoolTest.java`、测试fixture。
- **验收**：1) interrupt后仍等所有runnable-returned；2)first + suppressed稳定；3)FileOperationThreadPool compatibility通过。

### Task 5：TableCommit maintenance结构化生命周期

- **依赖**：Task 2、Task 3A、Task 3B。
- **工作**：以opaque handle提供graceful shutdown/join/outcome；移除所有正常close旁路中的`shutdownNow()`和“先commit.close”；`checkFilesExistence`消费structured handle；drain后统一读取SYNC/ASYNC `maintainError`。
- **文件（≤4）**：`TableCommitImpl.java`、maintenance handle/outcome类、`TableCommitMaintenanceLifecycleTest.java`、commit-file structured test。
- **验收**：1) outcome仅SUCCESS/FAILED_DRAINED/WAITING；2)maintainError非空返回同Throwable的FAILED_DRAINED；3)FAILED_DRAINED时IO释放资格为false。

### Task 6：显式禁用AsyncRecordReader

- **依赖**：Task 0。
- **工作**：新增稳定Core option；false时本次factory invocation不构造`AsyncRecordReader`、不提交reader Future；保留upstream默认行为。
- **文件（≤4）**：`CoreOptions.java`、`KeyValueFileReaderFactory.java`、factory test、option test。
- **验收**：1) ORC且`fileSize=Long.MAX_VALUE`仍同步；2)true保持原路径；3)不宣称删除classloader-static executor对象。

### Task 7：GlobalIndexAssigner自有状态清理

- **依赖**：Task 0。
- **工作**：close清理自有stateFactory/key index、bootstrapKeys、bootstrapRecords、RocksDB/path；null-safe/idempotent；不得关闭外部注入`IOManager`。
- **文件（≤3）**：`GlobalIndexAssigner.java`、`GlobalIndexAssignerCleanupTest.java`、failure fixture。
- **验收**：1) open/bootstrap/endBoostrap三点故障通过；2)buffer/index/path恰好清理；3)IOManager close count=0。

## Paimon Fork：Hadoop ownership与制品

### Task 8：固定secured wrapper non-owning合同

- **依赖**：Task 0 / G1a。
- **工作**：分离operational wrapper与raw owner概念；generic/cached wrapper close不转发delegate，测试注入不隐式取得ownership。
- **文件（≤3）**：`HadoopSecuredFileSystem.java`、ownership contract test、测试FS。
- **验收**：1)wrapper close raw=0；2)owned FileIO后续可exact-close raw；3)shared mode不被误关。

### Task 9：修复FileIO access probe生命周期

- **依赖**：Task 8。
- **工作**：让`checkAccess`返回并复用已configure/validate的FileIO；无法复用时finally exact-close provisional；rollback close失败fail-fast且suppressed保留。
- **文件（≤4）**：`FileIO.java`、loader/result helper、`FileIOAccessProbeTest.java`、fallback测试loader。
- **验收**：1)S3A/fallback success只有一个live raw；2)configure/exists失败exact rollback；3)rollback失败不继续fallback。

### Task 10：实现OwnedFileSystemEntry single-flight与关闭状态机

- **依赖**：Task 8、Task 9。
- **工作**：短锁reservation、锁外create、短锁publish；`OPEN -> CLOSING -> CLOSED_SUCCESS | CLOSE_FAILED_RETAINED`；close join in-flight creator；losing/late raw exact-close；failure sticky且不重试。
- **文件（≤5）**：`HadoopFileIO.java`、`OwnedFileSystemEntry.java`、owned test、race test、unique-cache test。
- **验收**：1)first-access双向latch无lost raw；2)close/create线性化且close后无新reservation；3)close failure重复调用raw close count不增加。

### Task 11：定义patched capability ABI

- **依赖**：Task 2、5、6、7、10。
- **工作**：用编译期/公开稳定marker声明structured API、maintenance、async disable、GlobalIndex cleanup、access probe和owned FS feature set；绑定实际FileIO mode能力，不提供反射fallback。
- **文件（≤4）**：capability接口/实现、ABI test、mixed-version test。
- **验收**：1)原版1.3.2 RED；2)完整补丁GREEN；3)缺一feature或实际FileIO非owned均fail-fast。

### Task 12：闭合Maven effective-POM依赖图

- **依赖**：Task 11。
- **工作**：区分patched stack version与upstream ecosystem version；Core/Common/API patched，codegen-loader/format/filesystem/plugin等未fork模块显式固定1.3.2；父POM可解析。
- **文件（≤5）**：Paimon parent/BOM或发布POM、API/Common/Core POM（合计不超过5）。
- **验收**：1)effective POM无不存在的`1.3.2-tapdata.1` ecosystem坐标；2)dependency tree无upstream/patched stack混装；3)clean repository可解析构建。

### Task 13：发布不可变Paimon修复制品

- **依赖**：Task 4、12；本任务完成后形成G1b，不把G1b写成自身前置条件。
- **工作**：执行模块定向与全量测试；发布binary/sources/POM；归档commit、patch、effective POM、dependency tree、SHA-256和capability manifest。
- **文件（≤2）**：release manifest、测试/制品证据索引。
- **验收**：1)所有坐标非SNAPSHOT且不可变；2)关键XML tests>0；3)从空仓库按manifest可复现。

## Connector：基础设施与写生命周期

### Task 14：Connector依赖拆分与actual-instance启动门禁

- **依赖**：Task 13。
- **工作**：POM拆分patched stack/upstream ecosystem版本并显式patched API；owned option在CatalogContext/createCatalog前设置；capability绑定实际Catalog-owned FileIO/resolved scheme/mode。
- **文件（≤5）**：Connector `pom.xml`、`PaimonService.java`初始化段、capability gate类、gate test、dependency test。
- **验收**：1)原版/混版/late opt-in均启动失败且无新资源；2)patched actual instance通过；3)生产gate不使用反射。

### Task 15：建立ResourceCoordinator admission与锁序

- **依赖**：Task 14。
- **工作**：实现service/table admission、read fence、operation join与规范锁序；锁内只做状态转换，禁止外部调用/等待。
- **文件（≤3）**：`PaimonServiceResourceCoordinator.java`、coordinator test、lock-order fixture。
- **验收**：1)双向latch证明无lost admission；2)无锁内await/close；3)表A fence不阻塞表B。

### Task 16：实现exact lease、proof与close状态模型

- **依赖**：Task 14。
- **工作**：generation-scoped WRITER/DDL_ONLY lease；proof reason仅STOP/DDL/FACTORY_ROLLBACK；分开active WAITING与terminal retained；delegate close progress一次性。
- **文件（≤5）**：`PhysicalTableWriterLease.java`、proof/outcome类、retained registry类、lease test、outcome test。
- **验收**：1)ABA/exact compare-remove通过；2)FAILED_DRAINED不能释放IO；3)terminal close failure同进程不重试。

### Task 17：实现spill路径能力与安全删除

- **依赖**：Task 14、15、16。
- **工作**：approved root stable identity、direct child、严格UUID basename、NOFOLLOW/fileKey revalidation；优先SecureDirectoryStream，缺失等价能力时自动递归cleanup fail-closed。
- **文件（≤4）**：`PaimonSpillDirCleaner.java`、path capability类、security test、filesystem fixture。
- **验收**：1)越界/symlink/identity缺失或变化均不删；2)合法stale目录descriptor-relative删除；3)日志不泄露credential URI。

### Task 18：实现Connector-owned CompactionRuntime

- **依赖**：Task 14。
- **工作**：own executor、submission fence、graceful shutdown与actual TERMINATED proof；不提供无Future handle的force path。
- **文件（≤3）**：`PaimonCompactionRuntime.java`、runtime test、executor fixture。
- **验收**：1)blocked worker时writer/IO close=0；2)shutdown后拒绝late submit；3)TERMINATED事件只在worker实际返回后发布。

### Task 19：建立PreparedWriter类型边界

- **依赖**：Task 18。
- **工作**：在first write/restore/prepare前注入Compaction executor；strategy factory只接收prepared type；construction scope精确记录submission flag。
- **文件（≤4）**：prepared runtime类、bucket runtime factory、factory interface、prepared writer test。
- **验收**：1)生产raw writer不可越过边界；2)五模式编译接入；3)构造期submission计数为0。

### Task 20：实现不落盘RuntimeTableFactory

- **依赖**：Task 14、Task 6。
- **工作**：创建generation/read-scope runtime copy并设置async=false；原表Catalog metadata/options/physical identity不变；所有read/write builder只能使用copy。
- **文件（≤3）**：`PaimonRuntimeTableFactory.java`、factory test、option fixture。
- **验收**：1)原表options零变化；2)所有copy async=false；3)ORC sentinel不会构造AsyncRecordReader/Future。

### Task 21：实现version-locked sequential index bootstrap

- **依赖**：Task 2、3A、3B、6、20。
- **工作**：新增`PaimonSequentialIndexBootstrap`，等价复现LATEST、trimmed PK、bucket filter、一次采样currentTime的TTL、partition+bucket；逐split exact-close reader/batch。
- **文件（≤3）**：sequential adapter、golden test、reader failure fixture。
- **验收**：1)与1.3.2输出multiset一致；2)zero/multi split及readBatch/release/reader.close失败覆盖；3)不构造ParallelExecution。

### Task 22：接入HASH preflight与GlobalIndex staged ownership

- **依赖**：Task 7、17、20、21。
- **工作**：KEY_DYNAMIC/HASH_DYNAMIC生产对`IndexBootstrap#bootstrap`调用count=0；HASH先用async=false runtime table，再在validation copy移除index TTL保持原pollution语义；assigner在open前登记，endBoostrap成功后transfer。
- **文件（≤5）**：`PaimonDynamicBucketPreflight.java`、Key/Hash dynamic strategy、strategy factory、preflight test。
- **验收**：1)withoutIndexTtl语义不变；2)三个assigner故障点返回typed retained；3)IOManager不被assigner close。

### Task 23：实现maintenance adapter

- **依赖**：Task 5、14。
- **工作**：Connector消费opaque lifecycle handle，不反射executor；映射SUCCESS/FAILED_DRAINED/WAITING，保留原始Throwable。
- **文件（≤3）**：maintenance adapter、adapter test、capability integration fixture。
- **验收**：1)WAITING可同operation继续join；2)FAILED_DRAINED只允许独立committer close且阻断IO；3)未知outcome fail-closed。

### Task 24：实现WriteResourceLifecycle固定偏序

- **依赖**：Task 16、18、19、23。
- **工作**：compaction proof → writer → maintenance closeAndDrain → committer → success判定 → IO/spill/lease；writer失败仍尝试maintenance与独立committer；未知部分close不重试。
- **文件（≤3）**：`PaimonWriteResourceLifecycle.java`、lifecycle test、failure-injection fixture。
- **验收**：1)严格事件偏序且exactly-once；2)FAILED_DRAINED/writer/committer失败IO=0；3)WAITING无terminal发布。

### Task 25：Factory staged construction与固定rollback

- **依赖**：Task 17、20、22、24。
- **工作**：每个真实handle创建即登记；使用FACTORY_ROLLBACK proof和固定安全偏序，不做普通逆序close；失败只产生SAFE_ROLLBACK或RETAINED_RESTART_REQUIRED。
- **文件（≤5）**：`PaimonTableWriteContextFactory.java`、construction envelope、creation failure、factory test、rollback fixture。
- **验收**：1)每个注入点无unregistered handle；2)non-termination不继续IO/delete/release；3)safe rollback完整exact-close。

### Task 26：激活Context全生命周期WRITER lease

- **依赖**：Task 25。
- **工作**：Context/generation持有一次physical WRITER lease直到scenario-safe release；write/commit仅operation admission并在表锁后revalidate；close委托统一lifecycle。
- **文件（≤3）**：`PaimonTableWriteContext.java`、context test、context integration test。
- **验收**：1)write/commit不重复physical acquire/release；2)late DML被二次fence拒绝；3)Context无旁路close。

### Task 27：建立deterministic真实spill回归

- **依赖**：Task 26。
- **工作**：Surefire `PaimonCompactionSpillLifecycleTest`，threshold=2且输入生成>=3 sorted readers/runs；在delegate createChannel前latch；参数化STOP/DDL。
- **文件（≤3）**：real-spill test、test seam/fixture、Surefire配置（仅需要时）。
- **验收**：1)阻塞时目录存在、IO close=0、DDL action=0；2)释放后actual TERMINATED且严格偏序；3)无`.channel ENOENT`且>=3 runs/readers有硬断言。

## Connector：Service、Read、DDL与Global Barrier

### Task 28：PaimonService采用协调器与post-lock revalidation

- **依赖**：Task 15、16、26。
- **工作**：所有Context get/create/write/commit入口接入operation admission与表锁后二次校验；Service不再自行拥有重复状态机。
- **文件（≤4）**：`PaimonService.java`、coordinator adoption test、post-lock test、service lifecycle test。
- **验收**：1)已拿ingress但后拿表锁的DML被fence拒绝；2)无重复physical lease；3)锁序测试通过。

### Task 29：拆分fatal write failure与真实STOP编排

- **依赖**：Task 24、28。
- **工作**：fatal write只sticky-fence并记录`failureOrigin=WRITE_PATH`；只有真实STOP建立/加入teardown proof；STOP先完成stop-drain/callback admission再snapshot并round-robin join。
- **文件（≤4）**：`PaimonService.java`、service close test、write failure test、STOP concurrency test。
- **验收**：1)write failure不会启动资源teardown；2)STOP slice timeout保持active WAITING；3)failure/deadline后offset/callback不ack。

### Task 30：实现ReadResourceScope与structured owner

- **依赖**：Task 2、3A、3B、15、16。
- **工作**：public read在首次Catalog访问前登记parent/per-table child；resource owner跟踪structured operation、batch、reader、stream executor；按closeAndDrain→batch→reader顺序关闭。
- **文件（≤5）**：`PaimonReadResourceScope.java`、table borrow类、scope test、borrow test、structured read fixture。
- **验收**：1)消费0/1条也drain；2)STOP/DDL只request/join不double-close；3)close failure retained并阻断global cleanup。

### Task 31：实现DDL exact-lease与read fence编排

- **依赖**：Task 1/G0、16、29、30。
- **工作**：先fence目标read再join；有Context持WRITER，无Context取DDL_ONLY；不等待自身lease；action前deadline产生active deferred；action failure转移到RetainedDdlActionLease并执行既有finally invalidation/guard cleanup。
- **文件（≤5）**：`PaimonService.java`、`RetainedDdlActionLease.java`、DDL test、read-fence test、lease transfer test。
- **验收**：1)blocked read时action=0且表B不受影响；2)timeout只允许内部STOP join、普通DDL不可retry；3)action失败callback=0且lease原子转移。

### Task 32：接入stream/batch/count/query read路径

- **依赖**：Task 20、21、28、30、31。
- **工作**：四类public read在任何Catalog/Table/FileIO访问前注册scope并使用runtime table；所有正常/异常/early-stop路径exact-close；stream force后做第二次positive await。
- **文件（≤5）**：`PaimonService.java`、read integration test、stream termination test、batch/count/query test、failure fixture。
- **验收**：1)provisional failure无borrower泄漏；2)stream未TERMINATED不unregister；3)structured operation未drain时Catalog/FileIO close=0。

### Task 33：实现Service-global borrower barrier

- **依赖**：Task 15、16、29、30、32。
- **工作**：统一检查active/retained generation、read parent/child、RetainedDdlActionLease、structured child outcome和scheduler/stream termination；禁止broad clear/null。
- **文件（≤4）**：coordinator、`PaimonService.java`、global barrier test、retained registry test。
- **验收**：1)任一waiting/retained时global close=0；2)全部CLOSED_SUCCESS才exactly-once global close；3)terminal retained发布CLOSED_WITH_RETAINED_RESOURCES且保留handle。

### Task 34：收口actual Hadoop FileIO并删除production反射

- **依赖**：Task 9、10、14、33。
- **工作**：global barrier后只关闭actual Catalog-owned FileIO/exact raw；删除`closeHadoopFileIOCachedFileSystems`及所有production fallback；区分HDFS/S3A、file、native s3。
- **文件（≤5）**：`PaimonService.java`、capability gate、Hadoop ownership test、service close test、结构扫描test。
- **验收**：1)HDFS/S3A A停B可读写且A raw=1；2)file无raw断言、native s3独立gate；3)reflection/closeAll/fsMap.clear production count=0。

## 回归、性能与发布

### Task 35：五BucketMode与跨层E2E回归

- **依赖**：Task 22、27、31、32、34。
- **工作**：精确覆盖HASH_FIXED、HASH_DYNAMIC、KEY_DYNAMIC、POSTPONE_MODE、BUCKET_UNAWARE；验证row/bucket/TTL、snapshot/commit/offset/callback/stateMap零变化。
- **文件（≤5）**：每种模式一个参数化/现有测试文件，合计不超过5。
- **验收**：1)五模式全部实际执行；2)KEY/HASH dynamic不调用ParallelExecution；3)offset/exactly-once边界无变化。

### Task 36：执行Paimon与Connector完整测试门禁

- **依赖**：Task 3A、3B、4、5、6、7、9、10、11、13、27、35。
- **工作**：先按模块精确测试，再跑Paimon API/Common/Core全量和Connector全模块无filter；保存Surefire XML并程序化断言关键tests>0。
- **文件（≤3）**：CI脚本、report assertion、测试证据索引。
- **验收**：1)所有命令exit 0；2)关键XML tests>0；3)无`*IT`/`-DskipITs`假执行。

### Task 37：执行W1-W4性能与部署清理审计

- **依赖**：Task 36。
- **工作**：执行Spec W1–W4并使用原值判定：W1 throughput≥95%、write P99≤110%、Compaction P95≤120%；W2 P95≤200%且最大集≤300s；W3 throughput≥80%、P99≤125%；W4 termination P99<25s且STOP 30s完成率100%。同时调查tmpfiles/cron/emptyDir/agent/人工脚本。
- **文件（≤4）**：benchmark harness、结果报告、部署审计清单、回滚阈值文件。
- **验收**：1)W1-W4每项给出P50/P95/P99及硬判定；2)超阈值即阻断不豁免；3)外部删除源有owner/action plan或EXTERNAL_OR_UNATTRIBUTED结论。

### Task 38：汇总release evidence并做最终人工审查

- **依赖**：Task 37。
- **工作**：映射FR→class→test→event→outcome；归档patched coordinates/sources/POM/checksum/dependency tree、failure matrix、性能与rollback说明；逐项复核Spec无语义回退。
- **文件（≤3）**：release manifest、traceability matrix、final review report。
- **验收**：1)G0–G4全部通过；2)每个Required有代码和测试证据；3)人工批准前不构建可上线包。

## 全局禁止项

- 不允许普通lazy `Iterator`替代structured handle，不允许read scope省略`closeAndDrain()`。
- 不允许把`FAILED_DRAINED`、Future done/cancelled、parent executor TERMINATED或timeout解释为IO可释放。
- 不允许fatal write冒充STOP proof、DDL等待自身lease、Factory普通逆序close或close failure进程内重试。
- 不允许Connector production反射unwrap/close/clear Hadoop对象，不允许`FileSystem.closeAll*`。
- 不允许把`file://`当Hadoop raw FS，或把native`s3://`测试替代`s3a://`。
- 不允许只发布三个patched JAR而不验证effective-POM闭包。
- 不允许用`*IT`配合`-DskipITs`、命令exit code或fixed sleep冒充测试实际执行/termination proof。
