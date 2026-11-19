# Paimon Plus 同机故障恢复与主键最终一致性

**实现状态（2026-11-19）**　连接器已实现 Paimon 1.3.2 原生准备消息的本机持久化和按表恢复，默认关闭。目标端进程故障已在隔离仓库验证；真实源、引擎及 Mongo 联调仍待验收。实施与测试结果见[任务清单](../../../tasks/local-recovery/todo.md)。

## 1. 业务承诺与验收口径

选定源端位点 P，在源端取得该位置的基准，待目标追到 P 后比对。每个应同步的主键都要存在，映射后的字段值相同，源端已删除的主键应当消失，目标也不能多出键。持续有新写入时，使用暂停在 P 的隔离源或同一时点的基准副本和目标快照，避免将两个不同时间的表直接相减。

本文的验收语义是「允许事件重放，追平后逐主键与源端一致」。同一次源事件可能多次进入 writer，也可能出现在多个 snapshot 中。业务使用普通表查询，接受恢复时重放和短暂读到旧值，不消费 changelog。首次全量正常运行时，在引擎的全量完成回调后按目标表发布 snapshot（多来源与同机重启的例外见第 5 节），目的是减少 snapshot 和小文件。CDC 按时间窗口合并提交；同一轮的各表允许先后可见。承诺覆盖同机进程崩溃、任务停止后重启、本机持久盘仍可读的情形。跨机接管、持久盘丢失和跨表事务同时可见均在本版范围外。

### 节点配置与切换

在 Paimon **目标节点**设置 `localRecoveryDir` 为该节点在持久磁盘上的专用绝对路径，例如 `/data/tapdata/recovery/orders-target`。任务使用稳定的节点 KV、相同主机及原目录。连接测试不会认领目录。设置 `enableAsyncCommit=true` 与正数 `commitIntervalMs`；默认 `cdcCommitTrigger=time`，按最早未发布 CDC 批次的等待时间调度。选择 `timeOrRows` 并设置正数 `batchAccumulationSize` 可允许提前按行数触发。

启用前先排空旧 writer 并逐键对账。目标端要求有稳定映射的主键及完整的更新后镜像，采用 deduplicate，保留删除；支持固定哈希桶、动态哈希桶、跨分区键动态桶。跨分区索引 TTL、删除向量、row tracking、额外顺序字段、非 none 的 changelog producer 等未获准。表属性按物理表实际生效值校验。业务仓库需要排除同表并发 writer、运行中 DDL、外部历史改写与未提交文件清理。源端还须能从已保存的起点完整重放到 P，特别要验证首次全量转 CDC 的锚点。若源允许修改主键，必须提供含原主键和路由字段的 before 镜像，或等价的删除旧键和插入新键事件。只提供新键的源无法让目标辨认应删除的旧键。

每次非空 DML 调用成功前都准备原生文件。全量完成后按表顺序发布，较大的区间会拆分；恢复时可能提前发布已确认接收的全量前缀。清理发生在相应区间的原生快照已核对、本机已确认、节点 KV 进度已核对以后。清单收回后仍保留绑定、表世代和快照指纹。停用或降级时应先用能读该格式的连接器排空，核对源位点和目标，再切换至新的节点基线。磁盘遗失、目录错配或仓库文件丢失需从可核对的源基线重建；删除目录会失去恢复材料。

按批 prepare 会刷出数据文件。在两个固定桶、16 批各 512 行、关闭自动 compaction 的测试中，两种模式的全量均只产生一个 snapshot，开启后物理数据文件从 2 个增至 32 个。吞吐、CDC 延迟和同机恢复时延见[三轮对照](../../../tasks/local-recovery/e2e/CAPACITY.md)。

## 2. 现有协议和故障窗口

| 边界 | 已核实的行为 | 设计后果 |
| --- | --- | --- |
| 单表全量完成 | 目标端处理单表完成事件时可把 `OVER` 纳入待持久化进度。源端恢复可跳过已标记的表。引擎在另一个全量阶段完成事件上遍历目标表、调用 `afterInitialSync`。 | A 收完、B 仍在跑时就要能恢复 A；不能依赖引擎再次发送 A。 |
| 数据交付与心跳 | 常规目标入口先按事件类拆批；全量非并发单独拆出单表完成事件。并发写由分区 worker 处理数据，watermark 等相关 worker 完成后推进。DML 可携带 stage、来源节点和可选的源 offset。 | 按实际投递顺序建立覆盖范围；时间戳、已见最大 offset 和单个目标表的进度都不足以证明全源已处理。 |
| 位点回调 | 启用 `FLUSH_OFFSET_CALLBACK` 时，心跳的 `processControl` 正常返回会结束该路径；异常经 `errorHandle` 后可能继续把原始心跳位点加入进度。回调是 `Consumer`，调用方拿不到 Mongo 写入的持久化凭据。 | 异常不能充当阻止位点前进的机制；回调返回也不能作为删除日志的依据。 |
| Paimon Plus | 全量 `acceptInitial` 累计缓冲行数，CDC 有大小、时间和后台触发器。现有 `pendingCommits` 在进程内；任务状态保存提交身份及下一个编号。停机和部分 DDL 也能触发表 flush。 | 在向 TapData 确认可能不再重发的数据之前，须取得可跨进程恢复的原生提交清单，并持久登记。常规、停机、DDL 使用同一套发布确认。 |

两种典型时序决定实现顺序。

- **全量。** 引擎送完 A 的行并持久化 `A=OVER`，B 仍在扫描，两张表都可能没有新的 Paimon 快照。此时进程退出，源端恢复时可能跳过 A。A 的每批必须在返回成功前已形成受保护的 Paimon 文件，并能从本机清单定位。
- **CDC。** 同一来源的心跳 H 覆盖 A 的第 12 批和 B 的第 9 批。A 已发布到 12，B 只发布到 8，此时必须扣住 H。B 发布到 9 且本地确认同步后才可发回 H。若此时 Mongo 仍保存更早位点，源端可以重放，最终按同一源位点逐键对账。

**全量和 CDC 的交界还有一个源端条件。** 初始扫描读到 A 后，源端可能在扫描结束与第一条 CDC 心跳之间更新或删除 A。串行目标只把带 batchOffset 的 INITIAL 事件送去保存进度，初始心跳可能只有 streamOffset。恢复到 NORMAL/LOG_COLLECTOR 且流位点为空时，源端调用 `initStreamOffsetFromTime(null)`，可能从当前起步。即使 A 的全量清单完好，这段变更也可能从未交付给目标。空表全量后首次插入同样受影响。位点回调只证明请求提交给引擎；源端 flush 可能早于 Mongo 持久化。

只在已有引擎和源配置能够证明原始起点 P0 可恢复、源日志仍覆盖到 P0、目标已接收输入可恢复时，对该组合承诺全量转 CDC 收敛。分别验证空表、首个 DML 前和首个成功 CDC 心跳前崩溃。首次看到带 offset 的 DML、目标本机保存 offset 或让用户填一个时间，均不能强制源端按该值重启。硬删除无法交付的轮询 CDC、跳错跳表、会过滤业务事件的配置排除出未过滤源表的对账承诺。仅全量和有可信起点的纯 CDC 可分别验收。对于节点无法观测的来源能力、保留策略和其他 writer，由部署方针对实际任务/版本记录证据；节点能验证的条件启动即检查，外部事实未建立时强模式保持关闭。详见[实施与准入任务](../../../tasks/local-recovery/todo.md)。

源码基线和入口见第 9 节。

## 3. 优先使用 Paimon 原生文件和提交清单

### 3.1 内核负责什么

| 部件 | 1.3.2 的能力 | 连接器要补的部分 |
| --- | --- | --- |
| `StreamTableWrite.prepareCommit(waitCompaction, prepareId)` | 将本次写入形成数据文件及适用的索引、压实等变更，返回 `List<CommitMessage>`。prepare 可以早于对业务表发布 snapshot。 | 通过本连接器已有桶策略执行 prepare。在返回全量批次成功前，确保原生文件可恢复、完整消息已在本机持久登记。 |
| `CommitMessageSerializer` 和 `StreamTableCommit` | 使用内核版本化编解码保存和还原原始消息，支持按身份和编号提交、过滤重试。Flink sink 把这类待提交信息放进其 checkpoint 状态。 | TapData 现有接口没有把这份清单纳入同一 Flink checkpoint。连接器持久记录任务/表/来源、清单顺序和发布确认，并用故障证据决定何时回调。 |
| `TableWriteImpl.checkpoint()/restore()` 和 spill | 1.3.2 的 writer checkpoint 会对各活跃桶调用底层 `prepareCommit(false)`，返回包括文件、增量、序列号及维护器对象的状态。写缓冲 spill 供执行中的 writer 使用。 | 二者均不自动为 TapData 保存每个已确认批次。每批调用 writer checkpoint 同样可能刷出小文件。动态桶还有独立 assigner 状态。 |

数据与索引保存在目标表原本使用的 warehouse。本机 `localRecoveryDir` 只保存提交消息和恢复所需的元数据，不在本机另存一份业务行，也不增加远端恢复服务。[原生 prepare][prepare] · [消息编解码][ser] · [writer 状态][writer-checkpoint] · [PK 写缓冲刷新][flush]

### 3.2 从收到一批到全量发布

1. **绑定身份。** 默认不开启。启用时使用目标节点专属的本机持久目录，持有固定文件独占锁，在引擎按 node ID 提供的 `getStateMap()` 中核对目录 UUID。首次绑定保留可恢复的创建意图，已绑定目录或 KV 丢失、冲突时停止。绑定宿主身份和规范路径，防止两个克隆目录持有相同 UUID 和不同的文件锁。若部署允许两节点使用不同目录写同一物理表，节点锁无法实现表级互斥。测试连接不进入恢复。表状态以 catalog、仓库、分支、表世代、writer 布局与原 `commitUser` 隔离。writer 打开、首次产生未发布文件之前就持久记录基线（无快照也显式记录）、最近确认快照内容指纹及世代；封存时另记当时 head。恢复比较两者之间的世系。原生 rollback 可重用 snapshot ID，截表可保留 UUID/schema，单凭这些数字不能识别历史。启动先以只读方式核对目录、身份和未决 K，再允许旧 `PaimonCommitStateStore.bind` 调高 KV 编号。[表身份][table-uuid]
2. **生成原生消息。** 在已有表锁内，沿用原 `writeRecord → PaimonService → writerStrategy.write` 的类型转换、更新/删除展开与分桶逻辑。全量每个可独立返回成功的批次走 `writerStrategy.prepareCommit(prepareId)`，由相同 writer 保持序列与索引连续。prepare ID 递增，和引擎 offset 分开。单批写到一半、prepare 失败，冻结旧 writer，不确认该批；重启先处理之前完整登记的批。由源重送未确认数据的前提是该入口的源进度与错误策略已通过验收。PDK 对同一表调用可以在进程内重试，同一引擎多表组也可能只确认 A。对已改变 writer 的失败锁存到目标实例，后续同批重试不能复用该 writer 或仅在 `errorMap` 中报告成功。
3. **本机只保存清单。** 使用 `CommitMessageSerializer` 的版本与原始消息字节，连同格式版本、表内连续批号、阶段、来源依赖和 prepare ID 存入表目录。文件先写同目录临时名、校验长度和校验和、force、原子 rename、同步父目录，再原子推进连续的已登记批号 `prepared`。首次表登记及目录创建也要同步。至此才返回全量批次成功。崩溃后不采用有效尾以外的文件。每批独立或按不可变段追加都可以，禁止每批重写全部历史清单。有界合并多个并发批次时，所有等待者必须在自身切点持久后才能返回；串行调用不能等待尚未送来的下一批。
4. **封闭表区间。** 全量到 `afterInitialSync(table)`，CDC 到窗口或恢复切点时，按表锁固定连续区间。用单个提交编号可以控制发布批次，但具体 snapshot 数仍由内核产生的业务/压实变更决定。把原生消息的有序引用及摘要、已打开 writer 的原始基线与独立的封存 head、schema、同一 `commitUser`、各子组区间与 K 原子保存为不可变的发布计划。K 不小于本区间的 prepare ID，并严格大于该身份以往的发布编号；下一窗口也从更高编号起步。所有重试复用同一个 K 和同一组原始字节。同一表同一时刻只准一个未决发布。按序解码消息。在第 4 节的前置校验通过后，每个封存子组首次发布和重试均调用 `filterAndCommit(singletonMap(K, messages))`。返回的数量只用于诊断。按第 4 节确认后再向引擎回调。一个最终编号仍可能产生多种 snapshot。
5. **限制单次合并。** 基本验证覆盖同一 writer、同表、同 schema、固定桶。禁止把多个独立 writer 的序列混合或对消息按文件名去重。跨批 compaction 输入输出、动态索引、删除向量与大表清单占用均须按组合验证。启动恢复先发布旧进程完整登记的连续前缀，再从最新快照建立新 writer。若一个区间超过提交器能处理的消息数或字节数，按已验证的 prepare 边界分组，各组使用独立、递增且持久化的 K，在全量末尾依次发布；第一次调用原生提交器前固定全部范围、摘要、次序和 K。组间按前缀确认，发生半组成功、阈值变化、KV 超前或重启时不重新编号、重排或重新切组。采用有界的清单大小、元素数、工作堆预算及整数溢出检查；单条消息超过预算时在 ACK 前明确拒绝。组间依赖未验证时，停止并报告；不得丢弃或重排消息来满足上限。

**纯 CDC 的延迟 prepare 属于后续优化。** 当且仅当真实引擎验证该任务所有可持久化的 CDC 位点都受安全回调约束、同键有序且源日志可从旧位点重送时，纯 CDC 批次可留在 writer 中，等定时窗口再执行上述 prepare、登记和发布。窗口前崩溃由旧位点重读；窗口已 prepare 的清单由启动恢复先处理。首版在这项验证通过前，对每次确认采用与全量相同的 prepare 规则。混有未封闭全量的区间同样采用保守规则。无论哪种方式，CDC 的常规目标快照发布都按所选窗口执行。

### 3.3 代码落点

| 入口 | 实现职责 |
| --- | --- |
| `PaimonConnector.onStart/writeRecord/processControl`、节点配置 | 区分任务与连接测试。任务启动先锁定、恢复所有表，再允许新输入；只为已满足跨表依赖的心跳回调。 |
| `PaimonService.writeRecordsWithStreamWriteInternal` 和现有桶策略 | 保留原始 DML 映射和分桶。全量返回成功前执行 prepare 并登记，避免引入第二条行转换、编码和重放路径。 |
| `PaimonTableWriteContext`、`PaimonCommitStateStore` | 拆开当前连续执行的 prepare、commit、confirm。新增本机清单保存与读取，恢复时直接把原消息交给提交器。任务 KV 保留现有提交身份、编号核对语义。 |
| `PaimonMicroBatchCoordinator`、后台调度器、DDL、stop | 复用表锁、来源 reservation 和失败锁存。表的 `published` 是已持久确认的连续发布前缀。先确认、再解除相应的心跳依赖。所有能提交或释放 writer 的入口遵守同一顺序。 |

## 4. 发布、恢复与清理

**发布前要同时验证三件事。**

1. **文件可用。** 对要求存在的数据、sidecar 与索引通过原生路径解析器校验，处理不同增删种类；有元数据长度的对象同时核对长度。将内容完整性交给具备相应保障的不可变 warehouse，后续测试若模拟同长度静默损坏，应明确外部校验和能力。Paimon 的原生恢复检查针对部分正向引用调用 `exists`，不会对每个历史删除输入做相同检查，也不检查数据文件内容。普通 `commit` 首发不走该文件存在性检查，可能发布不可读的 snapshot。任何一次 stat 都不能替代覆盖提交过程的文件保留保障。[原生文件检查][files]
2. **历史仍属同一世代。** 从 writer 打开时的历史锚点验证到当前 head，检查最近 published 的指纹（表、branch、snapshot 内容及所引用的 manifest 身份）。历史缺失、rollback 分叉、同名重建、无法解释的 overwrite 或 maintenance 时保留现场。首次发布前还须保证 writer/仓库使用期间排除可使基线失效的外部操作；同机目录锁、UUID、snapshot ID 和发布前的一次检查都挡不住另一作业在检查后提交。动态桶须排除同物理表/分支上的并发分配 writer，即使两个任务的源主键不相交。
3. **顺序和效果可核对。** 内容相同的消息组交换先后，也可能改变下一 writer 的路由。按表内批号、prepare 内消息次序和完整原生字节存储、恢复。通过原生 manifest/索引读取接口从前置基线和本次 delta 得到预期效果。在 HASH_DYNAMIC 下，原生维护器保存累计 hash 集合，同一分区、桶、索引类型的最后一个有效 ADD 会替代以前的映像。验证最终有效映像与新 writer 路由，不要求每个原始 ADD 都留在最新 manifest。数据文件、DV 与 changelog 按各自内核语义处理，不能统一当成文件名集合。[累计 hash 索引][hashwrite] · [原生索引合并][idxfold]

内核会将一次清单中的业务和 compaction 变更分别组织。恢复以 writer 打开时记录的表世代、分支、快照基线和封存时的 head、`commitUser` 和 K 查找该表所有相关物理 snapshot，按提交种类及数据、索引 manifest 效果核对。仅凭 `filterAndCommit` 返回值或单次编号查找无法确认整个区间。第 8 节给出同编号部分发布的实测结果。[消息汇总][collect] · [分步提交][sub-snaps] · [编号过滤][filter] · [单编号查找][snap-lookup]

| 同机启动时的状态 | 恢复动作 |
| --- | --- |
| 表尾有临时文件、有效尾之外的清单，且没有发布意图 | 该批尚未确认。只恢复连续有效尾，让引擎重发未确认数据。仓库里的孤立文件交给其生命周期处理。 |
| 已登记全量或 CDC 清单，尚未生成发布意图 | 当作前一进程已准备的前缀，先持久建立发布意图，再用原始消息提交。即使无后续源事件也执行。 |
| 已有发布意图，连续保留的历史证明尚未发布，且原文件齐全 | 先校验引用长度/先决条件和当前世系，再使用原身份、原编号、原消息 `filterAndCommit`，核对可以归于该区间的业务/索引变化。对被原生设计折叠掉的中间索引按当时的有效映像核对，不能拿当前仓库目录列表代替。清单仅含元数据；引用文件若永久缺失，连接器无法凭清单还原行。须停止并从可核对的源基线补齐。 |
| 全部预期效果已发布，本地确认尚未落盘 | 补写、同步本地 `published`，再核对任务 KV，清理已覆盖清单。无需第二次写入业务文件。 |
| 同编号只有 `APPEND/OVERWRITE`，预期的 `COMPACT` 缺失 | 逐项检查业务与路由索引是否全部可见。仅对实验证明剩余部分完全属于可重建维护工作的模式，持久记录维护待重建并从最新快照重新规划。否则保留完整清单、停止回调与清理。 |
| 空表、非空输入得到空消息或已知历史断档 | 空表沿用完成提交语义，记录零行完成标志。非空输入只有在能证明逻辑无效果时才可确认。历史缺口、文件缺失、世代或身份冲突均保留现场，并阻止依赖该表的心跳。 |

**清理分两处。** 清单 GC 后仍保存最近可信 published 指纹和世代。表已发布前缀先在本机同步、任务 KV 完成核对，之后只删除本机已被完整覆盖的清单。本机清单的保留不等于 warehouse 对文件加锁：原生 orphan cleaner 根据仓库内可识别的引用判断，本机清单对其不可见。部署前必须验证从最早 prepare 到最迟恢复完成期间，孤儿清理、对象生命周期、writer 关闭和并行维护都不会删除这些文件；保留期从所有引用中最早的相关文件 mtime 起算（包括已存在而被引用的文件），覆盖全量剩余时间、最大停机、恢复与发布期间的清理竞态。若允许无限期停机且清理有有限 TTL，需在部署层实际排除该类清理或明确恢复时间边界。无法提供该保留条件的部署不能采用仅清单方案。已提交历史也须留足核对窗口。[原生文件检查][files] · [孤儿清理][orphan]

## 5. 阶段、心跳与停机

### 全量何时发布

- **正常全量。** 普通数据批只 prepare 并登记，不发布业务 snapshot。收到 `afterInitialSync(table)` 后，将当时的表世代和切点持久化，再发布该连续区间。已经完成同一切点的回调可重入。引擎对每一个来源的阶段完成事件都可能遍历全部目标表，回调只给 `TapTable`。因此该信号只能封闭当前表前缀，不能宣告所有来源都已完成。后续合法全量行可开启下一段，同一个切点不重复发布。不要以“已收过一次完成回调”丢弃后来数据。
- **同机重启的明确例外。** 对已封闭区间直接续交。对于尚未收到阶段信号、但在旧进程已留下有效清单的全量表（CDC 已登记未发布的尾部也必须主动扫描），启动时将恢复切点固定为旧 `prepared`，持久化并发布这个前缀一次，表的全量阶段仍是开放的。后台静默、该表已被引擎跳过或接下来只收到 CDC，也能恢复这些行。第二次重启用已发布前缀跳过同一切点。继续到达的全量仍在正常完成回调时提交。这个选择可能在异常恢复时让业务较早看到部分全量，并额外产生 snapshot；正常首次全量继续末尾提交，符合已确认的减少 snapshot/小文件目标。正常停止不主动发布开放的全量，重新启动时才执行上述恢复规则。
- **源端静态证据。** 普通 source 的 `need2InitialSync` 在所有表 `OVER` 且阶段仍为 INITIAL 时仍可为真，正常运行的 `finally` 会发送阶段完成。若进度已为 CDC，这一分支可以跳过；内部 `TapdataStartingCdcEvent` 也没有对应的目标连接器阶段回调。因此上述恢复前缀必须在目标启动时主动处理，不以空闲超时或某条后续 DML 当作唯一触发条件。上线前仍要验证真实任务确实启动了目标实例。没有启动过目标进程的任务无法自行运行恢复程序。

### CDC 何时推进位点

新模式的节点配置 `cdcCommitTrigger`（提议新增）默认 `time`：只在有待发布数据时从本地接收时间起计时，复用 `commitIntervalMs`，默认 30000。显式选 `timeOrRows` 时才把正的 `batchAccumulationSize` 作为提前提交阈值。新模式要求 `commitIntervalMs > 0` 且 `enableAsyncCommit=true`；配置冲突在目标启动时给出字段原因。既有未启用模式保留原先 `batchAccumulationSize=0` 等行为。单调时钟计算运行中的相对延迟，源时间和心跳时间不参与调度；旧进程留有 CDC 积压时启动恢复直接处理。空窗口不制造业务快照。每表串行，每个窗口取固定 cutoff，窗口后的新输入进入下一轮。慢提交表现为积压。如果窗口内有缓冲行，选取切点和 prepare 均在同一表锁内完成；如果已经逐批 prepare，只封闭该切点之前登记的清单。在同表 INITIAL 和 CDC 有交叠时按连续批号处理，不能从中间跳过开放全量行去提交后面的 CDC。配置要求所有来源全量均结束才可首次可见时，当前回调只能为单来源目标证明这一条件；多来源共享目标遵循前述按表前缀语义。

以 `nodeIds[0]` 定位来源。按现有 coordinator 的 reservation 记录收到心跳时该来源在各目标表的所需已接受批号。在纯 CDC 窗口模式下，心跳依赖按已接受的批号登记，包括仍在 writer 中未 prepare 的批次，不能用 `prepared` 代替。每表 `published` 覆盖其对应批号后，锁外通过现有映射构造 `TapCallbackOffset`，把原始流位点、来源、阶段和时间交回引擎。一次最多执行一个该来源的回调；被更新心跳替代的 reservation、关闭前的旧世代和回调在途状态沿用现有 lifecycle 保护。恢复可弃置旧进程的回调对象，先提交已登记的积压表区间、重建来源依赖，再接收新心跳。不得把来源 A 的位点用于来源 B；不得用某一行的位点独自代表整条通道。首次全量也要从每行元数据保存可得的来源，现有 `validateDmlMetadata` 会跳过 INITIAL 的来源收集，此处需要补齐。对于允许来源缺失的全量行，额外登记为所有来源的公共依赖，直至相应表前缀发布。CDC 数据缺少必需的来源或阶段时，在确认该批前报告错误。

**异常边界。** 当前引擎正常串行入口按事件实际 Java 类拆批，正常并发入口以 watermark 等待前序 worker。以这两条实际入口构造故障测试，不用手工混排内部方法的参数来代替真实入口。启用恢复模式时，`processControl` 在任何可能不安全的心跳分支上不启动 offset 回调。对缺 stage/来源、提交失败、服务空、回调失败等分别区分可忽略的非 CDC 控制和真正故障。对故障先锁存、禁止后续成功准入，在锁外调用任务提供的 `Log.error(message, throwable)` 报告一次，再让心跳控制调用正常返回，避免引擎的异常回落。报告函数自身失败时也须保住禁止回调状态，并通过独立状态/下一数据入口暴露故障。当前引擎使用 `StopTaskOnErrorLog` 转入 `errorHandle`；涉及只剩心跳的任务，必须验证告警、停止和 Mongo 落点。DML、全量完成和停止函数仍传播真正的异常；强停时引擎可能先保存 Mongo 进度再进入连接器 close，任何交付保证都不能依赖 onStop 最后补刷。源/引擎显式配置跳错、跳表或过滤有效业务 DML 的任务不可按照未过滤源表宣称一致。

### DDL 与资源释放

已知 schema 变更在原表锁内先处理旧区间，再切换布局；外部字段重排按原 schema ID 和字段 ID 校验映射，映射无法证明则隔离。将建表入口与 `dropTable/clearTable → runTableDdl` 各自列入实现，不能假定它们都通过同一方法。对可证明幂等的建表，持久记录期望 schema/表身份并核对实际结果。删除、截表等破坏性 DDL 在执行前持久记录操作身份、前置状态、世代和期望后置状态，确认后才开放下一世代，保留已完成 DDL 的重放判定。尤其 `truncate` 后表的位置、UUID 可不变，表为空也不足以证明哪次操作生效。只有当现有 PDK 能提供可复认的事件身份/顺序且 catalog 能核对结果时，才允许自动恢复该种 DDL。否则在操作前阻止其进入自动恢复流程并报告需受控排空、对账和开启新世代；已执行但结果不明的旧意图保持隔离。不能猜测并再次 truncate 已经有新行的表。

关闭先停入口、等在途 prepare 和清单登记完成，再处理已封闭区间。对尚未封闭的全量 writer 跳过 stop 的业务 drain 并显式调用现有 `closeForStop(false, …)` 路径；对已保存提交清单的 writer，先证明 close 不会删除该清单引用的文件，各桶模式用真实仓库验证。不具备这个条件时保留/隔离资源与意图。其余路径还要覆盖 final prepare/commit：任何含业务效果的清单通过相同持久协议；仅维护的清单不能越过未决业务编号。超时后沿用现有 stop 冻结逻辑，迟到线程不能推进 `published` 或复用新 owner。

## 6. 适用表及资源成本

以下判断以业务实际使用的普通批查询为准。读端配置若额外过滤刚发布的层级，也须用相同配置对账。

| 主键表模式 | 实现与准入判定 |
| --- | --- |
| `HASH_FIXED` | 按原分区与桶键路由。跨分区更新需可还原旧分区和新分区，测试两行转换为「旧位置删除、新位置写入」，两次恢复后仅新位置可读。 |
| `HASH_DYNAMIC` | 用原生清单先补交数据与 hash 索引，再基于已确认快照恢复 assigner 并路由新输入。测试在分配新桶、提交其索引、源端重放三个时点，同一主键不会落到两个可见位置。 |
| `KEY_DYNAMIC` | 启动先完成旧清单的补交，再用最新已确认快照重建跨分区全局索引，验证同键搬迁与删除。首版拒绝有限 `cross-partition-upsert.index-ttl`；长期没有更新的行今天仍可能被搬迁。1.3.2 对 cross-partition update 表直接禁止 `sequence.field`。[索引重建][idxboot] · [表约束][schema-seq] |
| `POSTPONE_MODE`（`bucket=-2`） | Paimon 的默认 BatchScan 使用 `onlyReadRealBuckets()`。只发布 postpone 文件后，普通查询仍可为空。按本版业务口径，**首版在写入前拒绝该模式**。要在连接器内支持，需另行实现或接入将待分桶文件改写到实际桶的 compaction，跟踪本批输入文件已全部替换，按普通读端验证后才能确认、回调及 GC。仅能流读出待分桶文件不足以通过。 [批读过滤][postpone-scan] · [1.3.2 分桶说明][bucket-guide] |

- 承诺面向有稳定主键、上下游键映射一致且重放后能收敛的表。验证现有 `PaimonWriteSemanticContractResolver` 及每种实际支持的主键桶模式，包括固定、动态哈希、跨分区键动态及延迟分桶各自的路由、删除、改键和索引恢复。原有配置校验继续生效。无主键追加、累加聚合、忽略删除或键映射有冲突的情形不能套用同一承诺。首版建议以 `merge-engine=deduplicate`、支持删除、同键输入有确定顺序的组合为准入条件。在内核允许 `sequence.field` 的其他组合中还须证明更新和删除的序列值能重现源端顺序，包括序列相等的情形。对跨源同键或分区并发导致同键失序的任务，若无法提供可重放的顺序，则排除出承诺范围。不能按主键去重输入事件，同一主键还可能有后续合法更新。
- 目标以本任务为恢复单元。不同任务无序写同一个键、源端日志失去可重放区间、源端全量游标自身可能跳行等问题，无法由目标端清单修正。首次启用基线需要确认合成键没有已知碰撞、字段布局与 writer 世代一致；已有脏数据首先单独对账。
- **容量与小文件。** 本机空间主要随未发布的批数、分区/桶数、文件元数据与索引清单增长。warehouse 要容纳全部未发布的真实数据文件，以及 compaction 过程中同时存在的输入和输出。spill 单独计费。prepare 会刷出文件，推迟 snapshot 不能保证减少小文件。记录每批触达的桶数、数据文件数、文件大小分位数、清单字节、prepare 时延和末尾合并的内存峰值。若顺序小批导致文件成本无法接受，先试已有引擎可投递的批大小，或合并已经并发到达且共同等待确认的批次。仍未达标时才对比复用成熟本地事务存储暂存行的方案，另行决定是否采用；本版不开发自定义行日志。
- 观测每表已接受、已 prepare、已发布的连续批号、未发布文件数和字节、最老清单年龄、剩余文件保留时间、提交编号与耗时、最老未确认的来源心跳、恢复耗时和磁盘余量。日志不输出行值或完整源位点。

## 7. 故障验收

使用隔离的同版本 TapData 引擎、真实 Mongo 与真实 Paimon warehouse，比较无故障对照；mock 测试补齐精确断点。每次先确认断点真的命中，再在同机启动**新进程**恢复。通过标准为第 1 节的逐键比较，同时验证 Mongo 位点、各表 snapshot、本机清单前缀和清理次序。单看行数相等会漏掉串键和更新错行。

| 断点或输入 | 验收结果 |
| --- | --- |
| 全量 A 已在 Mongo 标记 `OVER`，B 尚未完成，直接终止进程 | 启动主动发现并发布旧进程的 A 前缀，源端不重发 A 也能恢复；第二次重启不重复发布同一切点。分别验证所有表都已 `OVER`、恢复后完全静默、B 仍有后续全量，以及空表。 |
| writer 写到一半、prepare 已产生文件但本机清单未同步、清单生效后 ACK 前分别中断 | 仅采用已持久登记的清单；引擎能重发未确认部分，重复投递后逐键正确。另测纯 CDC 缓冲尚未 prepare 时的真实 Mongo 位点及重发范围。 |
| 生成提交清单前后、目标已提交响应丢失、本地发布确认前后、GC 中断 | 原清单恢复正确；重复恢复两次结果相同。确认目标发布前没有回收本机清单。 |
| 同一窗口 A 已发布、B 待发布，同时发心跳 | 心跳不越过 B；重启后 A 不重复发布原清单，B 完成后位点可推进。 |
| 同编号的 APPEND/OVERWRITE 已可见，在 COMPACT 前终止；另一次在 COMPACT 后、确认前终止 | 按预期分项核对业务数据及索引。已验证的纯物理维护可重建，不确定时隔离。两种情况均不能只凭 K 或返回零清理消息；用跨分区删除验证。 |
| 来源 A、B 共用目标，A 先发完成回调，B 随后仍有全量行 | 回调只封闭当前前缀，B 后续的插入、更新及应保留的来源依赖均存在。不要求跨来源同键乱序能自动定序。 |
| truncate 后、写入新世代行前后各中断，再从旧 Mongo 位点重放 | 有稳定 DDL 身份时只作用一次。无法证明身份或结果的路径，在破坏性动作前拒绝或发现未决意图后隔离，新行不被旧事件删除。 |
| **Paimon 已提交、Mongo 尚未存位点** | 从旧位点故意重复投递 insert/update/delete，含“改键后删除”；最后键集合与值完全一致，删除行没有复活。 |
| 慢表并发、混合来源、仅心跳、后台提交失败、心跳回调抛错、正常停止的最后一次存位点 | 原引擎串行拆批及并发 watermark 两条路径均无提前位点；Mongo 不被不安全心跳推进；仅心跳也能发现故障。 |
| 磁盘满、清单出错、文件长度异常或原子替换中断、锁冲突、同机升级、运行中 DDL、表名复用、快照过期或未提交文件被清理 | 不误认新世代，不删待恢复记录，不把不确定提交当成功；可恢复故障解除后能继续。 |
| `HASH_FIXED`、`HASH_DYNAMIC`、`KEY_DYNAMIC`、`POSTPONE_MODE` 分别跑多表全量与持续 CDC。按支持的能力触发实际 spill 和 compaction，记录文件或日志证据后重复断点 | 获准模式的逐键结果、动态索引与分区正确。移除旧 spill 后仍可从持久消息及 warehouse 中的原文件恢复。首版对 `POSTPONE_MODE` 断言写入前即拒绝，另用原生 writer 证明默认批读的可见性差异。 |
| 与相同批大小、数据量和硬件的现网基线比较 | 记录全量耗时、业务 snapshot 数、数据文件数量及大小分布、本机清单/warehouse/提交堆内存峰值、CDC 端到端延迟和恢复耗时。验收性能门槛根据基线实测确定。 |

## 8. 实现顺序、灰度与验证状态

[实施计划](../../../tasks/local-recovery/plan.md) · [逐项任务及检查点](../../../tasks/local-recovery/todo.md)。下表为设计阶段的实施顺序，代码与测试的完成情况见任务清单。隔离引擎和真实 Mongo 的联合验收仍待完成。

| 顺序 | 改动 | 通过条件 |
| --- | --- | --- |
| 1 · 原生路径 | 拆开桶策略 prepare 与提交，保存版本化 `CommitMessage`，实现目录绑定、有效尾和启动扫描。 | 串行批次不死锁，未登记清单绝不返回全量成功。新 JVM 可直接读取此前消息，关闭或丢失旧 spill 不影响可恢复性。 |
| 2 · 按表发布与恢复 | 持久化固定区间和最终编号 K。复用现有提交器，统一全量、CDC、DDL、stop 和后台的确认。 | 验证多个 prepare 汇入一轮、两次重启、业务与压实中途分离、索引与删除向量、文件被清理时显式停机。压实跨多个切点时检查所有输入/输出依赖。 |
| 3 · 引擎行为 | 接入来源心跳依赖、计时与错误报告。在真实 Mongo 上逐项验证纯 CDC 延迟 prepare 的适用条件。 | 全量已 `OVER`、恢复后静默、失败心跳与并发 watermark 分别验证。未证明能可靠重放的 CDC 配置继续在 ACK 前 prepare。 |
| 4 · 容量与上线 | 对目标桶模式、单表多分区、文件数量与全量时长做压力对比，包含孤儿清理策略和大批清单构造的内存峰值。 | 用生产形态记录 snapshot 数与物理文件大小分布，分别判定。存量任务从已排空且对账的基线切入。回滚须先排空所有未发布清单，或由兼容版本完成恢复。 |

**本次新增的固定桶跨 JVM 实验。** 本地 Paimon 1.3.2，单表、单固定桶、稳定主键及同一 writer。三个输入批依次插入键 1/2、更新 1 并插入 3、删除 2 并再次更新 1。每批 `prepareCommit(true, 1/2/3)`，用 `CommitMessageSerializer` 分别存盘。此时默认批读为空、无 snapshot，直接 `Runtime.halt(42)`，跳过 writer 与 catalog 的 close。新 JVM 用同一 `commitUser` 顺序还原三组消息并一次 `filterAndCommit(Map.of(10L, allMessages))`；第三个 JVM 重试同一封存输入。

| 固定桶设置 | 第一次恢复 | 第三 JVM 重试 | 仓库实测 |
| --- | --- | --- | --- |
| `write-only=true` | 默认批读得到 `{1=30, 3=300}`，键 2 不存在 | 返回 0，快照不变 | 一个 APPEND snapshot，3 个物理数据文件 |
| `write-only=false`，压实触发阈值 2 | 同样的主键和值 | 返回 0，快照不变 | 同编号 APPEND、COMPACT 两个 snapshot，3 个压实输入、1 个输出共 4 个物理数据文件 |

另外将探针改为连接器常规路径采用的 `prepareCommit(false, id)`，分别开启和关闭 writer compaction，再各运行一次三 JVM 实验。两组都在未发布任何快照时退出，恢复后普通读结果相同，再次恢复均无新增 snapshot。本次开启 compaction 的异步组只捕获到 APPEND；后台压实是否在 prepare 前完成取决于时序，不能把这一结果当成固定的 snapshot 数。

该实验验证了固定桶可用原生文件和清单从进程强制退出中恢复。它使用本地 warehouse，并在发送全部三批后退出。动态桶、半批、文件清理竞态、断电语义、生产批大小和 TapData/Mongo 的 ACK 时序仍按第 7 节验收。

**上一轮基线验证。** 在连接器基线、JDK 17 上离线运行 `PaimonTableWriteContextTest`（16）、`PaimonCommitStateStoreTest`（14）、`PaimonWriteSemanticContractResolverTest`（64）、`PaimonMicroBatchCommitTest`（6）、`PaimonServiceInitialSyncPendingTest`（8）。共 108 项，失败、错误、跳过均为 0。这些测试覆盖现有提交身份、写入约束和阶段回调行为。另运行 `PaimonCompactionSpillLifecycleIntegrationTest`、`PaimonWriteBufferSpillIntegrationTest`、`PaimonFinalPrepareBucketModeIntegrationTest` 各 6 项。两组合计 126 项，全部通过。后者包含已确认写入的 final prepare，以及延迟桶的批读与流读差异。

**原生部分提交实验。** 在临时本地 warehouse 建两张相同的固定桶主键表，设 `bucket=1`、`num-sorted-run.compaction-trigger=2`。对同一键先写 10 并提交编号 1，再写 20，调用 `prepareCommit(true, 2)`。两表都得到 1 个新增数据文件、2 个 compact 输入和 1 个 compact 输出。对照表完整提交编号 2；实验表只提交原始消息的 `newFilesIncrement()`，将 `compactIncrement` 置空，以构造两个快照之间的持久状态。关闭 writer/committer，重新取表，用同一 `commitUser` 对**原始完整清单**调用 `filterAndCommit`。JDK 17.0.20 / `paimon-core-1.3.2.jar` 实测如下。

| 状态 | 编号 2 重试前的 snapshot | 重试返回值 | 重试后的 snapshot |
| --- | --- | --- | --- |
| 对照表，完整提交 | #2 APPEND、#3 COMPACT | 0 | #2 APPEND、#3 COMPACT |
| 实验表，仅业务部分已提交 | #2 APPEND | 0 | 仍只有 #2 APPEND |

实验覆盖固定桶和重建 committer，采用提交消息投影模拟中间状态。它验证了编号和返回值无法区分两种结果。在实际 JVM 中命中两次发布之间的断点、验证 TapData/Mongo 的故障时序，以及各桶带 spill 的进程恢复，仍须按第 7 节验收。

### 本轮对抗性审查记录

下列结果区分原生仓库实测、真实引擎方法配模拟协作者和待做的端到端验收。每个原生负例都使用独立临时仓库；不会触碰业务任务。复核依据为第 9 节固定的提交。

| 反例或对照 | 实际观察 | 修改及回归位置 |
| --- | --- | --- |
| 三批 prepare 后强退、无 snapshot，将三个文件 mtime 改到两天前，执行默认 `LocalOrphanFilesClean.clean()` | 三个文件全被清掉，本机消息仍完整。独立对照中，普通 `commit` 成功发布 APPEND、随后读取抛 FileNotFoundException；`filterAndCommit` 拒绝缺文件。另把原长 1174 字节的数据文件截为零，原生过滤提交返回 1，批读抛 EOFException。 | §4 在原生调用前查必要文件长度，首发走带检查接口，部署约束覆盖并行清理。T02/T03/T08/T15。 |
| 已有 key 1 的固定桶表 prepare key 2，外部 truncate 后再提交旧消息 | 截表后普通读为空，UUID 与 schema ID 均未变；原生过滤提交仍处理该组并使 key 2 再次可读。24 批含 compaction 的变体先发表 APPEND，随后删除依赖冲突，重试返回 0。 | §3–4 从 writer 起始基线核查，要求有实际排他或受控的外部维护。T02/T04/T08/T14。 |
| 从 snapshot 2 回滚到 1，再由另一 writer 提交 | 新 snapshot 又是 2，同 UUID/schema，主键集已从 {1,2} 变为 {1,3}。即使本机没有 pending 也会发生。 | cleanup 后保留可信指纹。启动也验证已确认表世系，无法证明则重新建立基线。T04/T09/T14。 |
| HASH_DYNAMIC 同一份消息按正序与反序合入一个 K | 两组首次普通读一致，提交均成功。新 writer 先填桶再更新/删除后，反序组同时读到键 4 的值 40 和 44，正序组只有 44。另一个正序 24 批用例覆盖 8 个新桶；恢复后新 assigner 对 24 个新增键给出原桶。 | 有序消息摘要、按桶验证最终有效索引，加入后继写入。T07/T08/T10。 |
| KEY_DYNAMIC 同一组三批，在新 JVM 重建 writer 后迁移键 4 和 5 | 无 TTL、DV 关闭的正例没有残留旧分区；1 ms 索引 TTL 的反例中两个键都留在旧、新分区。跨分区表另配 `sequence.field` 时，1.3.2 建表直接拒绝。 | §6 将首版可验证组合与拒绝配置区分。启用 DV 但没有产生非空向量的用例不构成该能力的证明。T03/T11。 |
| 只带原始 streamOffset 的 INITIAL 心跳，以及恢复时缺少已存的流位点 | 隔离 JDK 17 调用真实串行目标方法，INITIAL 没有转发至进度保存，换为 CDC 则转发一次；在普通源路径传入空位点，确实转入 `initStreamOffsetFromTime(null)`。该实验使用模拟协作者，未运行真实 Mongo。 | §2 明确全量转 CDC 的源端前提。T01/T12/T15 需核对真实任务和 Mongo。 |

本轮另在基线执行 `PaimonCommitStateStoreTest`、`PaimonMicroBatchCoordinatorTest`、`PaimonOffsetBarrierCoordinatorTest`、`PaimonFinalPrepareBucketModeIntegrationTest` 和 `PaimonServiceInitialSyncPendingTest`，44 项通过，无失败、错误或跳过。与上一轮的测试类有交集。这些是设计阶段的基线结果。本次实现的跨进程故障测试与容量记录见任务清单。真实 Mongo 闭环、主机断电、仓库并行维护与生产容量仍待验收。

## 9. 源码定位

| 版本 | 本地源码与复核点 |
| --- | --- |
| Paimon Plus `develop@c21910aed005` | [写入入口](../../main/java/io/tapdata/connector/paimon/PaimonConnector.java)、[正常写入、全量完成、心跳和 stop](../../main/java/io/tapdata/connector/paimon/service/PaimonService.java)、[表提交与进程内 pending](../../main/java/io/tapdata/connector/paimon/write/PaimonTableWriteContext.java)、[提交身份存储](../../main/java/io/tapdata/connector/paimon/commit/PaimonCommitStateStore.java)、[心跳跨表依赖](../../main/java/io/tapdata/connector/paimon/commit/PaimonMicroBatchCoordinator.java)、[写入语义校验](../../main/java/io/tapdata/connector/paimon/schema/PaimonWriteSemanticContractResolver.java)。 |
| TapData `65add935fdee` | 本地 `tapdata` 仓库 `iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/` 下，`HazelcastTargetPdkBaseNode.java` 的 `initFlushOffsetConsumer`、`dispatchTapdataEvents`、`flushSyncProgressMap`、`processConnectorAfterSnapshot`，`HazelcastSourcePdkDataNode.java` 的 `doSnapshot` 及完成事件发送分支，`concurrent/PartitionConcurrentProcessor.java` 的 `flushWatermarkEvent`、`processPartitionEvents`。启动顺序见目标节点 `createPdkAndInit` 及 `HazelcastPdkBaseNode.createPdkConnectorNode`，来源恢复分支见上一级目录 `HazelcastDataBaseNode.need2InitialSync`，任务错误上报见 `StopTaskOnErrorLog.error(String, Throwable)`。节点状态按 node ID 分配见 `iengine-common` 的 `PdkStateMap` 构造方法。源起点查 `HazelcastSourcePdkBaseNode` 第 702、792、803 行和目标 `HazelcastTargetPdkBaseNode` 第 1754、2098 行。表调用/错误与重试见 `HazelcastTargetPdkDataNode` 第 759、1119、1187 行；最终保存早于基类关闭见目标节点第 2484 行起。 |
| Apache Paimon `release-1.3.2@c05f7d1f1b1e` | [多表提交与恢复源码笔记](../reviews/FLINK-PAIMON-1.3.2-MULTI-TABLE-COMMIT.md)、[提交消息序列化][ser]、[Java API][java-api]。Flink 用 checkpoint 保存待提交状态。本方案通过本机持久登记原生提交消息，对接现有 TapData 位点协议。 |

[java-api]: https://paimon.apache.org/docs/1.3/program-api/java-api/#stream-write
[filter]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L260
[files]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/TableCommitImpl.java#L265
[ser]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/CommitMessageSerializer.java
[sub-snaps]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L323
[table-uuid]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/AbstractFileStoreTable.java#L186
[snap-lookup]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/utils/SnapshotManager.java#L638
[postpone-scan]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/source/DataTableBatchScan.java#L68
[bucket-guide]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/docs/content/primary-key-table/data-distribution.md#L67
[prepare]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/table/sink/StreamTableWrite.java#L33
[writer-checkpoint]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/AbstractFileStoreWrite.java#L320
[flush]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/mergetree/MergeTreeWriter.java#L252
[collect]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/FileStoreCommitImpl.java#L703
[orphan]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/operation/OrphanFilesClean.java#L75
[hashwrite]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/index/DynamicBucketIndexMaintainer.java#L70
[idxfold]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/manifest/IndexManifestFileHandler.java#L133
[idxboot]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/crosspartition/IndexBootstrap.java#L129
[schema-seq]: https://github.com/apache/paimon/blob/c05f7d1f1b1e5d37e64edab0f2978124d90b64f7/paimon-core/src/main/java/org/apache/paimon/schema/SchemaValidation.java#L546
