# TapData 恢复起点验证

本夹具在单独 JVM 中调用本地 `tapdata@65add935fdee7fa6bccb5abfb85555b973d69697` 已编译的引擎方法。它为 T01、T12 和 T15 提供可重复的位点检查。生产连接器 JAR 不包含该夹具。

## 执行

需要 JDK 17、同版本已构建的 TapData `iengine-app`、引擎测试依赖和 Maven。仓库路径及 Maven 可执行文件从环境读取。工作目录可以是包含本说明的目录。

```sh
export TAPDATA_REPO=/path/to/tapdata
export JAVA_HOME=/path/to/jdk-17
export MAVEN=mvn
bash run-anchor-probe.sh
```

未传 `PROBE_MONGO_URI` 时使用进程内 Manager 持久化边界。传入有专用测试库权限的 Mongo URI 时，脚本使用 driver 的真实 majority 写入，重新从 Mongo 读取进度，然后调用源端恢复方法。每次在固定命名空间 `paimon_local_recovery_e2e.anchor_probe` 创建随机 ObjectId 的新文档，并在退出前只清理该文档。客户端第一次操作是 `ping`。注入的失败发生在第二次 CDC 位点写入之前。

### 覆盖的实际路径

| 输入 | 断言 |
| --- | --- |
| 串行 INITIAL 心跳只有流位点 P0，随后空表 A 完成全量并保存 | 引擎跳过首次心跳的控制处理。重新解码保存的进度，A 的状态是 OVER，但流位点为空。`INITIAL_SYNC_CDC` 的普通源进入从当前取位点的分支。 |
| 同一份进度恢复纯全量任务；同一 P0 事件送入并发选择方法 | 纯全量恢复不会请求流位点，并发选择器会选取 P0 事件。本项没有运行整条并发 worker 队列。 |
| 串行 CDC 的 P1、P2 心跳经 PDK 回调，第二次 Manager 写入失败 | 重新读回第一次的进度后，纯 CDC 源能解码 P1。源端 `FlushOffsetFunction` 已收到 P1、P2，但第二次恢复读取的仍是 P1。源日志可以从 P1 重读是单独的源连接器前提。 |

测试走真实的串行事件筛选、PDK offset Consumer 安装、进度复制/序列化、目标 `saveToSnapshot`、重启 `foundAllSyncProgress`、全量位点解码及流位点恢复分支。PDK 目标回调在夹具中代表“该心跳已可确认”，源端 flush 仅记录调用，Manager HTTP 边界被替换成与 `TaskServiceImpl.updateSyncProgress` 对应的逐 edge 存储。它没有启动源数据库 CDC，也没有验证新 Paimon 连接器、两个 worker 的交错、日志保留或逐主键端到端结果。

### 当前实测

在本地引擎构建及 JDK 17 上运行，进程内 Manager 对照的断言如下。

```text
PASS engine produces the same target node configuration for initial, CDC and combined tasks
PASS serial empty A=OVER without P0; source recovery requested current
PASS initial-only restart needs no stream cursor; concurrent selector selected P0
PASS CDC P1 restored; ACK P2 precedes failed persistence leaving P1 durable
PASS engine + in-memory Manager boundary; real Mongo NOT RUN
```

用真实 Mongo 模式在相同环境发起了独立测试。TapData 管理 Java 进程有连接至本机 27017 的既有会话；此执行环境新建该连接返回 `Operation not permitted`，driver 在 `ping` 阶段抛出 `MongoTimeoutException`。浏览器端口和 Manager 端口的新连接也被系统拒绝；没有读取或修改当前业务任务。该次真实 Mongo 运行未执行任何写入，不能计入 T01/T15 的真实持久化和源端重放验收。

## 当前分支的运行边界

开启本机恢复后，目标连接器能在连续运行期间接受全量和 CDC；全量在结束回调发布，CDC 在满足提交条件时发布。已有绑定再次启动时先补交本机可恢复的批次，再以 `PAIMON_RECOVERY_SOURCE_POSITION_UNVERIFIED` 停止。该判断也适用于正常停机、空表以及没有新事件的任务。目标 PDK 没有获得源端实际重启位置。只给已有任务换一个目录无法建立正确的源基线。

设置上文环境变量，并先在连接器模块运行 `PaimonStartupRecoveryIntegrationTest`，使 Maven 生成联动测试的 classpath。以下命令都在本目录执行。每次运行均新建独立 warehouse 和本机恢复目录。

### 多表提交、源位点与进程重启

```sh
bash run-anchor-probe.sh --coupled
bash run-anchor-probe.sh --concurrent
```

`--coupled` 运行两个独立任务，每个任务各使用两个 JVM。DML 经引擎的 `handleTapdataEventDML` 生成元数据并交给正式注册的 PDK 写入口；心跳经真实引擎筛选、连接器 control、引擎位点回调和源恢复方法。目标使用真实 Paimon 表并通过普通批读核对。源变更和源端 flush 由夹具提供，节点 KV 与 Manager 默认采用文件持久化。设置 `PROBE_MONGO_URI` 可尝试用真实 Mongo 替代进度文件。

| 任务 | 首次运行 | 重启断言 |
| --- | --- | --- |
| 全量中断 | 两表各两行。串行引擎筛掉只有流位点的 INITIAL 心跳。A 的 OVER 已保存，两表都没有可见快照，随后强退。 | 没有新输入也补回全部四行，再拒绝启动。晚到心跳无法推进位点，随后写入的新键也被拒绝。 |
| 连续运行与重放 | 全量末尾发布两表，之后移动 A 的主键、删除旧行并更新 B。B 未提交时扣住 P2；B 发布后，模拟 Manager 写入失败，持久进度仍为 P1。同一进程重复交付 P2，逐键得到 A=`{3:moved}`、B=`{7:changed,8:keep}`，进度可解码为 P2。 | 即使夹具可以从 Manager 解码 P2，正式目标 PDK 仍得不到源端实际采用的起点。连接器拒绝启动，已有快照和键值正确。 |

`--concurrent` 用两个实际分区 worker 和可控的 Manager 请求顺序验证三个边界。并发路径将首个带 P0 的 INITIAL 心跳交给连接器，回调保持 INITIAL 阶段；较早的全量进度晚于 P1 保存时可以将 P1 回退到 P0。串行路径可在目标之前滤掉同一 P0，迟到的全量进度能清空 P1。在纯 CDC 段，较早的 P1 请求晚于 P2 完成仍可覆盖 P2。目标提交不会改变 Manager 的保存顺序。默认的 `--anchor` 探针另通过引擎 `generateNodeConfig` 比较纯全量、纯 CDC 与组合任务的目标节点配置。

### 物理表、默认值与时间主键

```sh
bash run-review-probe.sh LocalRecoveryImageAudit
bash run-review-probe.sh FallbackBranchReviewProbe
bash run-review-probe.sh TemporalPrimaryKeyProbe fixed
bash run-review-probe.sh TemporalPrimaryKeyProbe dynamic
```

四次运行都应成功退出。映射探针检查缺列、INT 主键写入 FLOAT 目标、带默认值列的显式 NULL 以及普通 NULL 对照。分支探针比较普通表与按分区使用 fallback 的表。时间探针通过实际 PDK codec 验证固定桶和动态桶中的插入、更新和删除，并读取原有已提交键以检查误覆盖。

同机固定桶的容量测量见[固定桶样本](CAPACITY.md)。旧版发现与源码定位见[审查报告](../../../src/doc/reviews/REVIEW-paimon-local-recovery-engine-2026-11-19.md)。
