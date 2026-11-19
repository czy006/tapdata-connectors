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

2026-11-19 使用本地引擎构建及 JDK 17，在进程内 Manager 边界得到四行 PASS，独立 JVM 退出码为 0：

```text
PASS serial empty A=OVER without P0; source recovery requested current
PASS initial-only restart needs no stream cursor; concurrent selector selected P0
PASS CDC P1 restored; ACK P2 precedes failed persistence leaving P1 durable
PASS engine + in-memory Manager boundary; real Mongo NOT RUN
```

用真实 Mongo 模式在相同环境发起了独立测试。TapData 管理 Java 进程有连接至本机 27017 的既有会话；此执行环境新建该连接返回 `Operation not permitted`，driver 在 `ping` 阶段抛出 `MongoTimeoutException`。浏览器端口和 Manager 端口的新连接也被系统拒绝；没有读取或修改当前业务任务。该次真实 Mongo 运行未执行任何写入，不能计入 T01/T15 的真实持久化和源端重放验收。

## 对上线结论的约束

新部署应对实际源连接器、有效任务配置和全量到 CDC 的切换时机验证旧位置能在重启后被传回源端。单独保存目标原生清单只能恢复已交付的输入。另建隔离任务时可在 A 全量结束、首次成功 CDC 心跳前更新或删除 A，并在初始空表插入一行。对照 Mongo 保存的实际起点、源端收到的恢复起点、Paimon 普通查询逐主键结果后，再对该任务组合签署验收。

固定桶全量、CDC 定时提交以及同机静默恢复的容量实测和重跑命令见 [固定桶容量样本](CAPACITY.md)。
