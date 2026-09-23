# StreamRead 本地实现交付记录

2026-09-22；分支 `codex/paimon-stream-read`，工作树 `/Users/SL/.codex/worktrees/paimon-stream-read/tapdata-connectors`。实现基线 `7c40958fdaf3c4f76ef8eb0312550ad16ee81faf`。用户确认“不需要保留旧的实现，完全基于 Spec 全新实现”，并授权开发、本地测试和逐单元本地提交。

## 交付行为

- 完全替换旧 StreamRead 循环及旧 offset 解析；继续通过 TapData capability、StreamReadConsumer、事件/offset 回调和原有 Service 生命周期集成。
- `read/` 下实现严格 v1 envelope、统一时间边界解析、逐 plan schema/历史校验、有界多表调度、单线程事件交付和完整 plan 结束标记。next 恢复不加一；半 plan 失败保留旧位点，可重放。
- 无业务事件但位点推进使用 HeartbeatEvent；callback 返回仅表示提交，不作下游持久化确认。多表无进展重新进入轮询，低流量按刷新间隔提交。
- Consumer 默认 none；fixed-start 通过 Paimon 内部 ConsumerManager 注册独立 attempt，始终保护原始 start。初始化注册覆盖全量阶段，流读复用匹配租约；独立 watchdog/续租、TTL 校验、保守 deadline、有限重试和 sticky failure；停止不删除 Consumer。
- 源会话接入现有停止门禁与资源账本，先取消再等待；无法证明 reader/线程关闭时报告失败并保留共享 Catalog/FileIO 的资源引用。
- 关键适配代码包含固定 Paimon commit、源码路径/方法/位置及语义解释。没有修改 Engine、PDK 或写入算法，没有新增 durable-ack 假设。

## 验证结果

| 验证 | 结果 |
|---|---|
| 开发前同环境完整基线 | 735 项，0 失败/错误/跳过 |
| 最终完整模块回归 | **789 项，0 失败、0 错误、0 跳过** |
| Surefire XML 交叉核对 | 82 个 suite，合计 789 项，与 Maven 汇总一致 |
| 净增加测试 | 54 项；其中部分方法覆盖多种原生配置组合 |
| 实表矩阵 | [表模式与恢复证据](stream-read-table-matrix.md) |
| 独立只读源码复核 | 已处理明确问题，最终未发现仍需处理的明确 P0/P1/P2 缺陷；不等于无缺陷证明 |
| 真实 Engine/目标故障恢复 | **未执行**，见 [缺失证据及验收矩阵](stream-read-recovery-results.md) |

执行时间：2026-09-22 15:33:47 +08:00 完成，耗时 2 分 50 秒。功能代码与实表矩阵对应 `e152c294`；其后仅整理文档及源码注释，未改变执行逻辑。

日志：`target/stream-read-validation/full-regression.log`，SHA256 `4204766b39aa43ed08c9cbcf0df50d52b9d864fe51e549b1b7b57bc789792707`。最终 XML 另保存于 `target/stream-read-validation/final-reports/`；这些是本地生成文件，不纳入 Git。

复现命令（工作树根目录；JDK 17，编译目标仍为 Java 8）：

```bash
env JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
  mvn -o -B -pl connectors/paimon-plus-connector \
  -Dmaven.gitcommitid.skip=true \
  -Dtapdata.pdk.runner.version=2.8-SNAPSHOT \
  -Dpdk-error-code.version=2.8-SNAPSHOT \
  -Djava.io.tmpdir=/Users/SL/.codex/worktrees/paimon-stream-read/tapdata-connectors/connectors/paimon-plus-connector/target/stream-read-validation/tmp \
  -Djacoco.append=false test
```

实际 Paimon 依赖 1.3.2，源码验证使用 release-1.3 的 `5c59e6cb01ed0b29563371f56e14fcade4597a2e`；PDK API 2.0.8-SNAPSHOT。runner/error-code 的命令行覆盖与基线一致，只用于已有本地依赖，不修改 pom。测试未触发 package，避免生成模块外 connectors/dist 产物。现有 SLF4J 多绑定、log4j 配置提示不影响测试；最终 JaCoCo 使用全新执行数据，没有沿用中间构建的旧 class 覆盖率结果。

## 实现提交账本

| 提交 | 单元 |
|---|---|
| d6c62884 / b10f0150 / b7c40bdb | Spec/Review、Plan、基线与实施授权 |
| 75b89527 / d790a412 / cf7d34a3 | offset、时间/历史范围、schema guard |
| 76d18bf1 / b5816d74 | 完整 plan 读取、交付边界与 Heartbeat |
| 4a704ee9 / d346b2d5 | null RowKind/nullable 修正、公平调度与可取消队列 |
| 082ac4ee / f88c25aa | Service 停止接缝、配置与语言文案 |
| 9c7bdc08 | 正式入口全新实现，删除旧循环 |
| 6901d68b / 61dba9ed / 20ceab70 | Consumer 注册、独立租约、全量至流读保护 |
| 21aaef39 | 审查修正：关闭竞态、安全 TTL、解耦语义、元数据错误分类 |
| e152c294 | 原生 changelog/overwrite/恢复/Consumer 隔离矩阵 |

最终文档提交在上述账本之后；以本分支 `git log` 为准。没有 push、merge 或发布。

## 范围核对与边界

所有改动限于 `connectors/paimon-plus-connector/`。原工作区仍为 hotfix-spill，原已暂存旧 Spec blob 仍为 `c087661c9c81ab71bf56d24547aed639b9129cf9`，原有及其他任务的未跟踪文档未改动。工作树的历史 Spill 文档只作归档保留。

迁移需要通过现有任务操作显式初始化 v1 位点；拒绝 null stream offset/旧 Map，不尝试猜测旧值。Consumer fixed-start 会保留整个读取周期历史，不能根据回调或进度自动前移。同一 Service 切换 epoch/请求表集合需重启。无 Consumer、历史已过期、租约失效或 schema 改变时明确失败。

本地单元和原生表测试不证明跨进程 fencing、多目标 Heartbeat 顺序、远端存储一致性、生产 exactly-once 或任意阻塞 I/O 都能限时终止。Paimon 原生 plan 内部循环、Consumer 过期没有 CAS、同路径重建后 id 重合等限制仍按 Spec 保留。S15 真实 Engine 场景未完成，不据此声称可直接生产发布。
