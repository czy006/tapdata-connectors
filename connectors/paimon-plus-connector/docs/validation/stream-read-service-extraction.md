# StreamRead Service 抽取验证

日期：2026-09-23。分支：`codex/paimon-stream-read`；抽取前基线：`c4a33428`。修改仅在本连接器内。

## 职责与保持条件

新增 [PaimonStreamReadService](../../src/main/java/io/tapdata/connector/paimon/read/PaimonStreamReadService.java)，迁入时间位点初始化、表身份/历史/schema 校验编排、scan 与 reader 绑定、RowKind 映射、活动会话、Consumer 保护状态及续租关闭。

[PaimonService](../../src/main/java/io/tapdata/connector/paimon/service/PaimonService.java) 保留原公开入口并委托新类，通过三个 Runtime 方法提供入口准入、Catalog 查表和受管 reader 创建；其行数由 3471 减至 3200。新类 367 行，增加的代码主要用于明确依赖边界。

- 停止锁、StopController、资源台账仍为同一实例；Catalog 仍由宿主关闭。
- 宿主先取消活动会话，再等待入口静默、关闭 Consumer 租约、释放共享资源。
- 超时保留根仍是 StopController 绑定的原宿主，不因方法移动后 `this` 改变而丢失 Catalog/写入资源。
- 全量/查询的入口和结果回调仍检查同一 Consumer 保护状态。
- 原 offset 规则、回调顺序、失败传播、线程数量、队列容量、Consumer 固定起点不变。
- 9 个迁移方法主体经去注释/空白比对，在明确替换宿主依赖调用后与基线一致；原 3 处固定 Paimon 源码链接随实现保留。
- 按用户要求，read 目录及相关源端接口、参数和测试注释统一使用英文，Markdown 文档保留中文。翻译前后 14 个文件的可执行 token 一致，与通过完整回归的代码行为保持一致。

## 测试结果

| 验证 | 结果 |
|---|---|
| 首轮 StreamRead / Consumer / 类型 / 有限读取定向测试 | 81 项通过 |
| 完整连接器回归，启用 MySQL 时间类型合同 profile | 810 项通过，0 失败、0 错误、0 跳过 |
| Surefire XML 汇总 | 87 个报告，共 810 项，与日志一致 |
| 英文注释调整后的主代码/测试代码编译 | test-compile 通过；只改注释，未重复执行完整回归 |
| 架构文档 | 8 张 Mermaid 图语法通过；相对文件链接存在 |
| 差异检查 | 无空白错误；未修改其他连接器或 TapData 内核 |

既有测试中仅随所有权迁移调整了两个测试类的内部访问位置，没有删除断言。额外新增/加强：

1. 通过公开 `streamRead` 入口读取真实本地 Paimon 表，调用方仍要求运行时，Service.close 能取消流读并完成 `streamReadEnded`；关闭不删除 Consumer 文件，关闭后拒绝初始化。
2. 注入 Consumer 保护失败后，位点初始化、batchCount、batchRead、executeQuery 都保留原始故障原因并拒绝继续。
3. 停止超时且仍有读取入口在执行时，资源保留根是原 PaimonService，Catalog 不被提前关闭。

完整回归耗时 2 分 31 秒，于 2026-09-23 00:18:46 +08:00 完成。日志在模块的忽略目录 `target/stream-read-validation/`：

| 日志 | SHA-256 |
|---|---|
| `extract-stream-focused.log` | `d18e0a4da2383d2e372635c8757972ac312c48180c47b3960e0a0f7a0c6c1804` |
| `extract-stream-full-regression.log` | `c7c3fca3b81f547b928ff675e69947b65c2c46cf552b3dcd500a2732d0d0053d` |

复现命令（仓库根目录；使用本 worktree 已准备的离线依赖）：

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-17.jdk/Contents/Home \
mvn -o -B -pl connectors/paimon-plus-connector \
  -Pmysql-temporal-contract-test \
  -Dmaven.repo.local="$PWD/connectors/paimon-plus-connector/target/stream-read-validation/offline-repository" \
  -Dmaven.gitcommitid.skip=true \
  -Dtapdata.pdk.runner.version=2.8-SNAPSHOT \
  -Dpdk-error-code.version=2.8-SNAPSHOT \
  -Djava.io.tmpdir="$PWD/connectors/paimon-plus-connector/target/stream-read-validation/tmp" \
  -Djacoco.append=false test
```

这是保持行为的结构抽取，不增加吞吐或 exactly-once 承诺。测试覆盖本地 Paimon、PDK/MySQL codec 合同及生命周期；没有执行真实 Engine→MySQL 数据库链路或生产容量测试。
