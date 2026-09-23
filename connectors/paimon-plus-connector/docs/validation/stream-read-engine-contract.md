# Engine 契约核对

2026-09-22；只读源码基线 tapdata/develop@ba3533f47fc3c43e9f0dc408f5e871986f1e8626。没有配置真实 Engine 测试任务，S15 黑盒验证未执行；不将本地测试代替部署兼容性证明。

- HazelcastSourcePdkBaseNode.java，initStreamOffsetFromTime（841 起）：初始化能力返回值写入 syncProgress；正常非共享 CDC 路径调用。share CDC 的空 Map 路径不能按本 Spec 宣称兼容。
- HazelcastSourcePdkDataNode.java，generateStreamReadConsumer（1169 起）：仅非空事件进入处理，入队后保存内存 offset，异常可能在 Engine 处理；回调返回无 durable ack。
- HazelcastSourcePdkBaseNode.java，wrapTapdataEvent（1403 起）：批次最后事件携带新 offset；Heartbeat 有专门转换路径。
- HazelcastTargetPdkBaseNode.java，saveToSnapshot（2030 起）：源 FlushOffsetFunction 在持久化之前，不能用于 Consumer 前移。

来源路径根：/Users/SL/javaProject/tapdata/iengine/iengine-app/src/main/java/io/tapdata/flow/engine/V2/node/hazelcast/data/pdk/。源码链接及 PDK artifact 见 Spec T0–T4。

后续真实环境必须验证：普通/异步/多目标 Heartbeat 顺序；plan 中途、回调后、目标完成未保存、保存后四种退出；全量至增量租约；旧 attempt 退出。无环境时保持未验证，不修改内核。
