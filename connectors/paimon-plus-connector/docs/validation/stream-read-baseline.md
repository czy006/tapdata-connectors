# StreamRead 实施基线

2026-09-22；原代码基线 b10f0150（功能代码来自 7c40958f）。

JDK 17（Zulu），Java source/target 8；Paimon 1.3.2，PDK API 2.0.8-SNAPSHOT；本地 runner/error-code 2.8-SNAPSHOT 验证覆盖参数沿用 Plan。

执行 Plan G，附加 `-o -B` 与模块内 `-Djava.io.tmpdir=.../target/stream-read-validation/tmp`。BUILD SUCCESS；69 个报告，{'tests': 735, 'failures': 0, 'errors': 0, 'skipped': 0}。日志：模块 target/stream-read-validation/baseline.log；原始 XML 已另存 baseline-reports。本结果是本地模块测试，不是 Engine 端到端证明。

用户已授权基于 Spec 全新实现，不保留旧实现和歧义位点兼容路径；沿用 none 默认与手动 fixed-start。当前范围为本地开发和测试，真实部署验证保持独立发布门槛。
