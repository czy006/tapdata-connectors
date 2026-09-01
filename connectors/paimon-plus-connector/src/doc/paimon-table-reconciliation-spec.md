# Spec: Paimon 存量表校对与建表加固（Table Reconciliation & Create Hardening）

> 状态: DRAFT v1 — 等待人工评审
> 基线分支: `jira/fix-paimon-connector-types`
> 客户问题: 复制任务创建的表报错、开发任务建的不报错；客户不能重建表。

## 1. Objective

修复 paimon-plus-connector 的建表/存量表处置流程。两个交付物（各自独立分支 + cherry-pick 回基线）：

**PR-1 存量表校对（Reconciliation）**：任务启动时 `createTableInternal` 发现表已存在，不再"只 warn bucket 模式就跳过"，而是拿**期望 schema**（按当前连接器配置 + TapTable 重建，主键取 `tapTable.primaryKeys(true)`，含更新条件转化来的逻辑主键）与**物理表**做 diff：
- **可变选项**差异 → `catalog.alterTable` 自动原地对齐（连接器配置为唯一事实源），日志记录 before/after；
- **结构差异**（主键/分区键/列名/列类型不一致，含不可 alter 的选项冲突）→ 抛 `PAIMON_EXISTING_TABLE_CONFLICT`，报精确 diff 与可行动指引。

**PR-2 建表加固**：
- 新增连接器配置 `noPrimaryKeyStrategy`（**客户可选**）：`APPEND_ONLY`（默认，现状）| `ERROR`（源表无主键且无逻辑主键时建表直接拒绝）；
- append-only 表收到 retract 事件（DELETE/UPDATE_BEFORE）时，连接器侧 fail-fast 报可行动错误，替代 Paimon 原生晦涩文案。

**用户故事**：
- 作为使用无主键源表 + 手动更新条件的复制任务客户，我重启任务后，坏选项被自动修复，坏结构得到明确报错（说明"更新条件未作用到已存在的表"），而不是写入深处的 `Append-only writer...`。
- 作为支持工程师，我能从报错直接读出根因与出路，不再只能建议"重建表"。

## 2. Tech Stack

- Java（版本随父 pom），JUnit 5，现有 paimon-plus-connector 模块内实现。
- Apache Paimon 1.3.2（`Catalog.alterTable(Identifier, List<SchemaChange>, ignoreIfNotExists)`；`SchemaChange.setOption/removeOption`；不可变选项 = `CoreOptions.IMMUTABLE_OPTIONS` ∪ bucket 特殊规则：old=-1 / new=-1 / postpone 时禁止修改，见 SchemaManager#checkResetTableOption 上下文）。
- TapData PDK（`TapTable.primaryKeys(true)` 优先返回 `logicPrimaries`，为空时从字段级 `primaryKeyPos` 推导——已从 pdk-api 2.0.9 字节码验证）。

## 3. Commands

```
构建:      mvn -pl connectors/paimon-plus-connector -am install -DskipTests
全量测试:   mvn -pl connectors/paimon-plus-connector test
单测试类:   mvn -pl connectors/paimon-plus-connector test -Dtest=PaimonExistingTableReconcilerTest
```

## 4. Project Structure

```
src/main/java/io/tapdata/connector/paimon/
  service/PaimonExistingTableReconciler.java    [新增, PR-1] diff + 分层处置 + alter 执行
  service/PaimonService.java                    [修改, PR-1] 抽取 buildExpectedSchema(TapTable,tableName)；
                                                              createTableInternal 已存在分支挂 reconciler
  config/PaimonConfig.java                      [修改, PR-2] noPrimaryKeyStrategy 字段 + 校验
  write/bucket/BucketUnawareWriterStrategy.java [修改, PR-2] doWrite 增加 retract RowKind 守卫
src/main/resources/spec.json                    [修改, PR-2] 新增下拉配置项（默认 APPEND_ONLY）
src/main/resources/docs/paimon_zh_CN.md         [修改, PR-2] 文档
src/test/java/io/tapdata/connector/paimon/
  service/PaimonExistingTableReconcilerTest.java [新增, PR-1]
  service/PaimonServiceNoPrimaryKeyStrategyTest.java [新增, PR-2]
  write/bucket/BucketUnawareRetractGuardTest.java    [新增, PR-2]
```

## 5. Code Style

沿用模块现状：制表符缩进、英文 javadoc、涉及 Paimon 内核行为时注释引用 baseline 源码行号、错误码用 `PAIMON_` 前缀常量风格、测试遵循 JUnit5 包内私有 `Must` 断言惯例。示例（PR-1 的冲突异常构造）：

```java
	public static PaimonFatalWriteException existingTableConflict(
			String tableKey,
			ReconcileDiff diff) {
		return new PaimonFatalWriteException(
				"PAIMON_EXISTING_TABLE_CONFLICT table=" + tableKey
						+ ", reason=physical table does not match the schema the task expects"
						+ ", expectedPrimaryKey=" + diff.expectedPrimaryKeys()
						+ ", actualPrimaryKey=" + diff.actualPrimaryKeys()
						+ ", missingColumns=" + diff.missingColumns()
						+ ", typeMismatches=" + diff.typeMismatches()
						+ ", extraColumns=" + diff.extraColumns()
						+ ", immutableOptionConflicts=" + diff.immutableOptionConflicts()
						+ ". Paimon cannot alter primary keys, partition keys, column types in place;"
						+ " rebuild the table from the task model or align the task config with it.");
	}
```

## 6. Testing Strategy

- 单元测试（本地临时目录 FileStoreTable catalog，无需 MinIO/Spark）：
  - PR-1: ① 存量表带遗留 `changelog-producer=input` → 启动后被移除且选项与期望一致；② 期望有 PK（逻辑主键）而物理无 PK → 抛 conflict 且 message 含 expected/actual PK；③ 列类型不匹配（native ARRAY vs 期望 STRING）→ conflict、不尝试 alter；④ 不可变选项（bucket）冲突 → conflict（见 Open Questions Q1）；⑤ 无差异 → 不产生任何 alter 调用；⑥ hashKey 关闭后遇 `_hash_key` 遗留列 → conflict。
  - PR-2: ① `noPrimaryKeyStrategy=ERROR` + 无 PK TapTable → 建表抛 `PAIMON_NO_PRIMARY_KEY`（文案含三个出路）；② 默认 `APPEND_ONLY` 行为不变（建出无 PK 表）；③ `BucketUnawareWriterStrategy` 对 DELETE/UPDATE_BEFORE 抛 `PAIMON_APPEND_ONLY_RETRACT`，INSERT/UPDATE_AFTER 放行。
- 每个 PR：全模块 `mvn test` 绿 + subagent 评审 + 反向验证（先写测试看到新错误码，再实现）。

## 7. Boundaries

- **Always**: 每 PR 全量测试绿；一议题一分支，cherry-pick 回 `jira/fix-paimon-connector-types`；reconciler 对用户可见行为（alter/异常）必须打日志。
- **Ask first**: 修改 `createTableInternal` 已存在分支的处置语义超出本 Spec；新增配置默认值；对物理表执行任何写操作的新路径。
- **Never**: 不做影子表迁移/数据重写；不改引擎侧（更新条件合并时序）；不删/跳过既有失败测试；不自动修改 `IMMUTABLE_OPTIONS` 内的键。

## 8. Success Criteria

1. 带遗留坏选项（`changelog-producer=input`）的存量表，任务重启后无需重建即可正常写入（测试①证实）。
2. 结构性损坏的表在任务启动时抛 `PAIMON_EXISTING_TABLE_CONFLICT`，message 可直接读出差异与出路（测试②③④⑥）。
3. 无 PK 源表行为由 `noPrimaryKeyStrategy` 决定，默认不改变现状（PR-2 测试①②）。
4. append-only 表收到 UPDATE/DELETE 时报 `PAIMON_APPEND_ONLY_RETRACT`，文案可行动（PR-2 测试③）。
5. 既有测试全部保持绿色。

## 9. Open Questions

- **Q1（需裁决）**: 不可 alter 选项（bucket、`IMMUTABLE_OPTIONS`）的差异按已确认意图升级为 fail-fast，但现状是 warn-only 且写入路径兼容两种 bucket 模式——严格化会让"配置漂移但可运行"的存量任务开始报错。选项: (a) 按意图 fail-fast；(b) 仅此类差异维持 warn+continue（本 Spec 当前按 (a) 编写）。
- **Q2**: `noPrimaryKeyStrategy` 是否需要 per-table 覆盖（tableConfig 通道）？当前 Spec 只做连接级全局配置。
- **Q3**: 客户侧取证（问题表 schema、建表日志 `Created table ... with schema:`、更新条件配置时序）尚未回报；不影响本 Spec 实施，但影响售后话术。
