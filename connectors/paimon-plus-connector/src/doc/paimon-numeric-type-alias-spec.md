# Spec: Paimon 数值类型别名建表修复（Numeric Type Alias Fix）

> 状态: DRAFT v2 — 等待人工评审（v2 更正客户根因，见 §1）
> 基线分支: `jira/fix-paimon-connector-types`（注意: 该分支远端已被 force-reset 到 `1bbe93a6`，本地领先 24 提交；两个尖端的 converter 内容一致，`int(11)` 修复均已包含，差异仅在其余提交）
> 客户问题: MSSQL → Paimon 复制任务自动建表，`Rooms`/`ExtractionHour` 两个整数字段被建成 varchar/STRING；客户手工建的表是 INT。

## 1. Objective

**根因更正（v2，2026-08-31 实证）**：客户源端实际下发串为 `int(11)`。当前分支 `parse()` 将其归一为 name=`INT`（括号参数进 arguments 被忽略），round-1 类型重构（`9c51e0c8`/远端 `1bbe93a6`）新增的 `case "INT"` 直接命中 → `DataTypes.INT()`，**客户场景在两个尖端上均已修复**（scratch 测试实证 `int(11)`/`bigint(20)`/`tinyint(1)` 全部正确映射）。客户中招根因是其构建**早于** 8 月 9 日重构：旧代码只有 `case "INTEGER"`，经 `StringKit.removeParentheses`（正则 `\(.*?\)` 删参）得 `INT` 后落入 default → STRING → varchar——且同表 `DECIMAL(18,2)`/`DATE`/`TIMESTAMP(6)`/`VARCHAR(n)` 在旧代码均在白名单，故**恰好只有两个 int(11) 字段损坏**，与症状完全吻合。客户修复路径 = 部署含 round-1 的构建 + 重建坏表（Paimon 不能原地改列类型）。

本 Spec 的定位因此从"客户根因修复"调整为**防御性加固**：当前 `PaimonDataTypeConverter` 仍存在数值盲区——`NUMERIC(p,s)`/`NUMBER`（MSSQL/Oracle 常用，与 DECIMAL 同义混用）、`int(11) unsigned` 类后缀串仍会静默落入 `default → STRING()`。下一个同型客户问题只是时间问题。

**PR-1（唯一交付物）数值别名分档**：
- 新增数值别名分支：`NUMERIC` / `NUMBER`（含带参/不带参）、`INT2` / `INT4` / `INT8`、`MEDIUMINT`、整型 `UNSIGNED [ZEROFILL]` 后缀提档；
- `NUMERIC`/`NUMBER` 按 scale 分档映射（本次修复的核心规则，已裁决）：
  - 不带参 → `DECIMAL(38,10)`（与现有裸 `DECIMAL` 默认一致）；
  - scale > 0 → `DECIMAL(p, s)`；
  - scale = 0 → **p ≤ 10 → `INT`；11 ≤ p ≤ 19 → `BIGINT`；p ≥ 20 → `DECIMAL(p, 0)`**（阈值取自本连接器 spec.json `dataTypes` 自身约定：INT=10 位、BIGINT=19 位）；
  - 单参 `NUMERIC(p)` 视为 scale=0 参与分档；
  - 非法参数（非整数、p < s、p > 38）沿用现有 `PAIMON_INVALID_DATA_TYPE` 抛错路径。

**用户故事**：
- 作为 MSSQL → Paimon 的复制任务客户，源表里声明为 `NUMERIC(10,0)`/`NUMERIC(38,0)` 的整数列，自动建表后得到数值类型（int/bigint/decimal），而不是 varchar；数值写入、下游聚合/Join 不再被字符串类型破坏。
- 作为支持工程师，客户报"整数字段变 varchar"时，可直接对号入座本别名规则，不再需要逐字段取证。

**显式不做（已裁决）**：不加 tapType 结构化兜底；不补非数值别名（`DATETIME2`/`BIT`/`MONEY` 等仍维持 → STRING 现状）；不给剩余 default 路径加 warn 日志（见 Open Questions Q1）。

## 2. Tech Stack

- Java（版本随父 pom），JUnit 5，paimon-plus-connector 模块内实现，无新依赖。
- 纯静态方法改造：`PaimonDataTypeConverter`（`parse()` 增加尾缀 token 剥离 + `ParsedType` 增加 unsigned 标记；switch 增加别名 case 与分档私有方法）。

## 3. Commands

```
构建:      mvn -pl connectors/paimon-plus-connector -am install -DskipTests
全量测试:   mvn -pl connectors/paimon-plus-connector test
单测试类:   mvn -pl connectors/paimon-plus-connector test -Dtest=PaimonDataTypeConverterTest
```

## 4. Project Structure

```
src/main/java/io/tapdata/connector/paimon/
  schema/PaimonDataTypeConverter.java        [修改] parse() 剥离 UNSIGNED/ZEROFILL 尾缀；
                                                    switch 新增 NUMERIC/NUMBER/INT2/INT4/INT8/MEDIUMINT；
                                                    新增 numericAlias(...) 分档私有方法
src/test/java/io/tapdata/connector/paimon/
  schema/PaimonDataTypeConverterTest.java    [修改] 新增别名/分档/unsigned/回归用例
README.md                                    [修改] Data Type Mapping 一节补数值别名与分档规则
src/doc/paimon-numeric-type-alias-spec.md    [新增] 本 Spec
```

## 5. Code Style

沿用模块现状：制表符缩进、英文 javadoc、错误码 `PAIMON_` 前缀、测试遵循 JUnit5 包内私有 `Must` 断言惯例、Locale 无关（`Locale.ROOT` 归一化）。分档核心示例：

```java
	private static DataType numericAlias(ParsedType parsedType) {
		Pair<Integer, Integer> ps = numericPrecisionAndScale(parsedType);
		int precision = ps.getLeft();
		int scale = ps.getRight();
		if (scale == 0) {
			// Tier thresholds mirror this connector's own spec.json dataTypes contract:
			// INT declares precision 10, BIGINT declares precision 19.
			if (precision <= 10) {
				return DataTypes.INT();
			}
			if (precision <= 19) {
				return DataTypes.BIGINT();
			}
		}
		return DataTypes.DECIMAL(precision, scale);
	}
```

unsigned 提档表（仅作用于整型根）：

```
TINYINT  UNSIGNED → SMALLINT      SMALLINT UNSIGNED → INT
INT      UNSIGNED → BIGINT        BIGINT  UNSIGNED → DECIMAL(20,0)
```

## 6. Testing Strategy

全部用例先在当前代码上**反向验证失败**（新断言先红后绿），再实现：

- **分档（核心）**：`NUMERIC(10,0)`→INT；`NUMERIC(19,0)`→BIGINT；`NUMERIC(38,0)`→DECIMAL(38,0)（客户场景）；`NUMBER(10,0)`/`NUMBER(19,0)`/`NUMBER(38,0)` 同规则；`NUMERIC(18,2)`→DECIMAL(18,2)；裸 `NUMERIC`/`NUMBER`→DECIMAL(38,10)；单参 `NUMERIC(10)`→INT。
- **别名**：`INT2`→SMALLINT、`INT4`→INT、`INT8`→BIGINT、`MEDIUMINT`→INT、`MEDIUMINT(9)`→INT（显示宽度参数忽略）。
- **unsigned 后缀**：`INT UNSIGNED`→BIGINT、`int unsigned zerofill`→BIGINT、`SMALLINT UNSIGNED`→INT、`TINYINT UNSIGNED`→SMALLINT、`BIGINT UNSIGNED`→DECIMAL(20,0)；`DECIMAL(18,2) UNSIGNED`→DECIMAL(18,2)（非整型根忽略标志）。
- **零行为回归**：`DECIMAL(10,0)` 仍→DECIMAL(10,0)（**分档不作用于已识别的 DECIMAL**）；`DECIMAL(2,3)` 仍抛 `PAIMON_INVALID_DATA_TYPE`；`TIME(4)` 仍抛错；`source_specific_type` 仍→STRING；` int `/`integer` 仍→INT；Turkish Locale 下新别名归一化不受影响。
- **显示宽度回归锁（当前已通过，固化防倒退）**：`int(11)`→INT、`bigint(20)`→BIGINT、`tinyint(1)`→TINYINT、` int ( 11 ) `→INT（客户场景即属此类，必须有显式用例锁定）。
- **整链路（复用既有本地 FileStore catalog 测试设施，若 `PaimonServiceCreateTableValidationTest` 具备则加一例，否则以 converter 用例为准）**：TapField(`"NUMERIC(10,0)"`) 建出的物理列类型为 `INT`。
- 每个 PR：全模块 `mvn test` 绿 + subagent 评审 + cherry-pick 回 `jira/fix-paimon-connector-types`。

## 7. Boundaries

- **Always**: 既有已识别类型（含 `DECIMAL` 现行为）零行为变更；一议题一分支，cherry-pick 回基线；全量测试绿后才合入。
- **Ask first**: 修改 `DECIMAL` 既有语义（如让 `DECIMAL(10)` 单参合法化、让 DECIMAL 也参与分档）；给 default→STRING 增加日志；扩展非数值别名。
- **Never**: 不加 tapType 兜底（已裁决排除）；不改 `toInternalValue` 写入路径语义；不动 spec.json `dataTypes`；不迁移/重写存量 STRING 列（Paimon 无法原地改列类型，存量表由 reconciliation Spec 的 conflict 检测兜底，客户侧补救 = 重建表）。

## 8. Success Criteria

1. `NUMERIC(p,0)`/`NUMBER(p,0)` 按分档落地：p=10/19/38 三档用例分别断言 INT/BIGINT/DECIMAL(38,0)（测试先行，当前代码必红）。
2. `INT2/INT4/INT8/MEDIUMINT` 与整型 `UNSIGNED/ZEROFILL` 后缀提档全部按表映射。
3. 已识别类型行为零变更：全模块既有测试全绿，`DECIMAL(10,0)` 等回归用例逐条通过。
4. 客户场景端到端成立：MSSQL 数值整数列（`NUMERIC(...)`）自动建表得到数值列，写入 `Integer`/`Long` 值成功（严格转换路径不变）。

## 9. Open Questions

- **Q1（建议纳入，待裁决）**: 剩余未知串 default→STRING 维持静默（本次范围裁决），是否顺带加一行 `log.warn`（打印原始串与落点 STRING，零行为变更、纯可观测性）？本次排查耗时长的根因正是静默降级。
- **Q2（已闭环）**: 客户源端串已取证 = `int(11)`，当前分支已正确映射（scratch 测试实证）。剩余取证项：TapData MSSQL 连接器是否会为某些列下发 `unsigned` 后缀或 `NUMERIC(...)`——决定本加固 Spec 的实际触发面，但不影响规则设计。
- **Q3**: 分档阈值 10/19 沿用 spec.json 约定，属"乐观档"：`NUMERIC(10,0)` 的极值 9,999,999,999 实际超出 INT 上限 2,147,483,647，超界值会在写入时以 `PAIMON_VALUE_CONVERSION_FAILED` 显式报错（严格转换、不截断）。若希望建表期即杜绝溢出可能，可改保守档 9/18（p≤9→INT、p≤18→BIGINT）。默认按已批准的 10/19 实施。
- **Q4**: `DECIMAL(10)` 单参目前抛"expected precision and scale"，而新别名 `NUMERIC(10)` 合法——不对称是"已识别类型零变更"红线的代价，后续是否统一放宽另行裁决。
