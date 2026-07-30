<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

[English](./README.md) | [中文](./README-zh.md)
# TsFile Tools 手册
## 简介

## 开发

### 前置条件

构建 Java 版的 TsFile Tools，必须要安装以下依赖:

1. Java >= 17. 请确保设置了环境变量.
2. Maven >= 3.6 (如果要从源代码编译TsFile).


### 使用 maven 构建

```
mvn clean package -P with-java -DskipTests
```

### 安装到本地机器

```
mvn install -P with-java -DskipTests
```

## schema 定义

### 参数

| 参数 | 说明 | 是否必填 | 默认值 |
|------|------|---------|--------|
| table_name | 表名 | 是 | |
| time_precision | 时间精度（ms / us / ns / s） | 否 | ms |
| has_header | CSV 是否包含表头（true / false），Parquet / Arrow 忽略此项 | 否 | true |
| separator | CSV 行内分隔符（, / tab / ;），Parquet / Arrow 忽略此项 | 否 | , |
| null_format | CSV 中视为 null 的字符串，Parquet / Arrow 忽略此项（使用原生 null） | 否 | |
| tag_columns | 标签列（设备标识 / 联合主键），支持 DEFAULT 虚拟列 | 否 | |
| time_column | 时间列名称 | 是 | |
| source_columns | 源文件列定义，映射源文件中的每一列 | 是 | |

> **向后兼容**：`id_columns` 和 `csv_columns` 仍然可用，分别作为 `tag_columns` 和 `source_columns` 的别名。

### 列概念

- **time_column**：每个表有且仅有一个时间列，写入 TsFile 后列名固定为 `time`，类型为 `TIMESTAMP`。
- **tag_columns**：设备标识列（联合主键），可以为 0 到多个。支持通过 `DEFAULT` 关键字定义不在源文件中的虚拟列。
  - **数据类型固定为 `STRING`**，不可指定其他类型。即使在 `source_columns` 中为 TAG 列声明了类型，也会被忽略；推荐在 `source_columns` 里 TAG 列只写名字，不写类型。
- **source_columns**：映射源文件中的所有列，CSV 按位置对应，Parquet / Arrow 按列名匹配。使用 `SKIP` 跳过不需要的列。
- **FIELD**（推导结果，非配置项）：`source_columns` 中去掉 `time_column`、`tag_columns`、`SKIP` 后的剩余列，即为测点列，其值随时间变化。

> **列名大小写**：TsFile 表模型下列名/表名大小写不敏感，统一以小写存储。`import.schema` 中无论写 `Time` / `TIME` / `time`，落盘与读取均为 `time`。

### Schema 示例

> 同一设备内重复时间戳不支持 —— tag 列值相同且时间戳相同的行无法写入。

CSV 文件内容：
```
Region,FactoryNumber,DeviceNumber,Model,MaintenanceCycle,Time,Temperature,Emission
hebei,1001,1,10,1,1,80.0,1000.0
hebei,1001,1,10,1,4,80.0,1000.0
hebei,1002,7,5,2,1,90.0,1200.0
```

Schema 文件（`import.schema`）：
```
table_name=root.db1
time_precision=ms
has_header=true
separator=,
null_format=\N

tag_columns
Group DEFAULT Datang
Region
FactoryNumber
DeviceNumber

time_column=Time

source_columns
Region,
FactoryNumber,
DeviceNumber,
SKIP,
SKIP,
Time INT64,
Temperature FLOAT,
Emission DOUBLE,
```

说明：
- `Group` 是虚拟标签列（不在 CSV 中），默认值为 `Datang`
- `Region`、`FactoryNumber`、`DeviceNumber` 是从 CSV 中读取的标签列，类型固定为 `STRING`，无需声明
- `Model` 和 `MaintenanceCycle` 通过 `SKIP` 跳过
- `Temperature` 和 `Emission` 自动推导为 FIELD 列

Parquet / Arrow 在 schema 模式下，`source_columns` 按列**名称**匹配而非位置。也支持命名 SKIP：
```
source_columns
Time INT64,
unused_col SKIP,
Temperature FLOAT,
Emission DOUBLE,
```

**Parquet / Arrow schema 模式校验规则**（强制校验，不通过则报错并将源文件移至 `--fail_dir`）：

- **列数必须严格一致**：`source_columns` 的条目数必须等于 Parquet / Arrow 文件的列数。不需要导入的列请使用 `SKIP`。
- **每个名称都必须存在于源文件中**：所有非 SKIP 列名和所有命名 SKIP 都必须能在文件列中找到。
- **不允许匿名 `SKIP`**：按名匹配下，单独的 `SKIP` 无法定位具体列,必须写成 `columnName SKIP`。

## 命令行参数

| 参数 | 说明 | 是否必填 | 默认值 |
|------|------|---------|--------|
| -s, --source | 输入文件或目录 | 是 | |
| -t, --target | 输出目录 | 是 | |
| --schema | Schema 文件路径，不传则进入 auto 模式 | 否 | |
| --fail_dir | 失败文件存放目录 | 否 | failed |
| --format | 源格式：csv / parquet / arrow，不传则按文件扩展名自动识别 | 否 | 自动识别 |
| --table_name | 表名覆盖（auto 模式） | 否 | 从文件名推导 |
| --time_precision | 时间精度覆盖（auto 模式）：ms / us / ns / s | 否 | ms |
| --separator | CSV 分隔符（auto 模式）：, / tab / ; | 否 | , |
| -b, --block_size | CSV 分块大小（如 256M、1G） | 否 | 256M |
| -tn, --thread_num | 并行处理线程数 | 否 | 8 |

## 模式

### Schema 模式

传入 `--schema` 文件，显式定义列映射、类型、标签列和时间列。

```sh
# CSV
csv2tsfile.sh --source ./data/csv --target ./output --fail_dir ./failed --schema ./schema/import.schema
csv2tsfile.bat --source .\data\csv --target .\output --fail_dir .\failed --schema .\schema\import.schema

# Parquet
parquet2tsfile.sh --source ./data/parquet --target ./output --fail_dir ./failed --schema ./schema/import.schema
parquet2tsfile.bat --source .\data\parquet --target .\output --fail_dir .\failed --schema .\schema\import.schema

# Arrow
arrow2tsfile.sh --source ./data/arrow --target ./output --fail_dir ./failed --schema ./schema/import.schema
arrow2tsfile.bat --source .\data\arrow --target .\output --fail_dir .\failed --schema .\schema\import.schema
```

### Auto 模式

不传 `--schema`，自动推断列类型并识别时间列。

**Auto 模式规则：**
- **时间列**：必须严格命名为 `time` 或 `TIME`（区分大小写）。
  - Parquet / Arrow：若源文件中存在多个 Timestamp 类型的列，**只有名为 `time` / `TIME` 的那一列被选为时间轴**;其余 Timestamp 列会作为 FIELD 列写入,类型为 `INT64`(原值保留,但 TIMESTAMP 语义丢失)。如需保留 TIMESTAMP 类型,请改用 schema 模式显式声明。
- 其余所有列自动成为 FIELD（不自动推断标签列）
- **CSV 类型推断** 基于每列前 100 行采样。每个非空单元格先归类到一个基础类型(BOOLEAN / INT64 / DOUBLE / STRING)。
  - 一列在采样里只出现一种基础类型时,该列就用该类型。
  - **当同一列里出现不同基础类型时**触发提升:INT64 + DOUBLE → DOUBLE;其他任何混合组合(包括 BOOLEAN 与任意数字类型)→ STRING。
- Parquet / Arrow 直接使用原生 schema 类型
- **默认表名**：从源文件名推导（如 `sensor.csv` → 表名 `sensor`）。清洗规则按顺序执行:
  1. 去掉文件扩展名(`.csv` / `.parquet` / `.arrow` / `.ipc` / `.feather`,无法匹配时去掉最后一个 `.` 之后的内容)。
  2. 只保留 ASCII 字母(`a–z`、`A–Z`)、数字(`0–9`)、下划线(`_`)和点(`.`),其余字符全部替换为 `_`。
  3. 连续的 `_` 合并为一个;去掉首尾的 `_`。
  4. 若结果为空,使用按格式区分的默认名:`csv_data` / `parquet_data` / `arrow_data`。
  5. 若结果以数字开头,前面补 `t_`(TsFile 表名不允许以数字开头)。
- 默认 null 识别（仅 CSV）：空单元格和 `\N`

**Auto 模式示例：**

CSV 文件（`sensor.csv`）：
```
time,temperature,humidity,status
1000,25.5,60.0,true
2000,26.1,55.3,false
3000,27.0,58.1,true
```

Auto 模式推断结果：
```
表名：       sensor        （从文件名推导）
时间列：     time
FIELD 列：   temperature DOUBLE, humidity DOUBLE, status BOOLEAN
标签列：     （无）
```

**命令：**
```sh
# CSV
csv2tsfile.sh --source ./data/csv --target ./output --fail_dir ./failed
csv2tsfile.bat --source .\data\csv --target .\output --fail_dir .\failed

# CSV 带可选参数
csv2tsfile.sh --source ./data/csv --target ./output --table_name my_table --separator tab --time_precision us

# Parquet
parquet2tsfile.sh --source ./data/parquet --target ./output --fail_dir ./failed
parquet2tsfile.bat --source .\data\parquet --target .\output --fail_dir .\failed

# Arrow（.arrow / .ipc / .feather）
arrow2tsfile.sh --source ./data/arrow --target ./output --fail_dir ./failed
arrow2tsfile.bat --source .\data\arrow --target .\output --fail_dir .\failed
```

### 输出文件命名

- 单批次：`{源文件名}.tsfile`
- 多批次：`{源文件名}_1.tsfile`、`{源文件名}_2.tsfile`、...
- 表名与输出文件名相互独立——表名来自 schema 或 `--table_name`，文件名来自源文件。

## 表点数统计工具

`tools\tsfile-table-point-count.bat` 用于检查完整 TsFile 是否包含表级点数统计属性，并在属性缺失时补写。每张表的点数是其所有 FIELD 列中非空值的总数，不统计 TAG 列和时间列。

设置 `JAVA_HOME` 后，传入且仅传入一个 TsFile 路径：

```bat
tools\tsfile-table-point-count.bat C:\data\example.tsfile
```

工具会输出以下状态之一：

| 状态 | 说明 |
|------|------|
| `UPDATED` | 已计算缺失的点数统计属性并写回文件。 |
| `ALREADY_PRESENT` | 每张表都已有有效的点数统计属性，未修改文件。 |
| `NO_TABLE` | 文件不包含表 Schema，例如仅含树模型数据的 TsFile，未修改文件。 |

仅当状态为 `UPDATED` 时，工具才会原地修改输入文件。工具先在源文件所在目录生成完整的临时文件，确保重写后的元数据落盘后再替换源文件。运行前请确保目录可写、磁盘空间足以存放临时副本，并备份重要文件。不存在的文件和未写完整的 TsFile 会被拒绝处理。
