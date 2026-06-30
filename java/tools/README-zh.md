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

1. Java >= 1.8 (1.8, 11 到 17 都经过验证. 请确保设置了环境变量).
2. Maven >= 3.6 (如果要从源代码编译TsFile).


### 使用 maven 构建

```
mvn clean package -P with-java -DskipTests
```

### 安装到本地机器

```
mvn install -P with-java -DskipTests
```

## 混合 CSV 导入

将一条带真实时间列的主时序 CSV，与多条不含时间列的附属 CSV（列与主表 TAG/FIELD 相同）合并写入**单个** TsFile。附属行使用合成时间戳 `1, 2, …, N`（按文件内连续）；通过虚拟 TAG `batch_id` 隔离，每个附属文件对应一个 ChunkGroup（在同一业务 TAG 组合下）。

配置示例（`hybrid.conf`）：

```
output_tsfile=combined.tsfile
shared_schema=main.schema
main_csv=timeseries.csv
main_batch_id=main
batch_id_tag=batch_id
validate_uniform_tags=true
supplement_sort_by_variance=true
supplement_csv=experiment_1.csv
supplement_batch_id=experiment_1
supplement_csv=experiment_2.csv
supplement_batch_id=experiment_2
```

运行：

```sh
java -jar tsfile-tools.jar --hybrid_config hybrid.conf
```

附属 CSV 表头须包含 `shared_schema` 中除时间列外的全部业务 TAG 与 FIELD 列（例如 `Region,DeviceId,Temperature,Pressure`）。

对每个附属 CSV 单独处理（默认 `supplement_sort_by_variance=true`）：

1. 仅在该 CSV 内计算各 **FIELD** 列方差。
2. 按方差降序确定列排序优先级。
3. 对该 CSV 行做升序多键排序。
4. 写入一个 ChunkGroup；组内时间戳连续（`startId`, `startId+1`, …）。
5. 下一个附属文件从 `maxId + 1` 继续编号（file1: `1..n1`，file2: `n1+1..n1+n2`，…）。

编程接口：`HybridCsvTsFileAssembler.execute(HybridImportConfig)`。

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
- **source_columns**：映射源文件中的所有列，CSV 按位置对应，Parquet / Arrow 按列名匹配。使用 `SKIP` 跳过不需要的列。
- **FIELD**（推导结果，非配置项）：`source_columns` 中去掉 `time_column`、`tag_columns`、`SKIP` 后的剩余列，即为测点列，其值随时间变化。

### Schema 示例

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
Region TEXT,
FactoryNumber TEXT,
DeviceNumber TEXT,
SKIP,
SKIP,
Time INT64,
Temperature FLOAT,
Emission DOUBLE,
```

说明：
- `Group` 是虚拟标签列（不在 CSV 中），默认值为 `Datang`
- `Region`、`FactoryNumber`、`DeviceNumber` 是从 CSV 中读取的标签列
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
- 时间列：必须严格命名为 `time` 或 `TIME`（区分大小写）
- 其余所有列自动成为 FIELD（不自动推断标签列）
- CSV 类型推断基于前 100 行采样。提升规则：INT64 和 DOUBLE 混合提升为 DOUBLE；其他任何混合对（包括 BOOLEAN 与数字类型）直接提升为 STRING。
- Parquet / Arrow 直接使用原生 schema 类型
- 默认表名：从源文件名推导（如 `sensor.csv` → 表名 `sensor`）
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


