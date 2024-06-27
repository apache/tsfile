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

[English](./README.md) | [Chinese](./README-zh.md)
# TsFile Tools Manual
## Introduction

## Development

### Prerequisites

To build the Java version of TsFile Tools, you must have the following dependencies installed:

1. Java >= 1.8 (1.8, 11 to 17 are verified. Make sure the environment variable is set).
2. Maven >= 3.6 (if you are compiling TsFile from source).

### Build with Maven

```sh
mvn clean package -P with-java -DskipTests
```

### Install to local machine

```
mvn install -P with-java -DskipTests
```

## schema 定义
| Parameter             | Description                      | Required | Default  |
|----------------|--------------------------|----------|------|
| table_name     | Table name                       | Yes      |      |
| time_precision | Time precision (options: ms/us/ns) | No       | ms   |
| has_header     | Whether it contains a header (options: true/false) | No       | true |
| separator      | Delimiter (options: , /tab/ ;)    | No        | ,    |
| null_format    | Null value                       | No        |    |
| id_columns     | Primary key columns, supports columns not in the CSV as hierarchy     | No        |      |
| time_column    | Time column                      | Yes        |      |
| csv_columns    | Corresponding columns in the CSV in order            | Yes        |      |

Explanation:

id_columns are set in order and can include columns not in the CSV file as hierarchy.
For example, if the CSV has only columns a, b, c, d, time, then
id_columns
a1 default aa
a
Here, a1 is a virtual column with a default value of aa.

After csv_columns, the content defines the time series, with each time series on a new line. The first field on each line is the measurement name in the tsfile, and the second field is the type.
If a column does not need to be written into the tsfile, it can be set as SKIP.
SKIP will be ignored and not written into the tsfile.

Example:
csv_columns
Region TEXT,
Factory Number TEXT,
Device Number TEXT,
SKIP,
SKIP,
Time INT64,
Temperature FLOAT,
Emission DOUBLE,

Data Example
CSV file content:

### 数据示例

CSV file content:
```
Region,FactoryNumber,DeviceNumber,Model,MaintenanceCycle,Time,Temperature,Emission
hebei,1001, 1,10,1,1,80.0,1000.0
hebei,1001,1,10,1,4,80.0,1000.0
hebei,1002,7,5,2,1,90.0,1200.0
```
Schema definition

```
table_name=root.db1
time_precision=ms
time_format=ISO8601
has_header=true
aligned=true
separator=,
null_format=\N


id_columns
Group DEFAULT Datang
Region
FactoryNumber
DeviceNumber

time_column=Time

csv_columns
RegionTEXT,
FactoryNumber TEXT,
DeviceNumber TEXT,
SKIP,
SKIP,
Time INT64,
Temperature FLOAT,
Emission DOUBLE,
```
## Commands

```
csv2tsfile.sh --source ./xxx/xxx --target /xxx/xxx --fail_dir /xxx/xxx 
csv2tsfile.bat --source ./xxx/xxx --target /xxx/xxx --fail_dir /xxx/xxx 
```
