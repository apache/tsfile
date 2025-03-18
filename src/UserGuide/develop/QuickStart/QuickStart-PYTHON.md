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
# Quick Start

## Dependencies

If want to compile on your local envirment, the dependencies are below:

- CMake >=3.11
- Maven >=3.9.6
- GCC >=4.8.5
- Make >=4.3
- cython >= 3.0.10
- numpy >= 1.26.4
- pandas >= 2.2.2
- setuptools >= 70.0.0

or use pip install, the dependencies are:

numpy >= 1.26.4
pandas >= 2.2.2

## Installation Method

### Compile on your local envirment
Clone the source code from git:

```shell
git clone https://github.com/apache/tsfile.git
```
Run Maven to compile in the TsFile root directory:

```shell
mvn clean install -P with-python -DskipTests
```

### Install into your local envirment

run pip install to get tsfile package.

```bash
pip install tsfile
```

### Directory Structure

• **wheel**: Located at `tsfile/python/dist`, you can use pip to install this wheel.

## Writing Process

table_data_dir = os.path.join(os.path.dirname(__file__), "table_model.tsfile")
if os.path.exists(table_data_dir):
    os.remove(table_data_dir)

column1 = ColumnSchema("id", TSDataType.STRING, ColumnCategory.TAG)
column2 = ColumnSchema("id2", TSDataType.STRING, ColumnCategory.TAG)
column3 = ColumnSchema("value", TSDataType.FLOAT, ColumnCategory.FIELD)

### Free resource automatically

```Python
with TsFileWriter(table_data_dir) as writer:
    writer.register_table(TableSchema("test_table", [column1, column2, column3]))
    tablet_row_num = 100
    tablet = Tablet("test_table",
                    ["id1", "id2", "value"],
                    [TSDataType.STRING, TSDataType.STRING, TSDataType.FLOAT],
                    [ColumnCategory.TAG, ColumnCategory.TAG, ColumnCategory.FIELD],
                    tablet_row_num)

    for i in range(tablet_row_num):
        tablet.add_timestamp(i, i * 10)
        tablet.add_value_by_name("id1", i, "test1")
        tablet.add_value_by_name("id2", i, "test" + str(i))
        tablet.add_value_by_index(2, i, i * 100.2)

    writer.write_table(tablet)
```
