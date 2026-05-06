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
# Quick Start - Python

## Dependencies and Installation

### Install From Pypi

1. Platform support:

| Platform      | python                           |
| ------------- | -------------------------------- |
| Linux_x86_64  | py39, py310, py311, py312, py313 |
| Linux_aarch64 | py39, py310, py311, py312, py313 |
| MacOS_arm64   | py39, py310, py311, py312, py313 |
| MacOS_X86_64  | py39, py310, py311, py312, py313 |
| Win_amd64     | py39, py310, py311, py312, py313 |

2. Dependencies:

   * numpy >= 1.26.4
   * pandas >= 2.2.2

3. Use pip to install the latest version from pypi:

```shell
pip install tsfile
```

### Install From Wheel File

1. Download wheel from pypi: https://pypi.org/project/tsfile/#files
2. Install the wheel file using the pip install command.
```bash
pip install tsfile.wheel
```

### Install From Source Code Compilation
1. Dependencies

   - CMake >=3.11
   - Maven >=3.9.6
   - GCC >=4.8.5
   - Make >=4.3
   - cython >= 3.0.10
   - numpy >= 1.26.4
   - pandas >= 2.2.2
   - setuptools >= 70.0.0

2. Installation steps

* Clone the source code from git:

```shell
git clone https://github.com/apache/tsfile.git
```
* Run Maven to compile in the TsFile root directory:

```shell
mvn clean install -P with-python -DskipTests
```

* If Maven is not installed, you can compile tsfile using the following command:

  * Linux or Macos:
    ```shell
    mvnw clean install -P with-python -DskipTests
    ```
  * Windows:
    ```shell
    mvnw.cmd clean install -P with-python -DskipTests
    ```
* After successful compilation, the wheel file will be located in the `tsfile/python/dist` directory and can be installed locally using the pip install command (assuming its name is `tsfile.wheel`).
```bash
pip install tsfile.wheel
```


## Writing Process

```Python
import os
from tsfile import *

table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
if os.path.exists(table_data_dir):
    os.remove(table_data_dir)

column1 = ColumnSchema("id", TSDataType.STRING, ColumnCategory.TAG)
column2 = ColumnSchema("id2", TSDataType.STRING, ColumnCategory.TAG)
column3 = ColumnSchema("value", TSDataType.DOUBLE, ColumnCategory.FIELD)
table_schema = TableSchema("test_table", columns=[column1, column2, column3])


### Free resource automatically
with TsFileTableWriter(table_data_dir, table_schema) as writer:
    tablet_row_num = 100
    tablet = Tablet(
                    ["id", "id2", "value"],
                    [TSDataType.STRING, TSDataType.STRING, TSDataType.DOUBLE],
                    tablet_row_num)

    for i in range(tablet_row_num):
        tablet.add_timestamp(i, i * 10)
        tablet.add_value_by_name("id", i, "test1")
        tablet.add_value_by_name("id2", i, "test" + str(i))
        tablet.add_value_by_index(2, i, i * 100.2)

    writer.write_table(tablet)
```

## Reading Process

```Python
import os
from tsfile import * 

table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
### Free resource automatically
with TsFileReader(table_data_dir) as reader:
    print(reader.get_all_table_schemas())
    with reader.query_table("test_table", ["id2", "value"], 0, 50) as result:
        print(result.get_metadata())
        while result.next():
            print(result.get_value_by_name("id2"))
            print(result.get_value_by_name("value"))
            print(result.read_data_frame())
```

Use `to_dataframe` to read tsfile as dataframe.

```Python
import os
import tsfile as ts
table_data_dir = os.path.join(os.path.dirname(__file__), "table_data.tsfile")
print(ts.to_dataframe(table_data_dir))
```

`TsFileDataFrame` allows you to read time series data from TsFile just like operating a DataFrame, without worrying about the underlying file format and data loading details.

```Python
from iotdb_ai import TsFileDataFrame

df = TsFileDataFrame("data/")                # Load all TsFiles in the directory
                                            # Browse all time series

ts = df["weather.Beijing.humidity"]         # Retrieve a single time series
window = ts[20:100]                         # Slice by row index -> np.ndarray

data = df.loc[start:end, [                  # Align multiple time series by timestamp
    "weather.Beijing.temperature",
    "weather.Beijing.humidity",
]]
data.values                                  # -> np.ndarray, shape=(N, 2)
```

## Sample Code

The sample code of using these interfaces is in：https://github.com/apache/tsfile/blob/develop/python/examples/example.py

> Note: The above read/write examples are all based on the table model interface. For details about the interface definition, please refer to [Python Interface Definition](./InterfaceDefinition/InterfaceDefinition-Python.md). If you need information regarding the tree model, please contact us.

