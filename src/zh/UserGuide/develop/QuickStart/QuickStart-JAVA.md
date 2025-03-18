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
# 快速上手

## 依赖

- JDK >=1.8
- Maven >=3.6

## 安装

从git克隆源代码:

```shell
git clone https://github.com/apache/tsfile.git
```
在 TsFile 根目录下执行 maven 编译:

```shell
mvn clean install -P with-java -DskipTests
```

在 Maven 中引用 TsFile API:

```shell
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>2.0.1</version>
</dependency>
```

## 写入示例

### 构造 ITsFileWriter

```shell
String path = "test.tsfile";
File f = FSFactoryProducer.getFSFactory().getFile(path);

String tableName = "table1";

TableSchema tableSchema =
        new TableSchema(
                tableName,
                Arrays.asList(
                        new ColumnSchemaBuilder()
                                .name("id1")
                                .dataType(TSDataType.STRING)
                                .category(Tablet.ColumnCategory.TAG)
                                .build(),
                        new ColumnSchemaBuilder()
                                .name("id2")
                                .dataType(TSDataType.STRING)
                                .category(Tablet.ColumnCategory.TAG)
                                .build(),
                        new ColumnSchemaBuilder()
                                .name("s1")
                                .dataType(TSDataType.INT32)
                                .category(Tablet.ColumnCategory.FIELD)
                                .build(),
                        new ColumnSchemaBuilder()
                                .name("s2").
                                dataType(TSDataType.BOOLEAN)
                                .build()));

long memoryThreshold = 10 * 1024 * 1024;

ITsFileWriter writer =
             new TsFileWriterBuilder()
                     .file(f)
                     .tableSchema(tableSchema)
                     .memoryThreshold(memoryThreshold)
                     .build();
```

### 写入数据

```shell
Tablet tablet =
        new Tablet(
                Arrays.asList("id1", "id2", "s1", "s2"),
                Arrays.asList(
                        TSDataType.STRING, TSDataType.STRING, TSDataType.INT32, TSDataType.BOOLEAN));

for (int row = 0; row < 5; row++) {
    long timestamp = row;
    tablet.addTimestamp(row, timestamp);
    tablet.addValue(row, "id1", "id1_filed_1");
    tablet.addValue(row, "id2", "id2_filed_1");
    tablet.addValue(row, "s1", row);
    tablet.addValue(row, "s2", true);
}

writer.write(tablet);
```

### 关闭文件

```shell
writer.close();
```

### 示例代码

使用这些接口的示例代码位于：<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/v4/WriteTabletWithITsFileWriter.java>

## 查询示例

### 构造 ITsFileReader

```shell
String path = "test.tsfile";
File f = FSFactoryProducer.getFSFactory().getFile(path);

ITsFileReader reader = 
             new TsFileReaderBuilder()
                     .file(f)
                     .build();
```

### 构建查询请求

```shell
ResultSet resultSet = reader.query(tableName, Arrays.asList("id1", "id2", "s1", "s2"), 2, 8)
```

### 查询数据

```shell
ResultSetMetadata metadata = resultSet.getMetadata();
System.out.println(metadata);

StringJoiner sj = new StringJoiner(" ");
for (int column = 1; column <= 5; column++) {
    sj.add(metadata.getColumnName(column) + "(" + metadata.getColumnType(column) + ") ");
}
System.out.println(sj.toString());

while (resultSet.next()) {
    Long timeField = resultSet.getLong("Time");
    String id1Field = resultSet.isNull("id1") ? null : resultSet.getString("id1");
    String id2Field = resultSet.isNull("id2") ? null : resultSet.getString("id2");
    Integer s1Field = resultSet.isNull("s1") ? null : resultSet.getInt(4);
    Boolean s2Field = resultSet.isNull("s2") ? null : resultSet.getBoolean(5);
    sj = new StringJoiner(" ");
    System.out.println(
            sj.add(timeField + "")
                    .add(id1Field)
                    .add(id2Field)
                    .add(s1Field + "")
                    .add(s2Field + "")
                    .toString());
}
```

### 关闭文件

```shell
reader.close();
```

### 示例代码

使用这些接口的示例代码位于：<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/v4/ITsFileReaderAndITsFileWriter.java>

