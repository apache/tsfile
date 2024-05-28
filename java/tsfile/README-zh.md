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
# TsFile Java Document
<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.0
             \/     \/                 \/  
</pre>

## 开发

### 前置条件

构建 Java 版的 TsFile，必须要安装以下依赖:

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

## 使用

### 在 Maven 中添加 TsFile 依赖

当前发布版本是 `1.0.0`，可以这样引用

```xml  
<dependencies>
    <dependency>
      <groupId>org.apache.tsfile</groupId>
      <artifactId>tsfile</artifactId>
      <version>1.0.0</version>
    </dependency>
<dependencies>
```

当前 SNAPSHOT 版本是 `1.0.1-SNAPSHOT`, 可以这样引用

```xml  
<dependencies>
    <dependency>
      <groupId>org.apache.tsfile</groupId>
      <artifactId>tsfile-java</artifactId>
      <version>1.0.1-SNAPSHOT</version>
    </dependency>
<dependencies>
```

### TsFile Java API

#### 写入 TsFile
TsFile 可以通过以下三个步骤生成，完整的代码参见"写入 TsFile 示例"章节。

1. 注册元数据 (Schema)

    创建一个`Schema`类的实例。
    
    `Schema`类保存的是一个映射关系，key 是一个 measurement 的名字，value 是 measurement schema.
    
    下面是一系列接口：
    
    ```java

    /**
     * measurementID: 物理量的名称，通常是传感器的名称
     * type: 数据类型，现在支持六种类型：`BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `TEXT`
     * encoding: 编码类型
     */
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) // 默认使用 LZ4 压缩算法

    // 使用预定义的 measurement 列表初始化 Schema
    public Schema(Map<String, MeasurementSchema> measurements)

    /** 
     * 构造 TsFileWriter 进行数据写入
     * file : 写入 TsFile 数据的文件
     * schema : 文件的 schemas
     */
    public TsFileWriter(File file, Schema schema) throws IOException
    ```

2. 使用 `TsFileWriter` 写入数据。
  
    ```java
    /**
     * 使用接口创建一个新的`TSRecord`（时间戳和设备）
     */
    public TSRecord(long timestamp, String deviceId)

    /**
     * 创建一个`DataPoint`（度量 (measurement) 和值的对应），并使用 addTuple 方法将数据 DataPoint 添加正确的值到 TsRecord。
     */
      for (IMeasurementSchema schema : schemas) {
        tsRecord.addTuple(
            DataPoint.getDataPoint(
                schema.getType(),
                schema.getMeasurementId(),
                Objects.requireNonNull(DataGenerator.generate(schema.getType(), (int) startValue))
                    .toString()));
        startValue++;
      }
    /**
     * 写入数据
     */
    public void write(TSRecord record) throws IOException, WriteProcessException
    ```

3. 调用`close`方法来关闭文件，关闭后才能进行查询。

    ```java
    public void close() throws IOException
    ```

写入 TsFile 完整示例

[构造 TSRecord 来写入数据](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTSRecord.java)。

[构造 Tablet 来写入数据](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTablet.java)。


#### 读取 TsFile

* 构造查询条件
```java
/**
 * 构造待读取的时间序列
 * 时间序列由 deviceId.measurementId 的格式组成（deviceId内可以有.）
 */
List<Path> paths = new ArrayList<Path>();
paths.add(new Path("device_1.sensor_1"));
paths.add(new Path("device_1.sensor_3"));

/**
 * 构造一个时间范围过滤条件 
 */
IExpression timeFilterExpr = BinaryExpression.and(
		new GlobalTimeExpression(TimeFilter.gtEq(15L)),
    new GlobalTimeExpression(TimeFilter.lt(25L))); // 15 <= time < 25

/**
 * 构造完整的查询表达式
 */
QueryExpression queryExpression = QueryExpression.create(paths, timeFilterExpr);
```

* 读取数据

```java
/**
 * 根据文件路径`filePath`构造一个`ReadOnlyTsFile`实例。
 */
TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);

/**
 * 查询数据
 */
public QueryDataSet query(QueryExpression queryExpression) throws IOException
```

读取 TsFile 完整示例

[查询数据](../examples/src/main/java/org/apache/tsfile/TsFileRead.java)

[全文件读取](../examples/src/main/java/org/apache/tsfile/TsFileSequenceRead.java)
