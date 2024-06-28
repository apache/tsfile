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
# TsFile 快速上手

## 数据示例

![](https://alioss.timecho.com/docs/img/WX20240628-173452@2x.png)

## 安装方式

在 `pom.xml` 的 `dependencies`中添加以下内容

```shell
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>1.0.0</version>
</dependency>
```

## 写入流程

### 构造 TsFileWriter

```shell
File f = new File("test.tsfile");
TsFileWriter tsFileWriter = new TsFileWriter(f);
```

### 注册时间序列

```shell
List<MeasurementSchema> schema1 = new ArrayList<>();
schema1.add(new MeasurementSchema("电压", TSDataType.FLOAT));
schema1.add(new MeasurementSchema("电流", TSDataType.FLOAT));
tsFileWriter.registerTimeseries(new Path("太阳能板1"), schema1);

List<MeasurementSchema> schema2 = new ArrayList<>();
schema2.add(new MeasurementSchema("电压", TSDataType.FLOAT));
schema2.add(new MeasurementSchema("电流", TSDataType.FLOAT));
schema2.add(new MeasurementSchema("风速", TSDataType.FLOAT));
tsFileWriter.registerTimeseries(new Path("风机1"), schema2);
```

### 写入数据

```shell
TSRecord tsRecord = new TSRecord(1, "太阳能板1");
tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "电压", 1.1f));
tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "电流", 2.2f));
tsFileWriter.write(tsRecord);
```

### 关闭文件

```shell
tsFileWriter.close();
```

### 示例代码

<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/TsFileWriteWithTSRecord.java>

## 查询流程

### 构造 TsFileReader

```shell
TsFileSequenceReader reader = new TsFileSequenceReader(path);
TsFileReader tsFileReader = new TsFileReader(reader);
```

### 构造查询请求

```shell
ArrayList<Path> paths = new ArrayList<>();
paths.add(new Path("太阳能板1", "电压",true));
paths.add(new Path("太阳能板1", "电流",true));

IExpression timeFilter =
    BinaryExpression.and(
        new GlobalTimeExpression(TimeFilterApi.gtEq(4L)),
        new GlobalTimeExpression(TimeFilterApi.ltEq(10L)));

QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
```

### 查询数据

```shell
QueryDataSet queryDataSet = tsFileReader.query(queryExpression);
while (queryDataSet.hasNext()) {
  queryDataSet.next();
}
```

### 关闭文件

```shell
tsFileReader.cloFan 1se();
```

### 示例代码

<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/TsFileRead.java>