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

## Sample Data

![](https://alioss.timecho.com/docs/img/2024050517481.png)

## Installation Method

Add the following content to the `dependencies` in `pom.xml`

```shell
<dependency>
    <groupId>org.apache.tsfile</groupId>
    <artifactId>tsfile</artifactId>
    <version>1.0.0</version>
</dependency>
```

## Writing Process

### Construct TsFileWriter

```shell
File f = new File("test.tsfile");
TsFileWriter tsFileWriter = new TsFileWriter(f);
```

### Registration Time Series

```shell
List<MeasurementSchema> schema1 = new ArrayList<>();
schema1.add(new MeasurementSchema("voltage", TSDataType.FLOAT));
schema1.add(new MeasurementSchema("current", TSDataType.FLOAT));
tsFileWriter.registerTimeseries(new Path("Solar_panel_1"), schema1);

List<MeasurementSchema> schema2 = new ArrayList<>();
schema2.add(new MeasurementSchema("voltage", TSDataType.FLOAT));
schema2.add(new MeasurementSchema("current", TSDataType.FLOAT));
schema2.add(new MeasurementSchema("wind_speed", TSDataType.FLOAT));
tsFileWriter.registerTimeseries(new Path("Fan_1"), schema2);
```

### Write Data

```shell
TSRecord tsRecord = new TSRecord(1, "Solar_panel_1");
tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "voltage", 1.1f));
tsRecord.addTuple(DataPoint.getDataPoint(TSDataType.FLOAT, "current", 2.2f));
tsFileWriter.write(tsRecord);
```

### Close File

```shell
tsFileWriter.close();
```

### Sample Code

<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/TsFileWriteWithTSRecord.java>

## Query Process

### Construct TsFileReader

```shell
TsFileSequenceReader reader = new TsFileSequenceReader(path);
TsFileReader tsFileReader = new TsFileReader(reader);
```

### Construct Query Request

```shell
ArrayList<Path> paths = new ArrayList<>();
paths.add(new Path("Solar_panel_1", "voltage",true));
paths.add(new Path("Solar_panel_1", "current",true));

IExpression timeFilter =
    BinaryExpression.and(
        new GlobalTimeExpression(TimeFilterApi.gtEq(4L)),
        new GlobalTimeExpression(TimeFilterApi.ltEq(10L)));

QueryExpression queryExpression = QueryExpression.create(paths, timeFilter);
```

### Query Data

```shell
QueryDataSet queryDataSet = tsFileReader.query(queryExpression);
while (queryDataSet.hasNext()) {
  queryDataSet.next();
}
```

### Close File

```shell
tsFileReader.close();
```

### Sample Code

<https://github.com/apache/tsfile/blob/develop/java/examples/src/main/java/org/apache/tsfile/TsFileRead.java>

