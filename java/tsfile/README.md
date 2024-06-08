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

## Building With Java

### Prerequisites

To build TsFile wirh Java, you need to have:

1. Java >= 1.8 (1.8, 11 to 17 are verified. Please make sure the environment path has been set accordingly).
2. Maven >= 3.6 (If you want to compile TsFile from source code).


### Build TsFile with Maven

```
mvn clean package -P with-java -DskipTests
```

### Install to local machine

```
mvn install -P with-java -DskipTests
```

## Use TsFile

### Add TsFile as a dependency in Maven

The current release version is `1.0.0`

```xml  
<dependencies>
    <dependency>
      <groupId>org.apache.tsfile</groupId>
      <artifactId>tsfile</artifactId>
      <version>1.0.0</version>
    </dependency>
<dependencies>
```

### TsFile Java API

#### Write TsFile
TsFile can be generated through the following three steps, and the complete code can be found in the "Write TsFile Example" section.

1. Register Schema

    you can make an instance of class `Schema` first and pass this to the constructor of class `TsFileWriter`
    
    The class `Schema` contains a map whose key is the name of one measurement schema, and the value is the schema itself.

    Here are the interfaces:
    
    ```java

    /**
     * measurementID: The name of this measurement, typically the name of the sensor
     * type: The data type, now support six types: `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `TEXT`
     * encoding: The data encoding
     */
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding) // default use LZ4 Compression

    // Initialize the schema using a predefined measurement list
    public Schema(Map<String, MeasurementSchema> measurements)

    /** 
     * construct TsFileWriter for write
     * file : The TsFile to write
     * schema : The file schemas
     */
    public TsFileWriter(File file, Schema schema) throws IOException
    ```

2. use `TsFileWriter` write data.
  
    ```java
    /**
     * Use this interface to create a new `TSRecord`(a timestamp and device pair)
     */
    public TSRecord(long timestamp, String deviceId)

    /**
     * Then create a `DataPoint`(a measurement and value pair), and use the addTuple method to add the DataPoint to the correct TsRecord.
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
     * write data
     */
    public void write(TSRecord record) throws IOException, WriteProcessException
    ```

3. call `close` to finish this writing process，Query can only be performed after close.

    ```java
    public void close() throws IOException
    ```

Write TsFile Example

[Construct TSRecord Write Data](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTSRecord.java)。

[Construct Tablet Write Data](../examples/src/main/java/org/apache/tsfile/TsFileWriteAlignedWithTablet.java)。


#### Read TsFile

* Construct Query Expression
```java
/**
 * Construct a time series to be read
 * The time series is composed of the format deviceId.measurementId (there can be.)
 */
List<Path> paths = new ArrayList<Path>();
paths.add(new Path("device_1.sensor_1"));
paths.add(new Path("device_1.sensor_3"));

/**
 * Construct Time Filter 
 */
IExpression timeFilterExpr = BinaryExpression.and(
		new GlobalTimeExpression(TimeFilter.gtEq(15L)),
    new GlobalTimeExpression(TimeFilter.lt(25L))); // 15 <= time < 25

/**
 * Construct Full Query Expression
 */
QueryExpression queryExpression = QueryExpression.create(paths, timeFilterExpr);
```

* Read Data

```java
/**
 * Construct an instance of 'ReadOnlyTsFile' based on the file path 'filePath'.
 */
TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);

/**
 * Query Data
 */
public QueryDataSet query(QueryExpression queryExpression) throws IOException
```

Read TsFile Example

[Read Data](../examples/src/main/java/org/apache/tsfile/TsFileRead.java)

[Sequence Read Data](../examples/src/main/java/org/apache/tsfile/TsFileSequenceRead.java)
