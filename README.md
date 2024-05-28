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

# TsFile Document
<pre>
___________    ___________.__.__          
\__    ___/____\_   _____/|__|  |   ____  
  |    | /  ___/|    __)  |  |  | _/ __ \ 
  |    | \___ \ |     \   |  |  |_\  ___/ 
  |____|/____  >\___  /   |__|____/\___  >  version 1.0.1-SNAPSHOT
             \/     \/                 \/  
</pre>
[![Maven Version](https://maven-badges.herokuapp.com/maven-central/org.apache.tsfile/tsfile-parent/badge.svg)](http://search.maven.org/#search|gav|1|g:"org.apache.tsfile")

## Abstract

TsFile is a columnar storage file format designed for time series data, which supports efficient compression, high throughput of read and write, and compatibility with various frameworks, such as Spark and Flink. It is easy to integrate TsFile into IoT big data processing frameworks.

[Click for More Information](https://www.timecho.com/archives/tian-bu-shi-chang-kong-bai-apache-tsfile-ru-he-chong-xin-ding-yi-shi-xu-shu-ju-guan-li)

## Motivation

Time series data is becoming increasingly important in a wide range of applications, including IoT, intelligent control, finance, log analysis, and monitoring systems. 

TsFile is the first existing standard file format for time series data. The industry companies usually write time series data without unification, or use general columnar file format, which makes data collection and processing complicated without a standard. With TsFile, organizations could write data in TsFile inside end devices or gateway, then transfer TsFile to the cloud for unified management in IoTDB and other systems. In this way, we lower the network transmission and the computing resource consumption in the cloud.

TsFile is a specially designed file format rather than a database. Users can open, write, read, and close a TsFile easily like doing operations on a normal file. Besides, more interfaces are available on a TsFile.

TsFile offers several distinctive features and benefits:

* Efficient Storage and Compression: TsFile employs advanced compression techniques to minimize storage requirements, resulting in reduced disk space consumption and improved system efficiency. 

* Flexible Schema and Metadata Management: TsFile allows for directly write data without pre defining the schema, which is flexible for data aquisition. 

* High Query Performance with time range: TsFile has indexed devices, sensors and time dimensions to accelerate query performance, enabling fast filtering and retrieval of time series data. 

* Seamless Integration: TsFile is designed to seamlessly integrate with existing time series databases such as IoTDB, data processing frameworks, such as Spark and Flink. 


# Features

When conceptualizing the structure of TsFile, there were several key considerations:

- Efficient Compression: Recognizing the importance of space optimization, TsFile compresses data extensively to minimize storage requirements.

- Device Packing: Multiple devices are packed together to reduce the number of files, streamlining data management.

- Data Locality: Time series data expected to be queried together are kept close in physical locations to enhance query performance.

- Disk Fragmentation: TsFile ensures data is packed with sizes aligned with file systems to avoid disk fragmentation.

- Efficient Access: With millions of time series needing efficient access, TsFile is optimized for rapid data retrieval.

# Columnar Storage and File Structure

TsFile adopts a columnar storage design, similar to other file formats, primarily to optimize time-series data's storage efficiency and query performance. This design aligns with the nature of time series data, which often involves large volumes of similar data types recorded over time. However, TsFile was developed particularly with a structure of page, chunk, chunk group, block, and index:

- Page: The basic unit for storing time series data, sorted by time in ascending order with separate columns for timestamps and values.

- Chunk: Comprising metadata headers and several pages, each chunk belongs to one time series, with variable sizes allowing for different compression and encoding methods.

- Chunk Group: Multiple chunks within a chunk group belong to one or multiple series of a device written in the same period, facilitating efficient query processing.

- Block: Buffered in memory before being flushed to TsFile, all chunk groups form a block, allowing for efficient data locality in distributed file systems like HDFS.

- Index: The file metadata at the end of TsFile contains a chunk-level index and file-level statistics for efficient data access.

The following diagram illustrates TsFile's innovative columnar storage design, showcasing the efficiency of its page, chunk, and block structure.



![TsFile Architecture](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE3-20240315.png)

# Encoding and Compression Techniques
TsFile employs advanced encoding and compression techniques to optimize storage and access for time series data. It uses methods like run-length encoding (RLE), bit-packing, and Snappy for efficient compression, allowing separate encoding of timestamp and value columns for better data processing. Its unique encoding algorithms are designed specifically for the characteristics of time series data in IoT scenarios, focusing on regular time intervals and the correlation among series. Additionally, TsFile incorporates frequency domain encoding, utilizing quantization and bit-width reduction to efficiently store frequency domain data for reuse, ensuring space efficiency without compromising data accuracy.

The table below compares 3 file formats in different dimensions.

(![TsFile, Parquet and ORC in Comparison](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE4-20240315.png))


Its development facilitates efficient data encoding, compression, and access, reflecting a deep understanding of industry needs, pioneering a path toward efficient, scalable, and flexible data analytics platforms.

# Building With Java

## Prerequisites

To build TsFile wirh Java, you need to have:

1. Java >= 1.8 (1.8, 11 to 17 are verified. Please make sure the environment path has been set accordingly).
2. Maven >= 3.6 (If you want to compile TsFile from source code).


## Build TsFile with Maven

```
mvn clean package -P with-java -DskipTests
```

## Install to local machine

```
mvn install -P with-java -DskipTests
```

# Add TsFile as a dependency in Maven

The current SNAPSHOT version is `1.0.1-SNAPSHOT`, you can use it after Maven install

```xml  
<dependencies>
    <dependency>
      <groupId>org.apache.tsfile</groupId>
      <artifactId>tsfile-java</artifactId>
      <version>1.0.1-SNAPSHOT</version>
    </dependency>
<dependencies>
```

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

# TsFile Java API

## Write TsFile

1. construct a `TsFileWriter` instance.
    * Without pre-defined schema
        
    ```java
    public TsFileWriter(File file) throws IOException
    ```
    * With pre-defined schema

    ```java
    public TsFileWriter(File file, Schema schema) throws IOException
    ```
    This one is for using the HDFS file system. `TsFileOutput` can be an instance of class `HDFSOutput`.

    ```java
    public TsFileWriter(TsFileOutput output, Schema schema) throws IOException 
    ```

    If you want to set some TSFile configuration on your own, you could use param `config`. For example:

    ```java
    TSFileConfig conf = new TSFileConfig();
    conf.setTSFileStorageFs("HDFS");
    TsFileWriter tsFileWriter = new TsFileWriter(file, schema, conf);
    ```

    In this example, data files will be stored in HDFS, instead of local file system. If you'd like to store data files in local file system, you can use `conf.setTSFileStorageFs("LOCAL")`, which is also the default config.

    You can also config the ip and rpc port of your HDFS by `config.setHdfsIp(...)` and `config.setHdfsPort(...)`. The default ip is `localhost` and default rpc port is `9000`.

    **Parameters:**

    * file : The TsFile to write

    * schema : The file schemas, will be introduced in next part.

    * config : The config of TsFile.
2. add measurements
  
    Or you can make an instance of class `Schema` first and pass this to the constructor of class `TsFileWriter`
    
    The class `Schema` contains a map whose key is the name of one measurement schema, and the value is the schema itself.
    
    Here are the interfaces:

    ```java
    // Create an empty Schema or from an existing map
    public Schema()
    public Schema(Map<String, MeasurementSchema> measurements)
    // Use this two interfaces to add measurements
    public void registerMeasurement(MeasurementSchema descriptor)
    public void registerMeasurements(Map<String, MeasurementSchema> measurements)
    // Some useful getter and checker
    public TSDataType getMeasurementDataType(String measurementId)
    public MeasurementSchema getMeasurementSchema(String measurementId)
    public Map<String, MeasurementSchema> getAllMeasurementSchema()
    public boolean hasMeasurement(String measurementId)
    ```

    You can always use the following interface in `TsFileWriter` class to add additional measurements: 

    ```java
    public void addMeasurement(MeasurementSchema measurementSchema) throws WriteProcessException
    ```

    The class `MeasurementSchema` contains the information of one measurement, there are several constructors:
    ```java
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding)
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType)
    public MeasurementSchema(String measurementId, TSDataType type, TSEncoding encoding, CompressionType compressionType, 
    Map<String, String> props)
    ```
    
    **Parameters:**
    ​    
    * measurementID: The name of this measurement, typically the name of the sensor.
      
    * type: The data type, now support six types: `BOOLEAN`, `INT32`, `INT64`, `FLOAT`, `DOUBLE`, `TEXT`;
    
    * encoding: The data encoding. 
    
    * compression: The data compression. 

    * props: Properties for special data types.Such as `max_point_number` for `FLOAT` and `DOUBLE`, `max_string_length` for
    `TEXT`. Use as string pairs into a map such as ("max_point_number", "3").
    
    > **Notice:** Although one measurement name can be used in multiple deltaObjects, the properties cannot be changed. I.e. 
        it's not allowed to add one measurement name for multiple times with different type or encoding.
        Here is a bad example:

    ```java
    // The measurement "sensor_1" is float type
    addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    
    // This call will throw a WriteProcessException exception
  addMeasurement(new MeasurementSchema("sensor_1", TSDataType.INT32, TSEncoding.RLE));
  ```
  ```

  ```

3. insert and write data continually.
  
    Use this interface to create a new `TSRecord`(a timestamp and device pair).
    
    ```java
    public TSRecord(long timestamp, String deviceId)
  ```
  ```
    Then create a `DataPoint`(a measurement and value pair), and use the addTuple method to add the DataPoint to the correct
    TsRecord.
    
    Use this method to write
    
    ```java
    public void write(TSRecord record) throws IOException, WriteProcessException
  ```

4. call `close` to finish this writing process. 
  
    ```java
    public void close() throws IOException
    ```

We are also able to write data into a closed TsFile.

1. Use `ForceAppendTsFileWriter` to open a closed file.

	```java
	public ForceAppendTsFileWriter(File file) throws IOException
	```

2. call `doTruncate` truncate the part of Metadata

3. Then use `ForceAppendTsFileWriter` to construct a new `TsFileWriter`

```java
public TsFileWriter(TsFileIOWriter fileWriter) throws IOException
```
Please note, we should redo the step of adding measurements before writing new data to the TsFile.

### Example

You could write a TsFile by constructing **TSRecord** if you have the **non-aligned** (e.g. not all sensors contain values) time series data.

A more thorough example can be found at `java/examples/src/main/java/org/apache/tsfile/tsfile/TsFileWriteWithTSRecord.java`

You could write a TsFile by constructing **Tablet** if you have the **aligned** time series data.

A more thorough example can be found at `java/examples/src/main/java/org/apache/tsfile/tsfile/TsFileWriteWithTablet.java`

You could write data into a closed TsFile by using **ForceAppendTsFileWriter**.

A more thorough example can be found at `java/examples/src/main/java/org/apache/tsfile/tsfile/TsFileForceAppendWrite.java`

## Interface for Reading TsFile

* Definition of Path

A path is a dot-separated string which uniquely identifies a time-series in TsFile, e.g., "root.area_1.device_1.sensor_1". 
The last section "sensor_1" is called "measurementId" while the remaining parts "root.area_1.device_1" is called deviceId. 
As mentioned above, the same measurement in different devices has the same data type and encoding, and devices are also unique.

In read interfaces, The parameter `paths` indicates the measurements to be selected.

Path instance can be easily constructed through the class `Path`. For example:

```java
Path p = new Path("device_1.sensor_1");
```

We will pass an ArrayList of paths for final query call to support multiple paths.

```java
List<Path> paths = new ArrayList<Path>();
paths.add(new Path("device_1.sensor_1"));
paths.add(new Path("device_1.sensor_3"));
```

> **Notice:** When constructing a Path, the format of the parameter should be a dot-separated string, the last part will
 be recognized as measurementId while the remaining parts will be recognized as deviceId.


* Definition of Filter

 * Usage Scenario
Filter is used in TsFile reading process to select data satisfying one or more given condition(s). 

 * IExpression
The `IExpression` is a filter expression interface and it will be passed to our final query call.
We create one or more filter expressions and may use binary filter operators to link them to our final expression.

* **Create a Filter Expression**
  
    There are two types of filters.
    
     * TimeFilter: A filter for `time` in time-series data.
        ```
        IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter);
        ```
        Use the following relationships to get a `TimeFilter` object (value is a long int variable).
        
        |Relationship|Description|
        |---|---|
        |TimeFilter.eq(value)|Choose the time equal to the value|
        |TimeFilter.lt(value)|Choose the time less than the value|
        |TimeFilter.gt(value)|Choose the time greater than the value|
        |TimeFilter.ltEq(value)|Choose the time less than or equal to the value|
        |TimeFilter.gtEq(value)|Choose the time greater than or equal to the value|
        |TimeFilter.notEq(value)|Choose the time not equal to the value|
        |TimeFilter.not(TimeFilter)|Choose the time not satisfy another TimeFilter|
       
     * ValueFilter: A filter for `value` in time-series data.
       
        ```
        IExpression valueFilterExpr = new SingleSeriesExpression(Path, ValueFilter);
        ```
        The usage of  `ValueFilter` is the same as using `TimeFilter`, just to make sure that the type of the value
        equal to the measurement's(defined in the path).
    
* **Binary Filter Operators**

    Binary filter operators can be used to link two single expressions.

     * BinaryExpression.and(Expression, Expression): Choose the value satisfy for both expressions.
     * BinaryExpression.or(Expression, Expression): Choose the value satisfy for at least one expression.
    

Filter Expression Examples

* **TimeFilterExpression Examples**

    ```java
    IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.eq(15)); // series time = 15
    ```
```
    ```java
    IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.ltEq(15)); // series time <= 15
```
```java
    IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.lt(15)); // series time < 15
```
    ```java
IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.gtEq(15)); // series time >= 15
    ```
    ```java
    IExpression timeFilterExpr = new GlobalTimeExpression(TimeFilter.notEq(15)); // series time != 15
```
    ```java
    IExpression timeFilterExpr = BinaryExpression.and(
        new GlobalTimeExpression(TimeFilter.gtEq(15L)),
    new GlobalTimeExpression(TimeFilter.lt(25L))); // 15 <= series time < 25
```
    ```java
    IExpression timeFilterExpr = BinaryExpression.or(
        new GlobalTimeExpression(TimeFilter.gtEq(15L)),
        new GlobalTimeExpression(TimeFilter.lt(25L))); // series time >= 15 or series time < 25
    ```
* Read Interface

First, we open the TsFile and get a `ReadOnlyTsFile` instance from a file path string `path`.

```java
TsFileSequenceReader reader = new TsFileSequenceReader(path);
   
ReadOnlyTsFile readTsFile = new ReadOnlyTsFile(reader);
```
Next, we prepare the path array and query expression, then get final `QueryExpression` object by this interface:

```java
QueryExpression queryExpression = QueryExpression.create(paths, statement);
```

The ReadOnlyTsFile class has two `query` method to perform a query.
* **Method 1**

    ```java
    public QueryDataSet query(QueryExpression queryExpression) throws IOException
    ```

* **Method 2**

    ```java
    public QueryDataSet query(QueryExpression queryExpression, long partitionStartOffset, long partitionEndOffset) throws IOException
    ```

    This method is designed for advanced applications such as the TsFile-Spark Connector.

    * **params** : For method 2, two additional parameters are added to support partial query:
        *  ```partitionStartOffset```: start offset for a TsFile
        *  ```partitionEndOffset```: end offset for a TsFile

        > **What is Partial Query ?**
        >
        > In some distributed file systems(e.g. HDFS), a file is split into severval parts which are called "Blocks" and stored in different nodes. Executing a query paralleled in each nodes involved makes better efficiency. Thus Partial Query is needed. Paritial Query only selects the results stored in the part split by ```QueryConstant.PARTITION_START_OFFSET``` and ```QueryConstant.PARTITION_END_OFFSET``` for a TsFile.

* QueryDataset Interface

The query performed above will return a `QueryDataset` object.

Here's the useful interfaces for user.

  * `bool hasNext();`

    Return true if this dataset still has elements.
  * `List<Path> getPaths()`

    Get the paths in this data set.
  * `List<TSDataType> getDataTypes();` 

   Get the data types. The class TSDataType is an enum class, the value will be one of the following:

       BOOLEAN,
       INT32,
       INT64,
       FLOAT,
       DOUBLE,
       TEXT;
 * `RowRecord next() throws IOException;`

    Get the next record.
    
    The class `RowRecord` consists of a `long` timestamp and a `List<Field>` for data in different sensors,
     we can use two getter methods to get them.
    
    ```java
    long getTimestamp();
    List<Field> getFields();
    ```
    
    To get data from one Field, use these methods:
    
    ```java
    TSDataType getDataType();
    Object getObjectValue();
    ```



### Example


You should install TsFile to your local maven repository.


A more thorough example with query statement can be found at 
`java/examples/src/main/java/org/apache/tsfile/TsFileRead.java`
`java/examples/src/main/java/org/apache/tsfile/TsFileSequenceRead.java`