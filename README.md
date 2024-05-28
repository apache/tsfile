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

[Click for More Information](https://www.timecho-global.com/archives/apache-tsfile-time-series-data-storage-redefined)

## Motivation

Time series data is becoming increasingly important in a wide range of applications, including IoT, intelligent control, finance, log analysis, and monitoring systems. 

TsFile is the first existing standard file format for time series data. The industry companies usually write time series data without unification, or use general columnar file format, which makes data collection and processing complicated without a standard. With TsFile, organizations could write data in TsFile inside end devices or gateway, then transfer TsFile to the cloud for unified management in IoTDB and other systems. In this way, we lower the network transmission and the computing resource consumption in the cloud.

TsFile is a specially designed file format rather than a database. Users can open, write, read, and close a TsFile easily like doing operations on a normal file. Besides, more interfaces are available on a TsFile.

TsFile offers several distinctive features and benefits:

* Efficient Storage and Compression: TsFile employs advanced compression techniques to minimize storage requirements, resulting in reduced disk space consumption and improved system efficiency. 

* Flexible Schema and Metadata Management: TsFile allows for directly write data without pre defining the schema, which is flexible for data aquisition. 

* High Query Performance with time range: TsFile has indexed devices, sensors and time dimensions to accelerate query performance, enabling fast filtering and retrieval of time series data. 

* Seamless Integration: TsFile is designed to seamlessly integrate with existing time series databases such as IoTDB, data processing frameworks, such as Spark and Flink. 


## Features

When conceptualizing the structure of TsFile, there were several key considerations:

- Efficient Compression: Recognizing the importance of space optimization, TsFile compresses data extensively to minimize storage requirements.

- Device Packing: Multiple devices are packed together to reduce the number of files, streamlining data management.

- Data Locality: Time series data expected to be queried together are kept close in physical locations to enhance query performance.

- Disk Fragmentation: TsFile ensures data is packed with sizes aligned with file systems to avoid disk fragmentation.

- Efficient Access: With millions of time series needing efficient access, TsFile is optimized for rapid data retrieval.

## Columnar Storage and File Structure

TsFile adopts a columnar storage design, similar to other file formats, primarily to optimize time-series data's storage efficiency and query performance. This design aligns with the nature of time series data, which often involves large volumes of similar data types recorded over time. However, TsFile was developed particularly with a structure of page, chunk, chunk group, block, and index:

- Page: The basic unit for storing time series data, sorted by time in ascending order with separate columns for timestamps and values.

- Chunk: Comprising metadata headers and several pages, each chunk belongs to one time series, with variable sizes allowing for different compression and encoding methods.

- Chunk Group: Multiple chunks within a chunk group belong to one or multiple series of a device written in the same period, facilitating efficient query processing.

- Block: Buffered in memory before being flushed to TsFile, all chunk groups form a block, allowing for efficient data locality in distributed file systems like HDFS.

- Index: The file metadata at the end of TsFile contains a chunk-level index and file-level statistics for efficient data access.

The following diagram illustrates TsFile's innovative columnar storage design, showcasing the efficiency of its page, chunk, and block structure.

![TsFile Architecture](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE3-20240315.png)

## Encoding and Compression Techniques
TsFile employs advanced encoding and compression techniques to optimize storage and access for time series data. It uses methods like run-length encoding (RLE), bit-packing, and Snappy for efficient compression, allowing separate encoding of timestamp and value columns for better data processing. Its unique encoding algorithms are designed specifically for the characteristics of time series data in IoT scenarios, focusing on regular time intervals and the correlation among series. Additionally, TsFile incorporates frequency domain encoding, utilizing quantization and bit-width reduction to efficiently store frequency domain data for reuse, ensuring space efficiency without compromising data accuracy.

The table below compares 3 file formats in different dimensions.

(![TsFile, Parquet and ORC in Comparison](https://alioss.timecho.com/upload/Apache%20TsFile%20%E5%8F%91%E5%B8%83%E5%9B%BE4-20240315.png))


Its development facilitates efficient data encoding, compression, and access, reflecting a deep understanding of industry needs, pioneering a path toward efficient, scalable, and flexible data analytics platforms.

## Build TsFile

[Java](./java/tsfile/README.md#building-with-java)

[C++(InProcess)](./cpp/tsfile/README.md#build)


## Use TsFile

[Java](./java/tsfile/README.md#use-tsfile)

[C++(InProcess)](./cpp/tsfile/README.md#use-tsfile)