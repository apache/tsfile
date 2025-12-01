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

# Apache TsFile 2.1.1

## Improvement/Bugfix
* [JAVA] AbstractAlignedTimeSeriesMetadata.typeMatch always return true in #538
* [JAVA] Ignore the null value passed in the Tablet.addValue method in #540
* [JAVA] Implement extract time filters in #539
* [JAVA] Init all series writer for AlignedChunkGroupWriter in #545
* [JAVA] Check max tsfile version in #548
* [JAVA] Include common classes in tsfile.jar to fix #501 in #510
* [JAVA] Implement extract value filters in #554
* [JAVA] Fix wrong Private-Package declaration (related to #551) in #556
* [JAVA] Avoid repeated calculation of shallow size of map in #559
* [JAVA] Refactor UnknownType to extend AbstractType in #561
* [JAVA] Add Tablet.append in #562

# Apache TsFile 2.1.0

## New Feature
- [Java] Support setting default compression by datatype(#523).
- [Java] Support using environment variables to generate main encrypt key(#512).
- [Java] Support estimating ram usage of measurement schema(#508).
- [Java] Add TsFileLastReader to retrieve the last points in a TsFile(#498).
- [Cpp/C/Python] Support TsFile Table reader and writer.

## Improvement/Bugfix
- [Java] Fix memory calculation of BinaryColumnBuilder(#530).
- [Java] Resolved case sensitivity issue when reading column names(#518).
- [Java] Fix npe when closing the last reader that has not been used(#513).
- [Java] Fix float RLBE encoding loss of precision(#484).

# Apache TsFile 2.0.3

## Improvement/Bugfix
* move ColumnCategory to an outer class in (#461)
* restrict encrypt key length to 16 in (#467)
* Cache hash code of StringArrayDeviceID in (#453)
* Skip time column when generating TableSchema in (#414)
* Check blank column name or table name in (#471)
* Optimizations regarding chunk metadata sort & timeseries metadata serialization in (#470)
* Remove redundant conversion in TableResultSet in (#473)
* Add switch to disable native lz4 in (#480)

# Apache TsFile 2.0.2

## Improvement/Bugfix
- Correct the retained size calculation for BinaryColumn and BinaryColumnBuilder
- Don't print exception log when thread is interrupted (#386)
- Fix float encoder overflow when float value itself over int range (#412)
- Fix date string parse error (#413)
- compaction adapting new type when table alter column type (#415)
- primitive type compatible (#437)
- Fixed the empty string ser/de bug & null string[] array calculation bug
- add getter for encryptParam (#447)


# Apache TsFile 2.0.1

## Improvement/Bugfix
- Modify tablet usage (#358)
- Add column builder compatibility (#367)
- add cache table schema map option (#369)
- fix getVisibleMetadataList
- TimeColumn.reset() throws UnsupportedOperationException (#379)
- Add statistic compatibility (#382)

# Apache TsFile 2.0.0

## New Feature
- TsFile V4 for Table Model by @jt2594838 in #196
- Support dictionary encoding for STRING data type. by @jt2594838 in #238
- Modify default timestamp encoding by @shuwenwei in #309
- Tsfile java interfaces v4 by @shuwenwei in #307
- Convert column name and table name to lower case by @shuwenwei in #322
- Add type cast interfaces in TsDataType by @jt2594838 in #332

## Improvement/Bugfix
- Fix allSatisfy bug in InFilter by @JackieTien97 in #219
- Fix bug in the conversion of int types to timestamp. by @FearfulTomcat27 in #223
- Fix getValue method in Tablet doesn't support Date and Timestamp type by @HTHou in #243
- Fix error when write aligned tablet with null date by @HTHou in #250
- Fix tablet isNull method not correct by @HTHou in #255
- Fixed the issue that the time of the first data item written to TSFile by measurement cannot be a negative number by @luoluoyuyu in #297
- Fix float encoder overflow by @HTHou in #342

# Apache TsFile 1.1.1

## Improvement/Bugfix
* Fixed the issue that the time of the first data item written to TSFile by measurement cannot be a negative number (#297)
* Add LongConsumer ioSizeRecorder in TsFileSequenceReader for IoTDB scan (#301)
* Add readItimeseriesMetadata method (#312)
* Tablet.serialize() may throw an exception due to null values in the Date column (#330)
* Add FlushChunkMetadataListener (#328)
* Add final for readData methods (#347)
* Bump logback to 1.3.15 (#362)
* Fix example compile issue (#400)
* Fixed the empty string ser/de bug & null string[] array calculation bug (#449)

# Apache TsFile 1.1.0

## New Feature
- Support new data types: STRING, BLOB, TIMESTAMP, DATE by @Cpaulyz in #76
- Add an equivalent .getLongs() method to .getTimes() in TimeColumn. by @Sh-Zh-7 in #61
- Return all columns in TsBlock class by @Sh-Zh-7 in #80

## Improvement/Bugfix

- Fix value filter allSatisfy bug by @liuminghui233 in #41
- Fix error log caused by ClosedByInterruptException by @shuwenwei in #47
- Fix the mistaken argument in LZ4 Uncompressor by @jt2594838 in #57
- Remove duplicate lookups in dictionary encoder by @MrQuansy in #54
- Optimize SeriesScanUtil by memorizing the order time and satisfied information for each Seq and Unseq Resource by @JackieTien97 in #58
- Fix TsBlockBuilder bug in AlignedPageReader and PageReader. by @JackieTien97 in #77
- Fix ZstdUncompressor by @lancelly in #132
- fix RLBE Encoding for float and double by @gzh23 in #143
- Fix uncompress page data by @shuwenwei in #161
- Fix encoder and decoder construction of RLBE by @jt2594838 in #162
- Fix aligned TimeValuePair npe by @shuwenwei in #173
- Fix StringStatistics data type by @shuwenwei in #177
- Fix bug in the conversion of int types to timestamp. by @FearfulTomcat27 in #224
- Fix error when write aligned tablet with null date by @HTHou in #251

# Apache TsFile 1.0.0

## New Features

- Support registering devices
- Support registering measurements
- Support adding additional measurements
- Support writing timeseries data without pre-defined schema
- Support writing timeseries data with pre-defined schema
- Support writing with tsRecord
- Support writing with Tablet
- Support writing data into a closed TsFile
- Support query timeseries data without any filter
- Support query timeseries data with time filter
- Support query timeseries data with value filter
- Support BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TEXT data types
- Support PLAIN, DICTIONARY, RLE, TS_2DIFF, GORILLA, ZIGZAG, CHIMP, SPRINTZ, RLBE encoding algorithm
- Support UNCOMPRESSED, SNAPPY, GZIP, LZ4, ZSTD, LZMA2 compression algorithm
