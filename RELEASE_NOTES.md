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
