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
