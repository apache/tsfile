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

# TsFile Performance and Storage

Load this reference only for throughput, latency, memory, compression, encoding,
or file-size work. Treat every recommendation as a benchmark hypothesis rather
than a universal setting.

## Write Path

- Prefer tablets/batches over individual records for throughput-oriented
  ingestion.
- Benchmark batch size against row width, memory budget, flush frequency, and
  latency requirements. Avoid fixed record-count folklore across schemas.
- Keep related measurements together when it improves locality for actual
  queries.
- Flush deliberately where the binding requires it, then close the writer.

## Read Path

- Select only required measurements or columns.
- Bound time ranges and push supported filters into the reader.
- Stream or consume bounded batches instead of materializing an entire large
  result set.
- Separate metadata inspection from page decoding when the available tool or
  API supports it.

## Encoding and Compression

- Start from the implementation defaults for the target version and language.
- Match encodings to observed data distribution only after measuring: monotonic
  integers, floating-point continuity, boolean/cardinality patterns, and text
  repetition affect results differently.
- Verify that the selected encoding is valid for the data type and implemented
  by the chosen binding.
- Measure compression ratio together with CPU cost and read/write latency; a
  smaller file is not automatically the best operational result.

## Benchmark Checklist

1. Record the TsFile version, language binding, schema, encoding/compression,
   batch size, and dataset characteristics.
2. Warm up the runtime where applicable.
3. Measure write throughput, read latency, peak memory, and output size.
4. Validate file contents after each configuration.
5. Change one important variable at a time and retain the baseline results.
