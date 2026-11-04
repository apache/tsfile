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

# Pi query benchmark 2026-11-04

Raw result of [pi_query_bench.py](../../pi_query_bench.py) for D500-M15,
2,000,000 cold rows, a 200,000-row hot prefix, and a 50,000-row hot tail.
The run used 37 sealed segments, a 50k seal threshold, and one Python
connection.

`query-main.json` contains:

- `config`: dataset shape and extension paths.
- `phases.hot`, `phases.cold`, `phases.mixed`: each query's p50/p95/p99,
  result row count, RSS, and `/proc/self/io` counters.
- `seals`: every seal's elapsed time and RSS peak.
- `validation`: hot rows, file count, device count, watermark, and per-device
  row count.

`p0-results.json` is the targeted re-test after segment-level `min_time` /
`max_time` pruning was added. It contains p50/p95, row counts, and `rchar` for
the cold queries affected by P0.

The companion report is [PI_QUERY_RESULTS.md](../../PI_QUERY_RESULTS.md).
