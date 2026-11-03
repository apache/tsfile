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

# Raspberry Pi 2026-11-03 raw results

Raw output of [pi_write_seal_bench.py](../pi_write_seal_bench.py) for the
matrix described in [PI_RESULTS.md](../../PI_RESULTS.md): 2,000,000 rows per
configuration, 100/500/2,000 devices, 5/15/30 measurements, seal thresholds
20k/30k/50k/100k rows.

The `.json.gz` files are the original JSON payloads, gzip-compressed. Decompress
a file with:

```bash
gzip -dk 20261103-181747-main-D100-M30-S50000.json.gz
```

Each JSON contains:

- `config`: devices, measurements, rows, batch size, seal rows, extension path,
  SQLite/Python versions.
- `rss_samples`: `[elapsed_seconds, VmRSS_kB]`, sampled every 250 ms.
- `rss`: `mean_kb`, `peak_kb`, and whether the RSS watchdog fired.
- `seals`: one entry per seal with `returned` rows, `elapsed_s`,
  `rss_before_kb`, `rss_peak_kb`, output file count and bytes.
- `write_s`, `seal_s`, `wall_s`, `write_rows_per_s`, `write_points_per_s`,
  batch latency percentiles, DB/WAL/TsFile sizes, and validation results.

`env-20261103-164915.txt` and `env-20261103-180205.txt` are environment
snapshots taken before the runs (machine, CPU, memory, disk, Python, SQLite,
extension SHA256, temperature and throttle flags).

All values come from one run per configuration on a Raspberry Pi 4B. RSS is
sampled at 250 ms, so instantaneous peaks may be slightly higher than recorded.
