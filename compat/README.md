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

# Java / C++ cross-language compatibility fixtures

`compatibility-matrix.csv` is the single source of truth for the fixtures that
the Java and C++ implementations write for each other. Both test suites read
this file:

- Java: `java/tsfile/src/test/java/org/apache/tsfile/compatibility/EncodingCompressionCompatibilityTest.java`
- C++: `cpp/test/reader/table_view/encoding_compression_compatibility_test.cc`

Each side writes every case it declares writable, reads back its own fixtures,
and validates fixtures produced by the other language. The CI workflow
`.github/workflows/compatibility-test.yml` runs generation in one job per
language and cross-validation in both directions, then publishes the coverage
summary.

## Columns

| Column | Meaning |
| --- | --- |
| `id` | Stable case id. Expanded cases get `.` + compression + `.` + null pattern. The fixture file is `<expanded id>.tsfile`. |
| `model` | `table` (Tag/Field table model) or `tree` (device/measurement model). |
| `dataType` | `BOOLEAN`, `INT32`, `INT64`, `TIMESTAMP`, `DATE`, `FLOAT`, `DOUBLE`, `TEXT`, `STRING`, `BLOB`. |
| `encoding` | `TSEncoding` enum name. |
| `compressions` | `ALL` (the six codecs both projects ship: UNCOMPRESSED+SNAPPY+GZIP+LZ4+ZSTD+LZMA2) or a `+`-separated subset. |
| `rowCount` | Number of timestamps per device. |
| `pageRows` | Points per page; `0` keeps the writer default. |
| `chunks` | Number of chunks to produce by flushing between sub-batches. |
| `devices` | Number of devices written into one file (one chunk group per device). |
| `valueProfile` | Deterministic value set, see below. |
| `nullPatterns` | `|`-separated `NONE`, `MIXED`, `ALL`. |
| `javaWrite`/`cppWrite` | Whether that language writes the fixture. |
| `javaRead`/`cppRead` | Whether that language validates the fixture when the other side wrote it. |
| `gapReason` | Required whenever any write/read cell is `no`; the reason is reported in every CI run. |

A case with a `no` in a write column is an explicit declared gap. A case absent
from the file is not a gap: the completeness test in both suites enumerates the
cartesian product of every data type and encoding the local writer supports and
fails when the spec does not cover it. That is what keeps the matrix complete
as new encodings are added.

## Data scale and page-size tiers

Every (type, encoding, compression) combination is generated at the three
smaller data scales — 10, 1000 and 100000 rows — with a page size that crosses
page boundaries at that scale. The 1M scale uses a reduced, representative
encoding set per type, because cross-language compatibility is already fully
covered at the smaller scales and the 1M tier exists to stress the
page/chunk/index machinery at scale (largely encoding-independent):

| Scale | Page size | Page structure it exercises | Encoding coverage |
| --- | --- | --- | --- |
| 10 rows | 4 | 3 pages with a partial last page (4+4+2) | full cartesian |
| 1000 rows | 100 | 10 pages | full cartesian |
| 100000 rows | 10000 | 10 pages (writer default) | full cartesian |
| 1000000 rows | 10000 | 100 pages (writer default) | PLAIN / TS_2DIFF / RLE / GORILLA / DICTIONARY |

`UNCOMPRESSED` is dropped at the 1M tier: a million raw values (or long
repeated strings) is a multi-megabyte file per case for no extra coverage, and
it would blow the CI artifact budget. It is still fully covered at the three
smaller scales.

A reduced encoding set (`PLAIN`, `TS_2DIFF`, `RLE`) also gets a "many pages"
variant — page 100 at 100000 rows, page 1000 at 1000000 rows — which produces
1000 pages and crosses the 256-entry index-node fanout, stressing the
multi-level metadata index.

## Value profiles

Values are a pure function of the profile and the row index, implemented
identically in Java and C++. Expected values apply the same quantization the
writer performs, so a fixture has one expected result no matter which language
wrote it.

| Profile | Purpose |
| --- | --- |
| `default` | Type-specific values including numeric extremes and repeated values. |
| `ts2diff` | FLOAT/DOUBLE TS_2DIFF values covering scaled, scale-overflow and raw-bit forms (NaN, infinities) across the 129-value block boundary. |
| `ts2diff_mpn0` | Same values written with an explicit `max_point_number=0` property (Java only; the C++ writer has no property path). |
| `sprintz` | Values inside a range the SPRINTZ FIR predictor can encode (about ±1e8 for INT32 and ±1e12 for INT64). Outside it the predictor itself overflows, which is a codec limitation rather than a cross-language difference. |
| `runs` | Long runs for RLE encodings. |
| `strings` | Empty string, embedded NUL, multi-byte UTF-8, repeated values, long values. |
| `dict_high` | One distinct string per row to stress dictionary growth. |
| `blob` | Empty, all-zero, binary and text-like byte arrays. |

## Running locally

```bash
# Java writes, Java and C++ read
./mvnw -P with-java \
  -Dtest=EncodingCompressionCompatibilityTest#generateFixtures \
  -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtsfile.compat.spec="$PWD/compat/compatibility-matrix.csv" \
  -Dtsfile.compat.generate.dir="$PWD/compat-fixtures/java" test

TSFILE_COMPAT_SPEC="$PWD/compat/compatibility-matrix.csv" \
TSFILE_COMPAT_VALIDATE_DIR="$PWD/compat-fixtures/java" \
  ./cpp/target/build/test/lib/TsFile_Test \
  --gtest_filter=EncodingCompressionCompatibilityTest.ValidateFixtures
```

Swap the environments for the C++ → Java direction. Both suites also emit
`report.csv` next to the fixtures with a per-case status, which the CI job
summary aggregates.

## Adding coverage

1. Add a row (or edit an existing one) in `compatibility-matrix.csv`.
2. If a language cannot write or read the new combination, set the column to
   `no` and write the reason. Link a tracking issue when the gap is a defect.
3. Run both directions locally; the generator fails when a declared case cannot
   be produced and the validator fails when a declared-readable fixture cannot
   be read.
