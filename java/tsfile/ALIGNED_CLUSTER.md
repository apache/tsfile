<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native numeric Cluster pages (research)

This contribution targets `research/cluster-compress` in apache/tsfile, with a
companion contribution to the same research branch in apache/iotdb.

Aligned INT32, INT64, FLOAT and DOUBLE measurements marked ACLUSTER or KCLUSTER
use joint numeric-table encoding. Within an aligned writer, columns marked with
the same method form one group. Other encodings keep their ordinary column path.
This is native typed-column storage: the fields are not BLOB measurements.

The writer collects complete aligned rows, selects columns with the CV-elbow
selector, and performs one multidimensional ACluster/KCluster operation per block.
Unselected or unsafe-to-scale columns use ALP/exceptions/RAW. The selection is
fitted on the first block of this writer/schema and reused across its subsequent
pages. A new chunk writer or changed schema fits a new selection; this is not a
durable database-wide trained model. Native defaults are K=100, iterations=10,
seed=0. Scalar codec properties are not currently exposed as joint-group options.

## SQL through the companion IoTDB research branch

Install this branch's Java common/tsfile artifacts before building IoTDB. All
nodes opening the new files must use the matching research TsFile reader.

```sql
CREATE DATABASE root.cluster_demo;
CREATE ALIGNED TIMESERIES root.cluster_demo.d (
  c1 DOUBLE ENCODING=KCLUSTER,
  c2 DOUBLE ENCODING=KCLUSTER,
  c3 INT64 ENCODING=KCLUSTER
);
INSERT INTO root.cluster_demo.d(time,c1,c2,c3)
ALIGNED VALUES (1,1.2,3.4,5),(2,2.4,6.8,10);
FLUSH;
SELECT c1,c2 FROM root.cluster_demo.d WHERE c3 >= 5;
```

Use ACLUSTER in place of KCLUSTER for the adaptive method. Mark the entire
candidate numeric group; the selector chooses its clustered subspace internally.
An ALP companion still has the group's encoding name in schema metadata, because
the native page decoder dispatches its internal per-column mode.

Single-column CREATE TIMESERIES ... WITH DATATYPE=FLOAT ENCODING=KCluster (or
ACluster) remains supported. Its PageWriter also uses the timestamp-aware native
page envelope, while direct scalar Encoder/Decoder APIs retain their existing
unordered numeric-multiset stream contract.

## Layout and query behavior

ClusterNativePageCodec encodes a full table once, then copies each compressed
column body into its own independently decodable column page. It does not run
independent clustering per column. The prototype duplicates necessary counts,
schema and timestamp keys in these projections; it does not duplicate other
columns' values. This favors correctness and projection simplicity over metadata
size. Do not report its file sizes as the benchmark container's compression ratio.

Aligned value pages start with a negative native marker, version and bounded
block counts. Scalar native pages add a zero time-stream-length prefix, which
distinguishes them from nonempty legacy scalar pages. Nested CLTB v1 blocks retain
their checksums; the existing BLOB block format is unchanged. Blocks have at most
10,000 rows and 2,000,000 cells; a larger native page is split internally without
changing the native page's record membership. Native pages are limited to 100,000
rows and 1,024 candidate columns per method group.

Compressed records remain in cluster order. Native readers expose actual
timestamp keys in increasing order to satisfy IoTDB's scan interface. No original
row number or inverse permutation is serialized, and the table codec does not
promise restoration of input physical order. This SQL scan adaptation adds work
and must be included in future native scan performance measurements.

Timestamp keys are separate from the candidate numeric value columns. They follow
the same row reordering but are stored as exact fixed-width INT64 keys in the
table blocks, without ALP encoding. The aligned shared time column retains its
configured native TsFile time encoding and compression.

Native page/chunk statistics are computed from original typed input, including
nulls, before compression. Existing filtering, projection and aggregation consume
normal typed columns after page decoding. Native IoTDB duplicate-timestamp update
semantics still apply: arbitrary duplicate timestamp tuples are not a bag of
independent records at the native SQL interface. Equal-valued records at distinct
timestamp keys remain separate.

## Validation and limits

ClusterAlignedTsFileTest covers row-wise and column-wise aligned writers,
multiple pages/chunks, timestamp/value associations, single-column projection,
nulls/all-null columns, mixed ordinary encodings, exact large integers, scalar
native files, and malformed native envelopes. Existing ClusterTableCodecTest and
ClusterCodecTest preserve the independent BLOB and direct scalar APIs.

The companion Session module's ClusterAlignedSqlIT issues ordinary SQL only:
aligned and scalar schema creation, numeric INSERT, FLUSH, typed SELECT,
projection, time/value predicates, descending scans, count/sum/first/last, and a
read-only mode for checking persisted data after server restart.

Verified on 2026-10-08 (Asia/Shanghai): 72 distinct TsFile unit/regression cases and
3 Session cases passed, including the retained BLOB tests. DataNode's 3,266 Java
sources compiled against the companion TsFile dependency. On an isolated IoTDB
2.0.2-SNAPSHOT server at the research baseline e5dacc0, using the newly built
TsFile/common jars, each method passed 1,200 aligned SQL records and 400 scalar
SQL records. A forced DataNode restart recovered the final 200 unflushed aligned
records per method from WAL; the read-only checks repeated successfully. The
separate BLOB API also read back 1,200 complete records per method on this same
server before and after restart.

The TsFile build ran Checkstyle, Spotless and license checks. Full-repository
Failsafe integration tests were not run: the offline cache lacked their JUnit
provider. The final targeted build used `-Dtsfile.it.skip=true`; live SQL checks
were executed separately. Repeated Maven executions of the same tests are not
counted as additional test cases.

Example local build commands, after installing upstream dependencies:

```text
# TsFile root (common and parent artifacts already installed)
mvn -o -B -P with-java -pl java/tsfile -Dtest=ClusterAlignedTsFileTest,ClusterTableCodecTest,ClusterCodecTest,ValuePageWriterTest,PageWriterTest -Dtsfile.it.skip=true -Dmaven.javadoc.skip=true install
# IoTDB root
mvn -o -B -pl iotdb-client/session -Dtest=ClusterTableSessionTest test
mvn -o -B -pl iotdb-core/datanode -DskipTests compile
```

The isolated workspace runtime is `runtime/aligned_20261008`, with RPC port 22667;
it does not use production directories. Logs include `native-verified-build.log`,
`native-datanode-compile.log`, `native-session-final-test.log`,
`native-sql-final-write.log`, `native-sql-final-restart-read.log`, and
`native-blob-coexist-restart-read.log`. Start scripts and logs are in the outer
research workspace rather than the Apache source tree.

This remains a research prototype. Compaction variants, deletion/overwrite,
schema evolution, replication, migration of older research files, and full-scale
compression/throughput/memory benchmarks have not been comprehensively validated.
The custom page format requires the modified readers. This document does not
claim compatibility with unmodified released TsFile readers or completion of
the repositories' entire test suites.

The separate ClusterTableTsFile / ClusterTableSession BLOB APIs remain available;
see CLUSTER_TABLE.md. Native SQL does not route through those APIs.
