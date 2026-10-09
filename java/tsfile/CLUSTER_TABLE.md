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

# Complete numeric table blocks (research)

This opt-in Java API combines CV-elbow column selection, multidimensional ACluster/KCluster,
and ALP for companion columns. It stores independent, versioned CLTB blocks in standard TsFile
BLOB measurements. Its CLTB block format and direct scalar codec APIs are retained.
Native numeric SQL pages are a separate opt-in path described in [ALIGNED_CLUSTER.md](ALIGNED_CLUSTER.md).

Use `org.apache.tsfile.encoding.table.ClusterTable` with explicit timestamps, column names,
INT32/INT64/FLOAT/DOUBLE types and strictly matching boxed values (or null).
Use `ClusterTableOptions.aCluster()` or `kCluster()`.

```java
try (ClusterTableTsFile.Writer writer =
    new ClusterTableTsFile.Writer(new File("table.tsfile"), options)) {
  writer.append(table);
}
ClusterTableTsFile.read(new File("table.tsfile"), decoded -> {
  // Consume complete records; original physical order is not promised.
});
```

The writer selects from at most the first 10,000 rows and reuses that selection.
Pages are independent, with at most 10,000 rows and 2,000,000 numeric cells each.
All columns, timestamps and nulls share the cluster traversal; no inverse permutation is stored.
Inexact scaling or unsupported ranges use exact ALP exceptions/RAW storage.
All IEEE bits and all INT64 values are retained, including duplicate records.

This is a block-container API. Standard SQL/ordinary TsFile readers see BLOB blocks,
not the original numeric fields. The companion IoTDB research branch provides
`org.apache.iotdb.session.ClusterTableSession` to decode and filter original records.

The ALP implementation ports this project's exponent/factor + FOR + exceptions approach.
CLTB is not wire-compatible with either the earlier C++ benchmark container or upstream ALP.
The medoid objective uses the paper's exact bit-width formula (zero residual costs one bit).
Java and C++ seeded random initialization are not bit-identical.

Run `ClusterTableCodecTest` and the existing `ClusterCodecTest` through the normal Maven build.
`ClusterTableDataCheck <outer-workspace>` optionally verifies the public-data selector manifest
and bounded complete-record round trips. It is a functional check, not a performance benchmark.
