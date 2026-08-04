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

# Bloom Filter

TsFile v4 stores a file-level Bloom filter in `TsFileMetadata` to test whether a timeseries path
may exist. The filter can report that a path is definitely absent or possibly present. It cannot
confirm that a path exists.

## File Format

```text
BloomFilter :=
    UVarInt byte_length
    byte    bits[byte_length]
    UVarInt bit_count?           # present when byte_length > 0
    UVarInt hash_function_count? # present when byte_length > 0
```

When `byte_length = 0`, the file has no Bloom filter and the final two fields are absent.

Bit `i` is stored at bit `i mod 8` in `bits[i / 8]`; bits within each byte are numbered from the
least-significant bit. Trailing all-zero bytes are omitted from the file. `bit_count` records the
logical number of bits.

## Filter Input

The filter input is the UTF-8 representation of the complete timeseries path:

```text
UTF8(join(DeviceID segments, ".") + "." + measurement_id)
```

A null device segment is represented by the ASCII text `null` in the path.

## Hashing

Hashing uses the MurmurHash3 x64 128-bit variant. The final two 64-bit results are added and the
low 32 bits are used. If that result is the minimum 32-bit integer, the value is replaced with
zero; otherwise its absolute value is taken and reduced modulo `bit_count`.

The hash seeds, in order, are:

```text
5, 7, 11, 19, 31, 37, 43, 59
```

`hash_function_count` selects the number of seeds used, with a maximum of 8.

## Lookup Procedure

For an exact path lookup, a reader:

1. constructs the path bytes using the same `DeviceID` segment and measurement rules;
2. evaluates the first `hash_function_count` seeds;
3. tests the corresponding positions in the logical `bit_count`-bit vector;
4. returns **definitely absent** if any tested bit is clear, otherwise **possibly present**.

A positive result must still be checked in the metadata index. A negative result is reliable only
when the query uses exactly the same path canonicalization and UTF-8 bytes as the writer.

## Space and Probability

Let `n` be the number of inserted paths, `m` the logical `bit_count`, and `k` the
`hash_function_count`. The usual Bloom-filter approximation is:

```text
false_positive_probability ≈ (1 - exp(-k * n / m))^k
optimal_k                  ≈ (m / n) * ln(2)
```

These equations explain the writer's space/accuracy tradeoff; they are not additional file fields.
The serialized `bit_count` and `hash_function_count` are authoritative for reading.

## Validation

When `byte_length > 0`, `bit_count` must be positive, `hash_function_count` must be in the range
`1..8`, and `byte_length` must not exceed `ceil(bit_count / 8)`. Omitted trailing zero bytes are
logically reconstructed as zero. The reader rejects a bit-vector length that crosses the
`TsFileMetadata` boundary.
