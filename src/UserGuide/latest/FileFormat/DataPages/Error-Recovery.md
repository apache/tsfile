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

# Error Recovery

The footer is written after the data section. If writing stops before that happens, some complete
Chunks may still be present even though the file cannot be opened through its indexes. Recovery
finds those structures by scanning forward from the file header, then rebuilds the missing indexes
and footer from the data it can retain.

## Completion Check

For a file of size `F`, read the big-endian metadata length `L` from `[F - 10, F - 6)`. A completed
file has these relationships:

```text
bytes[F - 6, F)            = "TsFile"
TsFileMetadata interval    = [F - 10 - L, F - 10)
meta_offset                = offset of the top-level 0x02 separator
meta_offset < F - 10 - L
```

Also check the leading magic and version, and parse exactly `L` bytes of `TsFileMetadata`.
Tail magic alone is insufficient: the footer length and its internal offsets must be usable.

## Safe Forward Scan

A forward scan uses the same Chunk layout as an ordinary reader, but does not rely on the footer:

1. Check the magic and version. Inspect offset `7` and consume an optional `0x06` encryption
   header. The data starts at `7 + E`, where `E` is the header's total size or zero when absent.
2. At a structure boundary, read a marker. For `0x00`, read the DeviceID and remember the group.
3. For a Chunk marker, read its complete Header, including the AEAD suffix when required.
   Verify the Chunk proof before using its identity, then check that `data_size` fits in the
   remaining file. Inspect the Pages or skip exactly that many bytes.
4. For `0x04`, consume the two operation-index `int64` values.
5. At the top-level `0x02` separator, stop scanning the data section.

Stop at the last complete structure if a field or body is truncated. An unknown marker, a second
or misplaced encryption header, an invalid length, or an authentication failure must be treated
as an error, not skipped by searching for another familiar byte. A `0x02` inside a Page body is
data and cannot identify the end of the data section.

Retain only structures that have been read completely and checked. The readable part of a truncated
Chunk is not automatically a valid Chunk. The common bounds checks are in
[Binary Representation](../Binary-Representation.md), and Page boundaries are in [Data Pages](README.md).

## Recovery Limits

Rebuilding an index cannot restore missing values or identify every change to otherwise valid
bytes. The retained Pages still need their codecs and, when encrypted, their key material.
Operation-index records can help a storage engine relate the retained data to its external log,
but they do not replace structure checks.

For Page AEAD, preserve the original encryption header, file crypto ID, Chunk ordinals, and Page
indexes. Truncating the tail and replacing the footer can leave retained Chunks at their original
offsets. Moving a Chunk requires a new position proof; changing its file identity, ordinal, or
Page context also requires authenticating and re-encrypting the affected data as appropriate.
When appending, allocate ordinals above the highest retained ordinal.

An authenticated recovery needs the matching provider and external keys. The legacy footer cannot
replace a damaged AEAD header, because its legacy encryption fields are deliberately set to
unencrypted. [Encryption](Encryption.md) describes that mode selection; [Checksumming](Checksumming.md)
explains which changes remain outside the format's integrity checks.
