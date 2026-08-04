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

A completed TsFile contains the leading magic string, version byte, `0x02` separator, footer
metadata, 4-byte `TsFileMetadata` length, and trailing magic string. A file without the footer
length or trailing magic string is incomplete.

A recovery reader can scan the data section from offset 7 by markers and use the `data_size` in
each Chunk Header to skip a complete Chunk. Within a Chunk, the sum of the serialized Page lengths
equals `data_size`.

A scanner cannot treat an arbitrary `0x02` byte in Page data as a separator. It may read the next
marker only after completely parsing or skipping the structure delimited by the preceding marker.

## Completion Check

Given file size `F` and the big-endian footer length `L` stored at `[F - 10, F - 6)`, a completed
file must satisfy all of the following before ordinary metadata-first reading begins:

```text
bytes[F - 6, F)            = "TsFile"
TsFileMetadata interval    = [F - 10 - L, F - 10)
meta_offset                = offset of the top-level 0x02 separator
meta_offset < F - 10 - L
```

The leading magic and version must also be valid, and the `TsFileMetadata` decoder must consume
exactly `L` bytes. A valid tail magic by itself does not prove that the file is complete.

## Safe Forward Scan

A recovery scanner can identify the longest structurally complete prefix of the data section:

1. validate the leading magic and version and set the cursor to offset `7`;
2. at a known top-level boundary, read one marker;
3. for a Chunk Group Header (`0x00`), decode its complete `DeviceID` and remember the group context;
4. for a Chunk marker, decode the complete Chunk Header, validate `cursor + data_size`, then either
   skip that range or validate all Page boundaries inside it;
5. for an Operation Index Range (`0x04`), consume exactly two big-endian `int64` values;
6. stop successfully at the top-level separator (`0x02`), or stop at the last known-good boundary
   when a field or payload is incomplete.

The last known-good boundary is the end of a complete delimited structure, not the last byte that
happened to be readable. A recovery tool may use the complete Chunks before that boundary to
rebuild indexes and a new footer; it must not invent or partially decode the truncated Chunk.

The file is incomplete or corrupt when any of these conditions is detected:

- A Chunk's `data_size` extends beyond the file or the next structure boundary.
- A Page length extends beyond its Chunk boundary.
- The footer length cannot locate a complete `TsFileMetadata` structure.
- `meta_offset` does not point to the `0x02` separator.
- An index offset is outside the file or points into the middle of a structure.

Other failures include an unknown top-level marker, an unterminated VarInt or VarString, integer
overflow while computing an end offset, a Page decoder that produces the wrong number of elements,
and an aligned Time/Value Page mismatch.

## Recovery Limits

Recovery reconstructs structure, not missing information. A complete compressed or encrypted Page
still requires its declared codec and, where applicable, the external key. Without a file-level
checksum, a structurally valid byte substitution may be indistinguishable from original data.
Operation Index Range records can help a storage engine relate the salvaged prefix to its external
operation log, but their meaning does not replace Chunk and Page boundary validation.

TsFile v4 has no general checksum. See [Checksumming](Checksumming.md) for the resulting limits.
