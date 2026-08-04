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

# Format Versions

The TsFile format version follows the leading magic string:

```text
6-byte magic string "TsFile"
1-byte format version
```

This documentation describes version byte `0x04`. After validating the leading magic string, a
reader selects the corresponding file layout from the version byte.

Version 4 uses segmented `DeviceID` values and stores per-table device-index roots and Table
Schemas in `TsFileMetadata`. Version 3 uses a single-string device identifier and a different
footer layout; it cannot be parsed with the version 4 `DeviceID` or `TsFileMetadata` structures.

## Reader Dispatch

The version byte is interpreted before any data-section marker. A reader therefore follows this
order:

```text
validate leading magic -> read version -> select complete version grammar -> parse data/footer
```

An implementation that supports version 4 but not the observed byte must report an unsupported
version. It must not probe the file as v4 based on a familiar marker or footer suffix, because the
meaning and width of structures may differ between versions.

## Compatibility Contract

Compatibility is defined at the byte-format level:

- A v4 reader may read a v4 file when it supports every identifier required by the selected data.
- A v4 writer emits the v4 grammar described by this documentation, independent of its programming
  language or in-memory classes.
- Unknown footer properties can be consumed because each key and value is length-delimited.
- Unknown data-section markers cannot generally be skipped because markers have no common payload
  length.
- Numeric identifiers already assigned to types, encodings, compression methods, markers, and
  index-node kinds retain their meanings within v4.

Support for a version does not imply support for every optional codec or encryption algorithm.
When such a feature is unavailable, the containing Chunk may remain structurally skippable, but
its sample values are not decodable.

The project release series and the on-disk format byte are separate version spaces. A product in
the 2.x release series can read and write format byte `0x04`; the release label is not serialized
in the TsFile header.
