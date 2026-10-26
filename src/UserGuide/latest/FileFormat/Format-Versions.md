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

The format version is the byte immediately after the leading magic string:

```text
6-byte magic string "TsFile"
1-byte format version
```

This guide describes `0x04`. The format byte and the project release number serve different
purposes: a release in the 2.x series can use file format v4. The project release label is not
stored in the file header.

Version 4 uses segmented DeviceIDs and stores per-table device-index roots and Table Schemas
in `TsFileMetadata`. Version 3 uses a single-string device identifier and a different footer.
A reader needs the corresponding version's structures to parse either file correctly.

## Reader Dispatch

Read the version before interpreting data markers, then select the encryption mode:

```text
validate leading magic -> read version -> inspect optional encryption header
  -> resolve encryption mode/profile -> parse the matching data/footer grammar
```

If the observed version is unsupported, report that before parsing the data. A familiar marker
or tail magic does not establish that the rest of the file uses the expected layout.

## Compatibility Contract

Within a supported version, a reader also needs the types, codecs, and encryption features used
by the requested data. For example, v4 Page AEAD files have a `0x06` encryption header at offset
`7`, which selects an extended Chunk Header and a Page-body profile. The version byte stays
`0x04`; a v4 reader without this capability must report it as unsupported.

Assigned identifiers keep their meanings. Unknown footer properties have length-delimited keys
and values, so their byte ranges can be consumed. Unknown data markers generally cannot be
skipped because they have no common payload length. [Extensibility](Extensibility.md) explains
these boundaries, and [Encryption](DataPages/Encryption.md) defines the AEAD layout.
