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

# Extensibility

TsFile v4 provides numbered or self-describing extension points in the following locations:

- Data types, encodings, and compression methods use single-byte identifiers.
- A length-delimited string property map is stored at the end of `TsFileMetadata`.
- The file version byte selects the complete file layout.

A new data type, encoding, or compression method requires a new identifier. The wire meaning of
an existing identifier does not change.

Each footer property consists of a `VarString` key and value. A reader can consume an
unrecognized property without changing the boundaries of the other footer fields.

Data-section markers do not have a common length field. A reader cannot safely skip an unknown
marker. A new data-section structure that cannot be fully delimited by an existing marker
therefore requires either a new file version or an encoding contained within an existing
structure.

## Safe Evolution Rules

An extension is safely skippable only when an older reader can determine its complete byte range
without interpreting the new payload. Examples are a new footer property value and a new codec
inside a Chunk whose `data_size` is known. Adding bytes to a non-length-delimited header is not
skippable because every following field would move.

Within v4, extension authors must:

- allocate a previously unassigned identifier and never reuse an existing value;
- preserve byte order, length interpretation, and containment boundaries of existing structures;
- define which physical data types are valid for a new encoding;
- define output-size, element-count, and malformed-input behavior for new codecs;
- require an explicit capability error when a reader cannot decode the extension.

Reserved or unknown identifiers are not aliases for a default. In particular, an unknown encoding
is not `PLAIN`, an unknown compression method is not `UNCOMPRESSED`, and an unknown marker is not
padding.

## Properties and Semantics

Length delimitation makes an unknown footer property structurally readable, but it does not make
the property's semantics optional. A property that changes how existing bytes must be interpreted
must define how readers advertise and enforce support. Security-critical properties must fail
closed when recognized semantics cannot be applied.

Changes that alter the grammar of top-level structures, reinterpret assigned identifiers, or make
old boundaries ambiguous require a new format version.
