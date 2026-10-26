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

# Checksumming

TsFile v4 has no general Page, Chunk, or whole-file checksum. A completed-looking file can still
contain damaged values, so the head and tail magic strings should not be used as content checksums.

## What Structural Validation Detects

Lengths, offsets, and counts help detect incomplete or malformed structures. A Page that extends
beyond its Chunk, a footer length outside the file, or a decompressed payload with the wrong size
can all be rejected. These checks are described in [Binary Representation](../Binary-Representation.md)
and [Data Pages](README.md).

They do not detect every change. A modified value can occupy the same number of bytes and leave
all surrounding boundaries intact. Structural checks remain necessary even when a separate
checksum is available.

## Integrity at Rest or in Transit

To detect changes anywhere in a file, maintain an external checksum or use the storage system's
integrity mechanism. If protection against intentional modification is required, use a keyed
authentication code or signature with a trusted key or reference stored outside the file. Cover
the complete byte sequence, including the footer length and tail magic.

Page AEAD provides a narrower guarantee: it authenticates non-empty Page bodies and their context,
and binds Chunk ordinals to file positions. It does not cover all visible headers, Statistics,
indexes, or footer metadata. Empty aligned Value Pages have no Page authentication tag, and
legacy encryption does not necessarily authenticate its ciphertext. See [Encryption](Encryption.md)
for the exact scope.
