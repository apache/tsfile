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

TsFile v4 does not store a general Page checksum, Chunk checksum, or file checksum.

The leading and trailing magic strings, length fields, markers, offsets, and structure boundaries
can expose some truncation or structural corruption. They do not provide content verification or
a cryptographic integrity guarantee.

## What Structural Validation Detects

A strict reader can detect many malformed layouts by requiring:

- both magic strings and the version byte to be present;
- the footer length and `meta_offset` to resolve to valid boundaries;
- every `data_size`, `compressed_size`, string length, and metadata-list size to remain inside its
  containing structure;
- decompression to produce exactly `uncompressed_size` bytes;
- decoded element counts, aligned bitmaps, Statistics, and index ranges to be internally
  consistent.

These checks are necessary even when an external checksum exists. They can detect truncation and
many accidental changes, but a byte substitution may preserve every length and boundary and remain
undetected.

## Integrity at Rest or in Transit

Systems that need corruption detection should protect the complete file with an external checksum
or object-store integrity field. Systems that need protection against intentional modification
should use an authenticated digest or signature whose key or trusted value is stored outside the
TsFile. The external value must cover the exact complete-file byte sequence, including the footer
length and trailing magic string.

Encryption of Page payloads is a separate feature. Unless the selected encryption algorithm
explicitly authenticates its ciphertext, encryption alone does not add a TsFile checksum or verify
the unencrypted headers and metadata.
