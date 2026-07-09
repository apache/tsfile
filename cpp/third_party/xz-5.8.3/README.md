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

# XZ Utils

This directory contains a trimmed source subset from XZ Utils 5.8.3 for
building `liblzma` with the LZMA1/LZMA2 filters used by TsFile.

Upstream source: https://github.com/tukaani-project/xz

TsFile retains only the 0BSD-licensed `liblzma` source subset. GPL and LGPL
components from the upstream distribution are intentionally not included.

Retained files:

- `COPYING.0BSD`
- `src/common/`: common headers and the physical-memory helper required by
  `liblzma`
- `src/liblzma/api/`: public `liblzma` API headers
- `src/liblzma/check/`: CRC32/CRC64 check implementations
- `src/liblzma/common/`: shared container, block, stream, filter, index, and
  VLI implementations
- `src/liblzma/lz/`, `src/liblzma/lzma/`, `src/liblzma/rangecoder/`:
  LZMA1/LZMA2 codec implementation

Excluded upstream components include command-line tools, scripts, GNU getopt,
upstream build files, translations, tests, examples, documentation, debug
utilities, extra utilities, platform-specific packaging files, optional
Delta/BCJ filters, MicroLZMA, lzip, SHA-256, multithreaded stream helpers, and
source generators.
