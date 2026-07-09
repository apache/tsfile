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

# Zstandard

This directory contains a trimmed source subset from Zstandard 1.5.7.

Upstream source: https://github.com/facebook/zstd

TsFile uses Zstandard under the BSD license option. The GPLv2 `COPYING` file
from the upstream distribution is intentionally not included.

Retained files:

- `LICENSE`
- `lib/zstd.h`
- `lib/zstd_errors.h`
- `lib/common/`
- `lib/compress/`
- `lib/decompress/`

Excluded upstream components include command-line tools, tests, examples,
documentation, contrib code, dictionary builder code, deprecated APIs, legacy
format support, and zlib wrapper code.
