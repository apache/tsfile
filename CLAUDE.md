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

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache TsFile is a columnar storage file format designed for time series data. The project provides implementations in three languages, each in its own top-level directory. The main branch is `develop`.

TsFile uses a hierarchical storage model: **Page → Chunk → ChunkGroup → File**. Data is organized by device, with each device's measurements stored as individual time series. Two write models exist: **aligned** (all measurements share timestamps) and **non-aligned** (independent timestamps per measurement).

## Repository Structure

| Directory | Language | Build System | Details |
|-----------|----------|--------------|---------|
| `java/`   | Java     | Maven        | [java/CLAUDE.md](java/CLAUDE.md) |
| `cpp/`    | C++      | CMake        | [cpp/CLAUDE.md](cpp/CLAUDE.md) |
| `python/` | Python   | setuptools + Cython | [python/CLAUDE.md](python/CLAUDE.md) |

The root `pom.xml` orchestrates all three via Maven profiles:

```bash
./mvnw clean verify -P with-java       # Java only
./mvnw clean verify -P with-cpp        # C++ only
./mvnw clean verify -P with-python     # Python (requires C++ built first)
```

## Cross-Module Dependencies

- **Python depends on C++**: The Python module uses Cython to bind to the C++ shared library (`libtsfile`). C++ must be built before Python (`-P with-python` implies C++ build).
- **Java is independent**: The Java implementation shares no build-time dependency with C++ or Python.
- All three implementations read/write the same TsFile binary format.

## Code Formatting

```bash
./mvnw spotless:apply    # Format all languages (Java: Google Java Format, C++: clang-format, Python: Black)
./mvnw spotless:check    # Check formatting without modifying
```

## License Header

Every new file must include the Apache License 2.0 header at the top. Use the comment style appropriate for the file type (e.g., `<!-- -->` for Markdown, `/* */` for Java/C++, `#` for Python). See any existing file for the exact wording.

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.
