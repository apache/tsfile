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

# CLAUDE.md — Java Module

This file provides guidance to Claude Code (claude.ai/code) when working with the Java module.

## Build Commands

Maven with wrapper (`./mvnw` from repo root). All commands require `-P with-java`.

```bash
# Build (skip tests)
./mvnw clean package -P with-java -DskipTests

# Install to local Maven repo
./mvnw install -P with-java -DskipTests

# Run all tests (unit + integration)
./mvnw clean verify -P with-java

# Run unit tests only
./mvnw test -P with-java

# Run integration tests only
./mvnw verify -P with-java -DskipUTs=true

# Run a single test class (use -pl to target submodule)
./mvnw test -P with-java -pl java/tsfile -Dtest=TsFileWriterTest

# Run a single test method
./mvnw test -P with-java -pl java/tsfile -Dtest=TsFileWriterTest#testWrite

# Format code (requires Java 17+ runtime)
./mvnw spotless:apply

# Check formatting
./mvnw spotless:check
```

## Module Structure

Four submodules under `java/`:

- **common** — Shared types: `TSDataType`, `ColumnCategory`, `Binary`, `BitMap`, constants
- **tsfile** — Core implementation: read/write APIs, encoding, compression, file format
- **examples** — Reference implementations for read/write operations
- **tools** — CLI utilities for TsFile inspection

## Architecture

Key packages in `java/tsfile/src/main/java/org/apache/tsfile/`:

- `write/` — Write path: `TsFileWriter`, `Tablet` (batch API), `TSRecord` (row API)
- `read/` — Read path: `TsFileReader`, `TsFileSequenceReader`, `QueryExpression`
- `encoding/` — Encoding algorithms (RLE, TS_2DIFF, GORILLA, DICTIONARY)
- `compress/` — Compression codecs (Snappy, LZ4, ZSTD, XZ)
- `file/` — File format structures (metadata, headers, footers)
- `compatibility/` — Version compatibility handling
- `parser/` — ANTLR4-generated path parser (grammar in `src/main/antlr4/`)

Code generation: FreeMarker templates in `tsfile/src/main/codegen/` generate type-specific implementations (e.g., per-datatype readers/writers) into `target/generated-sources/`.

## Code Style

- **Formatter**: Spotless with Google Java Format 1.28.0 (Spotless requires Java 17+ to run but the project targets Java 8 bytecode)
- **Checkstyle**: Google style variant in root `checkstyle.xml`, 100 char line limit
- **Import order**: `org.apache.tsfile`, `javax`, `java`, static imports

## Testing Conventions

- **Framework**: JUnit 4
- Unit tests: `*Test.java` — run via `mvn test`
- Integration tests: `*IT.java` — run via `mvn verify`
- Tests run in random order with `reuseForks=false`

## License Header

Every new file must include the Apache License 2.0 header at the top. For Java files, use the `/* */` block comment style. See any existing `.java` file for the exact wording.

## Git Commit

- Do NOT add `Co-Authored-By` trailer to commit messages.
