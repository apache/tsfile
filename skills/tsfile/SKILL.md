---
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
name: tsfile
description: >-
  Work with the current Apache TsFile SDKs, tools, and file format. Use for
  Java, Python, C++, or C reads and writes; Python DataFrame and Arrow batches;
  Java CSV/Parquet/Arrow import; file properties; TAG filters; query-by-row and
  metadata APIs; C++ incomplete-writer recovery and append; static/shared C++
  builds; parallel and TsBlock writes; encoding/compression factories; schema
  design; performance; and cross-language integration. Route operations
  specific to the C++ tsfile-cli binary to the sibling tsfile-cli skill.
---

# TsFile

## Scope

Use this skill for SDK code, Java import and point-count tools, Tree/Table model
decisions, schema design, compatibility, and cross-language integration. Use
the sibling `../tsfile-cli/SKILL.md` for the C++ `tsfile-cli` binary: file
inspection, preview, export, sampling, and its explicit single-table CSV
`write` operation.

## Operating Rules

1. Before giving a dependency version or version-sensitive API, run
   `scripts/resolve-version.sh`. Pass `--root <checkout>` when the target is not
   the repository that bundles this skill. When no checkout is present, the
   script returns the current skill baseline. Inspect external dependency
   metadata only when the user targets a different project or release. Do not
   treat `latest` as a release number. Read `references/source-policy.md` only
   when the authority remains ambiguous, sources conflict, or a
   freshness/published-release claim matters.
2. Use `references/docs-map.yaml` only when an official online page, release,
   download, or repository link is needed.
3. Choose Tree or Table model, then choose one language binding. Do not load all
   language references by default.
4. Close writers, readers, and result sets so file footers and native resources
   are finalized. Validate files with every language that must consume them.

## Offline Reference Routing

Read only the files required by the current task:

- Source authority, version conflicts, offline/online selection, and update
  rules: `references/source-policy.md`
- Official website, download, release, and repository URL registry:
  `references/docs-map.yaml`
- Model selection, schema, data types, and generic read/write workflow:
  `references/core-concepts.md`
- Java SDK code and API guardrails: `references/java.md`
- Java CSV/Parquet/Arrow import and table point-count tools:
  `references/java-tools.md`
- Python DataFrame datasets, pandas conversion, and Arrow batch I/O:
  `references/dataframe-arrow.md`
- File-level properties and Table-model TAG filters across bindings:
  `references/properties-filters.md`
- C++ static/shared linking and parallel write configuration:
  `references/cpp-build-write.md`
- Current Java and C++ encoding/compression factory matrices:
  `references/encoding-compression.md`
- Query-by-row, timeseries metadata, C++ incomplete-file recovery and append,
  Java TsBlock writes, custom/buffered input, and Tablet object/size APIs:
  `references/advanced-apis.md`
- Python SDK code and binding-specific behavior: `references/python.md`
- C++ SDK code and resource management: `references/cpp.md`
- C wrapper entry points and lifecycle: `references/c.md`
- Version resolution, build requirements, and cross-version checks:
  `references/compatibility.md`
- Encoding, compression, throughput, memory, or storage tuning:
  `references/performance.md`

Do not read `references/performance.md` for ordinary API questions. Do not read
multiple language references unless the task explicitly crosses languages.

## Workflow

For writes, select the model and schema, prefer tablet/batch APIs, write data,
flush where required, close the writer, and reopen the result for validation.

For reads, identify the model and schema, select only needed columns, bound the
time range when possible, consume the result incrementally, and close all
resources.

For compatibility questions, report the local source version and the requested
release separately. Never silently combine signatures from different versions
or language bindings.

## Current Baseline Routing

- Use `references/dataframe-arrow.md` for `TsFileDataFrame`, pandas conversion,
  PyArrow `Table`/`RecordBatch`, or Arrow C Data batch paths.
- Use `references/properties-filters.md` for binary file properties or
  Table-model TAG predicate construction and composition.
- Use `references/cpp-build-write.md` for C++ library linkage or write
  concurrency. Configure the current source APIs; do not emit legacy aliases.
- Use `references/encoding-compression.md` before naming a factory or claiming
  that an encoding/type or compressor combination is constructible.
- Use `references/advanced-apis.md` for C++ crash recovery and append through
  `RestorableTsFileIOWriter`, plus the remaining current advanced read and write
  surfaces listed in Offline Reference Routing.

Treat these as baseline capabilities. Do not add alternate calls, degraded
results, or compatibility branches for earlier TsFile source lines unless the
user explicitly targets a different checkout or release.

## Java Tools

- Route `csv2tsfile`, `parquet2tsfile`, `arrow2tsfile`, schema-driven import,
  import auto-inference, directory import, and failed-file handling to the Java
  tools in `java/tools/`. Read `references/java-tools.md` before constructing or
  running a command.
- Route requests to inspect or backfill the persisted table-level point-count
  property to the Java `tsfile-table-point-count` tool. This operation can
  replace the input file when metadata is missing, so follow the mutation
  checks in `references/java-tools.md`.
- Do not substitute C++ `tsfile-cli count` for the Java point-count tool. The
  former reports per-series counts without changing the file; the latter
  validates and may backfill table-level metadata.

## Bundled Resources

- Run `scripts/resolve-version.sh [--root <checkout>]` to obtain Maven, C++,
  Python, and Git version metadata without loading or copying source files. Its
  output schema remains stable when no checkout is discovered and then reports
  the baseline shipped with this skill.
- When a compatible checkout is available, use maintained examples from the
  same commit: `java/examples/`, `python/examples/example.py`, or
  `cpp/examples/`. Do not copy an `assets/` template merely to answer an API
  question.
- When no compatible checkout is available and the user requests a starter
  project, copy only the needed files from `assets/`. Supply the target Java
  dependency as `-Dtsfile.version=<version>`; the template intentionally
  contains no default TsFile version.
- Run `scripts/validate-assets.sh [--root <checkout>]` after changing a
  template. It automatically uses a compatible checkout when found and always
  performs dependency-free checks. Explicitly select an external dependency
  with either `--tsfile-version <version>` or `--java-jar <path>` for Java,
  `--cpp-include <dir>` for C++, or `--python-runtime` for Python.
- Use `scripts/build_tsfile.sh` for repository language build checks.
- Use `scripts/example.py` only for Python API metadata or writer examples.
