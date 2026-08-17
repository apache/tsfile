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
description: Work with Apache TsFile programmatic SDKs and file-format concepts in Java, Python, C++, or C. Use for reading, writing, querying, schema or data-model design, encoding/compression decisions, performance analysis, API compatibility, and cross-language TsFile integration. Route shell inspection, preview, export, sampling, and CSV/TSV conversion to the sibling tsfile-cli skill.
---

# TsFile

## Scope

Use this skill for SDK code, Tree/Table model decisions, schema design,
compatibility, and cross-language integration. Use the sibling
`../tsfile-cli/SKILL.md` for shell-oriented inspection, preview, export,
sampling, and CSV/TSV-to-TsFile conversion.

## Operating Rules

1. Determine the target version before choosing an API. For work in a TsFile
   checkout, read its root `pom.xml` and language package metadata. For an
   external project, inspect its declared dependency or ask for the target
   release.
2. Prefer public headers, tests, and maintained examples from the same checkout
   over remembered or copied APIs. Treat the offline references as routing and
   compatibility guidance, not as a substitute for version-matched source.
3. Choose Tree or Table model, then choose one language binding. Do not load all
   language references by default.
4. Work offline first. Consult the official website only when the user requests
   current published documentation or the local archive and source checkout do
   not answer the question.
5. Close writers, readers, and result sets so file footers and native resources
   are finalized. Validate files with every language that must consume them.

## Offline Reference Routing

Read only the files required by the current task:

- Model selection, schema, data types, and generic read/write workflow:
  `references/core-concepts.md`
- Java SDK code and current v4 API guardrails: `references/java.md`
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

## Bundled Resources

- Use `scripts/build_tsfile.sh` for repository language build checks.
- Use `scripts/example.py` only for Python API metadata or writer examples.
- Copy or adapt templates from `assets/` only when the user needs standalone
  starter code; otherwise prefer maintained examples in the current checkout.
