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

1. Before giving a dependency version or version-sensitive API, run
   `scripts/resolve-version.sh`. Pass `--root <checkout>` when the target is not
   the repository that bundles this skill. A standalone skill download returns
   `source_mode=standalone` with unavailable version fields; inspect the target
   project's dependency metadata before giving version-sensitive code. Do not
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

- Run `scripts/resolve-version.sh [--root <checkout>]` to obtain Maven, C++,
  Python, and Git version metadata without loading or copying source files. Its
  output schema remains stable in standalone mode; check `source_mode` before
  using any version field.
- Use `scripts/build_tsfile.sh` for repository language build checks.
- Use `scripts/example.py` only for Python API metadata or writer examples.
- Copy or adapt templates from `assets/` only when the user needs standalone
  starter code; otherwise prefer maintained examples in the current checkout.
