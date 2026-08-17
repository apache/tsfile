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

# Version and Compatibility

Use this offline reference before selecting an API signature or diagnosing a
build/runtime mismatch.

## Resolve the Target

- Run `scripts/resolve-version.sh` for the checkout that bundles the skill, or
  `scripts/resolve-version.sh --root <checkout>` for another TsFile source tree.
- Check `source_mode` first. `standalone` is a successful degraded result, not
  a resolved TsFile version; its version fields intentionally remain
  `unavailable`.
- Use its Maven/Java, C++, Python, and Git fields independently; do not assume
  every language package uses an identical version string.
- If a language field is unavailable, inspect that binding's package/build
  metadata instead of substituting the Maven project version.
- External project: inspect Maven, Python, CMake, package-manager, lockfile, or
  deployed artifact metadata. Do not assume it matches this checkout.

The offline API references were audited against the v4 APIs in the checkout
that contained them. Re-audit them whenever the project version changes.

## Build Baseline for This Source Line

- Java: JDK 17 and Maven 3.6+; prefer the repository `./mvnw`.
- C++: CMake 3.11+, a C++11 compiler, make, clang-format, and platform UUID
  headers where required.
- Python: Python 3.9+ and the C++ module built by the `with-python` profile.

Repository checks:

```bash
./mvnw -P with-java clean verify
./mvnw -P with-cpp clean verify
./mvnw -P with-python clean verify
```

## Compatibility Guardrails

1. Prefer examples, tests, and public headers from the same commit.
2. Keep Java, Python, C++, and C signatures separate; similarly named methods
   may have different arguments, ownership, or return types.
3. Do not copy a snapshot version into an external project unless the artifact
   is installed or deployed where that project resolves dependencies.
4. Treat published `latest` documentation and the current source checkout as
   separate authorities when their versions differ.
5. Validate cross-language files using every required reader and representative
   null, time, text, date, and binary values.
6. Close writers, readers, and result sets to finalize file structures and
   release native resources.

## Troubleshooting Order

1. Confirm the library and file versions.
2. Reproduce with a maintained example from the same checkout.
3. Confirm the selected Tree/Table model and schema.
4. Check resource closure and native-library availability.
5. Reduce to a minimal file before attributing the failure to corruption.
