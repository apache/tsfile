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

# TsFile Source Policy

Apply this policy when choosing evidence, resolving versions, consulting the
website, or updating an offline reference. Use `docs-map.yaml` as the URL
registry; do not load it for ordinary offline API questions.

## Resolve Scope First

1. Classify the task as current-checkout development, a named release, an
   external project's dependency, or version-independent concepts.
2. For a checkout, read its root `pom.xml`, language package metadata, Git
   commit, and branch/tag. For an external project, inspect declared and locked
   dependencies before asking the user.
3. Treat `latest` website URLs as mutable V2.x navigation aliases, not release
   identifiers. Use the download/release sources in `docs-map.yaml` to discover
   published versions.

## Authority Order

Use the first source that matches the task and version:

1. **Exact API or behavior in a checkout:** public source/header, tests, and
   maintained examples from that same commit.
2. **A named release:** its tag/source, release notes, and published artifacts;
   then the matching official user-guide line.
3. **Build requirements:** the target checkout's build files and CI profiles;
   then release/download instructions.
4. **Concepts and model terminology:** version-matched official documentation
   or the corresponding local `docs/src/` source; use the offline skill archive
   when neither needs to be refreshed.
5. **Compatibility and operational guardrails:** tested local behavior and this
   skill's references, clearly labeled with their target source line.

Never let a remembered API, copied snippet, or mutable `latest` page override
version-matched source. Website code linked to `develop` is evidence for that
branch unless the same code is verified in the requested release.

## Offline and Online Use

- Work offline when the checkout plus one focused skill reference answers the
  task. Cite the resolved version and local path in the result when freshness
  matters.
- Consult an official URL only when the user requests current/published
  information, the target release is absent locally, a citation is required,
  or local sources conflict or are incomplete.
- Restrict authoritative online retrieval to `tsfile.apache.org`, Apache
  distribution links reached from it, and `github.com/apache/tsfile`.
- If network access is unavailable, continue with local sources and state the
  last locally verified scope; do not claim that a mutable page is current.

## Conflict Handling

1. Report each conflicting version/source explicitly.
2. Choose the source matching the requested artifact or checkout.
3. Do not combine constructors, method names, defaults, or dependencies across
   releases or language bindings.
4. If the target remains unknown and compilable code is required, stop and ask
   for the version. For conceptual guidance, state the assumed major line.
5. Treat ecosystem integration pages as leads that require dependency and
   connector-version verification because they may evolve independently.

## Deduplication and Updates

- Keep stable workflows, source-selection rules, and verified incompatibilities
  offline. Keep full tutorials, release inventories, and API catalogs on the
  official site/source tree.
- On a TsFile version change, recheck affected public APIs, examples, local
  docs, mapped URLs, and cross-language smoke behavior before editing a
  reference.
- Update `docs-map.yaml:last_verified` only after checking its official URLs.
  Do not record a discovered latest release as a permanent constant.
- Replace obsolete offline facts instead of appending historical sections.
  Preserve legacy guidance only when it has an explicit version scope.
