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

# TsFile C++ Packaging and Installation Design

Status: Mailing-list discussion draft. A review PR for this document is not
intended to be merged; the mailing-list thread remains the canonical decision
record.

This document defines the intended installation and packaging contract for
the TsFile C++ library and command-line tools. It is the design proposed in the
[package-manager discussion][package-discussion]. The contract must be agreed
before public binary packages are produced because downstream packages make
library names, paths, and compatibility promises costly to change.

## Goals

- Provide a conventional, relocatable CMake installation.
- Give downstream consumers stable CMake and pkg-config interfaces.
- Define the public ABI, library version, and SONAME policy.
- Define logical runtime, development, and tools components while allowing
  each package manager to follow its own conventions.
- Generate local test packages and validate their complete lifecycle in CI.
- Keep every published package traceable to a community-approved Apache
  source release.

The work proceeds in stages. First, make the shared library, headers, CMake
package, pkg-config metadata, CLI, and notices install correctly without any
platform package. Second, validate that installation with independent
consumers. Third, build native Homebrew and Debian/Ubuntu packages from the
same install rules. RPM packaging follows after that workflow is stable.

Native packages contain the shared library and use dependencies supplied by
their package manager. Static `libtsfile` packages are not planned. A portable
all-in-one archive with pinned bundled dependencies is a separate distribution
profile, not a fallback for every platform without APT.

## Release and distribution boundary

The ASF source release remains the official Apache release. A package in
Homebrew, Debian, Fedora, or another downstream repository is a downstream
distribution of that release, not a separate Apache release.

Public packages must:

- be built from a source release approved by the project community;
- use a version that maps unambiguously to that source release;
- retain provenance such as the source URL and source checksum in the
  platform-specific packaging metadata;
- carry `LICENSE` and `NOTICE` files that describe the exact artifact
  contents, including bundled third-party software when applicable; and
- avoid presenting snapshots, nightly builds, or release candidates as a
  general-purpose installation channel.

CI may create packages from development revisions for validation. Those
artifacts are development-only, must include a revision identifier, and must
not be promoted as releases. Public testing channels and release-candidate
handling require a separate community decision.

These rules follow the [ASF Release Policy][asf-release-policy], the
[ASF third-party license policy][asf-third-party-policy], and the
[ASF trademark policy][asf-trademark-policy].

## Installation contract

Installation uses `GNUInstallDirs` and respects `CMAKE_INSTALL_PREFIX`,
`DESTDIR`, and platform-specific directory overrides.

| Artifact | Installation path |
| --- | --- |
| Shared library | `${CMAKE_INSTALL_LIBDIR}/libtsfile.*` |
| Windows runtime library | `${CMAKE_INSTALL_BINDIR}/tsfile.dll` |
| Windows import library | `${CMAKE_INSTALL_LIBDIR}/tsfile.lib` |
| Public and required transitive headers | `${CMAKE_INSTALL_INCLUDEDIR}/tsfile/...` |
| CMake package | `${CMAKE_INSTALL_LIBDIR}/cmake/TsFile/` |
| pkg-config metadata | `${CMAKE_INSTALL_LIBDIR}/pkgconfig/libtsfile.pc` |
| CLI | `${CMAKE_INSTALL_BINDIR}/tsfile-cli` |
| License and project notices | `${CMAKE_INSTALL_DATADIR}/doc/tsfile/` |

Headers are installed below `${CMAKE_INSTALL_INCLUDEDIR}/tsfile`, while the
include root exported to consumers is `${CMAKE_INSTALL_INCLUDEDIR}`. With
those two settings, consumer code uses:

```cpp
#include <tsfile/reader/tsfile_reader.h>
#include <tsfile/writer/tsfile_writer.h>
```

For example, the exported target may obtain its installed include root through
the following rule or an equivalent `INSTALL_INTERFACE` declaration:

```cmake
install(TARGETS tsfile
        EXPORT TsFileTargets
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
        ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        INCLUDES DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")
```

The implementation must replace the current build-tree staging directory in
`install(TARGETS)` with the standard destinations above. It must also install
headers through an explicit reviewed manifest. Copying every source header is
not a declaration that every internal class is public API.

### CMake consumer interface

The supported CMake interface is:

```cmake
find_package(TsFile CONFIG REQUIRED)
target_link_libraries(my_application PRIVATE TsFile::tsfile)
```

Consumers choose where CMake searches for the installed package through
`CMAKE_PREFIX_PATH` or `TsFile_DIR`. They do not manually add the TsFile header
directory with `target_include_directories()`:

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH=/opt/tsfile
```

The installation provides:

- `TsFileConfig.cmake`;
- `TsFileConfigVersion.cmake`;
- `TsFileTargets.cmake`; and
- the imported target `TsFile::tsfile`.

The same target name is available in the build tree as an alias. Its usage
requirements include the installed header directory, the required C++
standard, platform libraries, and any feature definitions visible from public
headers. Generated CMake files must be relocatable and must not contain source
or build directory paths.

### pkg-config consumer interface

The module name is `libtsfile`:

```bash
pkg-config --cflags --libs libtsfile
```

The name follows the installed library basename, `libtsfile`, and is less
likely to collide with a generic module called `tsfile`. There is no central
pkg-config registry. Before the first public package is submitted, maintainers
must recheck the target Homebrew and Linux package indexes, plus commonly used
public `.pc` module names, for conflicts. Consumer CI must use an isolated
installation prefix and verify that `pkg-config` resolves `libtsfile.pc` from
that prefix rather than from an unrelated preinstalled package.

`libtsfile.pc` contains the same public include and link requirements as the
CMake target. Dependencies used only inside the shared library are omitted.
A dependency whose types appear in public headers belongs in `Requires`; it
must not be hidden in `Requires.private`.

Its include declaration is conceptually:

```pkg-config
includedir=${prefix}/include
Cflags: -I${includedir}
```

The generated `Cflags` keeps `${includedir}` as its root and does not append
`/tsfile`. CMake and pkg-config consumers therefore obtain the same
`#include <tsfile/...>` form from the package metadata.

## Public API and ABI policy

The first release containing the completed installation metadata establishes
the packaged ABI baseline. The public API consists of:

- headers listed in the installation manifest as public entry points;
- public declarations reachable from those headers and required to use them;
- the C wrapper declarations installed for external consumers; and
- the `TsFile::tsfile` CMake target and `libtsfile` pkg-config module.

An implementation header may need to be installed because a public header
includes it. That does not make undocumented declarations in the
implementation header a supported API. The implementation work should add a
small set of documented umbrella or entry-point headers and reduce exposed
implementation details over time without breaking those entry points.

The compatibility rules are:

- Adding API without changing existing binary layouts is compatible.
- Removing or changing an exported function, symbol, class layout, virtual
  interface, enum value, or public data-member type is incompatible.
- Changing compiler flags or feature macros in a way that changes a public
  type layout is incompatible.
- An incompatible change requires a new ABI epoch and therefore a new
  SONAME. Source deprecation alone does not permit an ABI break within an
  epoch.
- Platform toolchain ABI boundaries still apply. Portable C++ archives must
  identify their operating system, architecture, compiler/runtime baseline,
  and minimum supported operating-system version.

The C wrapper remains part of `libtsfile` for the first milestone; it does not
become a second runtime library. Splitting it later would require its own ABI
and SONAME decision.

### Library version and SONAME

The release version and ABI epoch are separate values:

```cmake
set(TSFILE_VERSION <approved-source-release-version>)
set(TSFILE_ABI_VERSION <initial-abi-epoch>)

set_target_properties(tsfile PROPERTIES
    VERSION   "${TSFILE_VERSION}"
    SOVERSION "${TSFILE_ABI_VERSION}")
```

`TSFILE_VERSION` identifies the source release. `TSFILE_ABI_VERSION` is a
positive integer changed only for an incompatible ABI change. Before selecting
the initial value, implementation work must inspect the SONAME or install name
produced by recent releases, identify known binary consumers, and establish a
baseline from the installed public headers and exported symbols. If no stable
binary compatibility promise exists, ABI epoch `1` is the recommended initial
value. If an existing binary contract must be preserved, the initial value and
transition plan must reflect it rather than silently relabeling the library.

For example, if epoch `1` is selected, a `2.4.0` build installs
`libtsfile.so.2.4.0` with SONAME `libtsfile.so.1` on ELF platforms.
Development suffixes such as `.dev` must never appear in a SONAME.

This replaces the current behavior where the complete project version is also
used as `SOVERSION`. Keeping the ABI epoch independent avoids changing the
runtime package name for every feature or patch release.

Before the baseline is declared, CI must compare builds with supported feature
profiles and remove configuration-dependent layouts from public headers where
possible. In particular, build-only and test-only definitions must not alter
the installed ABI. Required feature state should be expressed through an
installed generated configuration header and propagated by the consumer
metadata.

## Shared-library scope

The packaging contract supports one shared target, `tsfile`. Public binary
packages must use a fixed, documented feature profile so that packages with
the same SONAME have the same public ABI.

There is no plan to publish a static `libtsfile`. Native packages and the
portable all-in-one archive both expose a shared library. A future request for
a public static library requires a separate community proposal covering a
concrete use case, dependency availability, security updates, symbol
visibility, and license aggregation.

## Dependency policy

Native Homebrew, Debian/Ubuntu, Fedora, and EPEL packages should use system
dependencies where the platform provides a supported version. This allows the
platform to deliver security updates and avoids shipping duplicate libraries.
The build must therefore gain an explicit provider mode rather than selecting
bundled code implicitly:

```text
TSFILE_DEPENDENCY_PROVIDER=system|bundled
```

`system` is required for every native downstream package, including Homebrew,
DEB, and RPM. A dependency unavailable on a target platform may be disabled or
handled in that platform's packaging review; it must not silently fall back to
a bundled copy.

`bundled` is intended for reproducible local builds and the portable all-in-one
profile. Its dependency versions and complete licensing material must be
pinned and audited. The two provider modes produce distinct artifacts and must
not be mixed in one package build.

The package configuration must expose only dependencies required by consumers.
Private compression and parser implementations linked into the shared library
must not leak source-tree include paths or unnecessary link flags into
`TsFile::tsfile` or `libtsfile.pc`.

## Native package components

CPack and CI use three logical components:

| Component | Contents | Example native package names |
| --- | --- | --- |
| `runtime` | Versioned shared library and required notices | Debian: `libtsfile1`; RPM: `tsfile-libs` |
| `development` | Headers, unversioned linker name/import library, CMake config, pkg-config file | Debian: `libtsfile-dev`; RPM: `tsfile-devel` |
| `tools` | `tsfile-cli` and required notices | `tsfile-tools` |

The numeric Debian runtime suffix follows the ABI epoch, not the source release
major version. Exact names remain platform packaging decisions.

Homebrew should use one formula containing the runtime, development files, and
CLI unless Homebrew review establishes a reason to split them. Official
distribution repositories use their native packaging files. CPack-generated
DEB or RPM output is a local validation aid and is not submitted in place of a
formula, Debian packaging, or an RPM spec. The CPack TGZ profile described
below has a different purpose: it produces the portable all-in-one archive.

## Portable all-in-one archive

An all-in-one archive serves systems where a suitable native package is not
available or installing development dependencies is impractical. It is not
selected merely because a platform does not use APT. DNF/YUM, Homebrew, and
other capable native package managers still use the `system` dependency
profile.

A portable archive contains:

```text
apache-tsfile-cpp-<version>-<os>-<arch>/
├── bin/tsfile-cli
├── lib/libtsfile.<shared-library-suffix>
├── include/tsfile/...
├── lib/cmake/TsFile/...
├── lib/pkgconfig/libtsfile.pc
├── LICENSE
└── NOTICE
```

Pinned third-party dependencies may be linked privately into the shared
`libtsfile` or placed in a private library directory. Third-party symbols must
not leak into the public ABI, and any private shared libraries must use
origin-relative runtime lookup paths. The archive must not bundle foundational
system runtimes such as glibc.

Each archive is specific to an operating system, architecture, compiler/C++
runtime baseline, and minimum operating-system version. It must be relocatable,
run without development packages installed, and include exact third-party
`LICENSE` and `NOTICE` content. A dependency security update requires rebuilding
and republishing the complete archive.

The all-in-one archive is a convenience binary built from an approved source
release. It does not replace the ASF source release and does not contain a
public static `libtsfile`.

## CPack scope

CPack uses separate packaging profiles:

- `DEB` uses `TSFILE_DEPENDENCY_PROVIDER=system` and the runtime, development,
  and tools components.
- `TGZ` produces one portable all-in-one archive with
  `TSFILE_DEPENDENCY_PROVIDER=bundled` after its relocation, symbol, and license
  checks are available.
- `RPM` uses `TSFILE_DEPENDENCY_PROVIDER=system` and is enabled after RPM CI and
  package conventions are ready.

Package metadata includes the source release version, project URL, license
identifier, architecture, component dependencies where applicable, and package
maintainer contact agreed by the community.

CPack configuration must be owned by the TsFile top-level project. The bundled
ANTLR build currently calls `include(CPack)`, which can produce a top-level
configuration containing ANTLR metadata. Dependency CPack configuration must be
disabled or isolated before TsFile packages are generated.

## CI acceptance criteria

Packaging changes are complete only when CI verifies:

1. A clean release build with tests and examples excluded from installed
   artifacts.
2. Installation to a temporary, non-default prefix using `DESTDIR`, with both
   the minimum supported CMake version and `cmake --install` on newer CMake
   versions.
3. An independent CMake consumer that calls `find_package(TsFile CONFIG
   REQUIRED)`, links `TsFile::tsfile` without manually adding an include
   directory, verifies that the imported include root is `<prefix>/include`
   rather than `<prefix>/include/tsfile`, compiles with the resulting
   `<tsfile/...>` include form, and reads and writes a small TsFile.
4. An independent pkg-config consumer that verifies the same include root,
   uses the same `#include <tsfile/...>` spelling, and compiles and runs the
   same smoke operation.
5. Each public entry-point header compiles in an isolated translation unit
   using only the usage requirements provided by the installed package.
6. CLI execution, including `tsfile-cli --version`.
7. Package creation, installation on a clean image, upgrade from the previous
   compatible release when one exists, removal, and a check for unexpected
   remaining files.
8. Runtime/development/tools component dependencies and file ownership in
   native packages.
9. Native packages link to package-manager dependencies and do not install
   private copies of them.
10. The portable archive runs in a clean environment without development
   dependencies, remains relocatable, and resolves any bundled libraries only
   from inside the archive.
11. Absence of source/build paths, leaked third-party symbols, and unaccounted
   `LICENSE` or `NOTICE` content.
12. SONAME, symlink, exported-symbol, and ABI checks against the previous
   release in the same ABI epoch.

The initial native-package matrix covers macOS with Homebrew dependencies and a
supported Debian/Ubuntu release. A Linux all-in-one job is added only after the
bundled-dependency and license checks are ready. Fedora/RPM jobs are added
before RPM artifacts or repositories are advertised.

## Incremental implementation

The implementation should be split into reviewable changes:

1. Agree on this installation, naming, ABI, and component contract.
2. Add standard install destinations, the `tsfile/`-namespaced public-header
   layout and manifest, versioned shared-library rules, CMake export files,
   and pkg-config metadata.
3. Add independent CMake and pkg-config consumers plus install/uninstall and
   ABI checks.
4. Add explicit, isolated system/bundled dependency provider selection.
5. Add native `DEB` packaging and a Homebrew test formula using system
   dependencies, plus their lifecycle CI.
6. Add the portable all-in-one `TGZ` profile using pinned bundled dependencies,
   plus relocation, symbol, license, and clean-runtime checks.
7. Add native RPM packaging with system dependencies when maintainers and user
   demand justify it.
8. Discuss ownership, credentials, retention, and release-candidate policy
   before creating any public testing repository.

[package-discussion]: https://lists.apache.org/thread/cjd67hv8n1kwy2023r0906wjbgn0mfkb
[asf-release-policy]: https://www.apache.org/legal/release-policy.html
[asf-third-party-policy]: https://www.apache.org/legal/resolved.html
[asf-trademark-policy]: https://www.apache.org/foundation/marks/
