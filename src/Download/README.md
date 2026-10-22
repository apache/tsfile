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

# Releases Version

| Version | Dependency                                                                  | Sources                                                                                              | SHA512                                                                                                     | ASC                                                                                                  | Release Nodes                                                      | IoTDB adaptation version                                                         |
|---------|-----------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------| ---------------------------------------------------------------- |
| 2.5.0 | [pom](https://central.sonatype.com/artifact/org.apache.tsfile/tsfile/2.5.0) | [Source](https://www.apache.org/dyn/closer.cgi/tsfile/2.5.0/apache-tsfile-2.5.0-source-release.zip) | [sha512](https://www.apache.org/dyn/closer.cgi/tsfile/2.5.0/apache-tsfile-2.5.0-source-release.zip.sha512) | [asc](https://www.apache.org/dyn/closer.cgi/tsfile/2.5.0/apache-tsfile-2.5.0-source-release.zip.asc) | [Releases](https://dlcdn.apache.org/tsfile/2.5.0/RELEASE_NOTES.md) | IoTDB V2.x |
| 1.1.4 | [pom](https://central.sonatype.com/artifact/org.apache.tsfile/tsfile/1.1.4) | [Source](https://www.apache.org/dyn/closer.cgi/tsfile/1.1.4/apache-tsfile-1.1.4-source-release.zip) | [sha512](https://www.apache.org/dyn/closer.cgi/tsfile/1.1.4/apache-tsfile-1.1.4-source-release.zip.sha512) | [asc](https://www.apache.org/dyn/closer.cgi/tsfile/1.1.4/apache-tsfile-1.1.4-source-release.zip.asc) | [Releases](https://github.com/apache/tsfile/releases/tag/v1.1.4)   | IoTDB V1.x |

Legacy version are available here: https://archive.apache.org/dist/tsfile/
# Download
Our GitHub page: https://github.com/apache/tsfile

The latest version of TsFile is 2.5.0.

Downloading it from the [Maven central repository](https://search.maven.org/search?q=g:org.apache.tsfile)

Add the following dependency section to your pom.xml:

```
 <dependency>
   <groupId>org.apache.tsfile</groupId>
   <artifactId>tsfile</artifactId>
   <version>2.5.0</version>
 </dependency>
```

The release note of 2.5.0 can be found in the Archives of the Apache Software Foundation: https://dlcdn.apache.org/tsfile/2.5.0/RELEASE_NOTES.md

<a id="cpp-packages"></a>

## C/C++ binary packages (2.5.0)

Prebuilt packages do not require Maven, a JDK, or a local TsFile build. Choose packages according to your use case:

| Purpose | Ubuntu DEB | AlmaLinux RPM |
| --- | --- | --- |
| Run an application that uses TsFile | `tsfile`: shared runtime library | `tsfile`: shared runtime library |
| Develop C/C++ applications | `tsfile` + `tsfile-dev` | `tsfile` + `tsfile-devel` |
| Use the command-line tool | `tsfile` + `tsfile-tools` | `tsfile` + `tsfile-tools` |

Development packages provide headers, a CMake package, and pkg-config metadata; `tsfile-tools` provides `tsfile-cli`.
Homebrew, SDK archives, and Windows ZIPs already include the runtime, development files, and CLI.
Enabled codec dependencies are bundled, but compatible operating-system runtimes are still required.

### Downloads

| System / baseline | Architecture | Complete SDK (including CLI) |
| --- | --- | --- |
| Linux / glibc 2.28+ | x86_64 | [manylinux_2_28-x86_64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/manylinux_2_28-x86_64/tsfile-sdk-manylinux_2_28-x86_64-2.5.0.tar.gz) |
| Linux / glibc 2.28+ | aarch64 | [manylinux_2_28-aarch64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/manylinux_2_28-aarch64/tsfile-sdk-manylinux_2_28-aarch64-2.5.0.tar.gz) |
| macOS 12+ | Intel x86_64 | [macos12-x86_64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos12-x86_64/tsfile-sdk-macos12-x86_64-2.5.0.tar.gz) |
| macOS 12+ | Apple Silicon arm64 | [macos12-arm64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos12-arm64/tsfile-sdk-macos12-arm64-2.5.0.tar.gz) |
| macOS | Intel x86_64 | [macos-x86_64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos-x86_64/tsfile-sdk-macos-x86_64-2.5.0.tar.gz) |
| macOS | Apple Silicon arm64 | [macos-arm64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos-arm64/tsfile-sdk-macos-arm64-2.5.0.tar.gz) |
| Windows MSVC | x86_64 | [ZIP](https://packages.apache.org/artifactory/tsfile/windows/release/versions/2.5.0/tsfile-2.5.0-windows-x86_64.zip) |
| Windows MSVC | arm64 | [ZIP](https://packages.apache.org/artifactory/tsfile/windows/release/versions/2.5.0/tsfile-2.5.0-windows-arm64.zip) |

The `macos12-*` compatibility SDKs target a macOS 12 deployment baseline; the other macOS SDKs come from their native build environments.
Use the extraction command below for Linux/macOS SDKs rather than relying on an archive's top-level directory name.

| Native packages | Architecture | Runtime | Development | CLI |
| --- | --- | --- | --- | --- |
| Ubuntu 22.04 / 24.04 | amd64 | [tsfile](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-amd64/tsfile_2.5.0-1_amd64.deb) | [tsfile-dev](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-amd64/tsfile-dev_2.5.0-1_amd64.deb) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-amd64/tsfile-tools_2.5.0-1_amd64.deb) |
| Ubuntu 22.04 / 24.04 | arm64 | [tsfile](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-arm64/tsfile_2.5.0-1_arm64.deb) | [tsfile-dev](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-arm64/tsfile-dev_2.5.0-1_arm64.deb) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-arm64/tsfile-tools_2.5.0-1_arm64.deb) |
| AlmaLinux 9 | x86_64 | [tsfile](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/x86_64/tsfile-2.5.0-1.el9.x86_64.rpm) | [tsfile-devel](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/x86_64/tsfile-devel-2.5.0-1.el9.x86_64.rpm) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/x86_64/tsfile-tools-2.5.0-1.el9.x86_64.rpm) |
| AlmaLinux 9 | aarch64 | [tsfile](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/aarch64/tsfile-2.5.0-1.el9.aarch64.rpm) | [tsfile-devel](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/aarch64/tsfile-devel-2.5.0-1.el9.aarch64.rpm) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/aarch64/tsfile-tools-2.5.0-1.el9.aarch64.rpm) |

<a id="cpp-ubuntu"></a>

### Ubuntu 22.04 / 24.04 (DEB)

In an empty directory, download and verify the three packages for your architecture, then install the local files with APT.
Supported architectures are `amd64` and `arm64`; Ubuntu 24.04 uses the same Ubuntu 22.04-baseline packages.
Install only `tsfile` for runtime use; omit `tsfile-dev` if you only need the CLI.
No repository configuration or disabling of APT signature verification is required.

```bash
VERSION=2.5.0
ARCH=$(dpkg --print-architecture)
BASE="https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-${ARCH}"
for PACKAGE in tsfile tsfile-dev tsfile-tools; do
  FILE="${PACKAGE}_${VERSION}-1_${ARCH}.deb"
  curl --fail --location "$BASE/$FILE" --output "$FILE"
  printf '%s  %s\n' "$(curl --fail --silent --show-error "$BASE/$FILE.sha256")" "$FILE" \
    | sha256sum -c - || exit 1
done
sudo apt install "./tsfile_${VERSION}-1_${ARCH}.deb" \
  "./tsfile-dev_${VERSION}-1_${ARCH}.deb" \
  "./tsfile-tools_${VERSION}-1_${ARCH}.deb"
tsfile-cli --version
```

<a id="cpp-rpm"></a>

### AlmaLinux 9 (RPM)

Supported architectures are `x86_64` and `aarch64`. Download and verify the local RPMs before installing with DNF;
omit `tsfile-devel` if you only need the CLI. The EL9 package directories are not YUM metadata roots;
for repository configuration, the RPM metadata root is `https://packages.apache.org/artifactory/tsfile-rpm/`.
The local-file installation below does not modify repository configuration or disable DNF signature policy.

```bash
VERSION=2.5.0
ARCH=$(uname -m)
BASE="https://packages.apache.org/artifactory/tsfile-rpm/release/el9/${ARCH}"
for PACKAGE in tsfile tsfile-devel tsfile-tools; do
  FILE="${PACKAGE}-${VERSION}-1.el9.${ARCH}.rpm"
  curl --fail --location "$BASE/$FILE" --output "$FILE"
  printf '%s  %s\n' "$(curl --fail --silent --show-error "$BASE/$FILE.sha256")" "$FILE" \
    | sha256sum -c - || exit 1
done
sudo dnf install "./tsfile-${VERSION}-1.el9.${ARCH}.rpm" \
  "./tsfile-devel-${VERSION}-1.el9.${ARCH}.rpm" \
  "./tsfile-tools-${VERSION}-1.el9.${ARCH}.rpm"
tsfile-cli --version
```

<a id="cpp-homebrew"></a>

### macOS (Homebrew)

Import the published [Formula](https://packages.apache.org/artifactory/tsfile/homebrew/release/versions/2.5.0/Formula/tsfile.rb) into a local tap.
Published bottles are `arm64_tahoe` (macOS 26, Apple Silicon) and `sequoia` (macOS 15, Intel).
`--force-bottle` requires an available binary bottle; use an SDK below on other platforms instead of unexpectedly building from source.

```bash
brew tap-new --no-git local/tsfile250
FORMULA="$(brew --repository local/tsfile250)/Formula/tsfile.rb"
URL=https://packages.apache.org/artifactory/tsfile/homebrew/release/versions/2.5.0/Formula/tsfile.rb
curl --fail --location "$URL" --output "$FORMULA"
printf '%s  %s\n' "$(curl --fail --silent --show-error "$URL.sha256")" "$FORMULA" \
  | shasum -a 256 -c - || exit 1
brew trust --formula local/tsfile250/tsfile
brew install --force-bottle local/tsfile250/tsfile
TSFILE_HOME="$(brew --prefix local/tsfile250/tsfile)"
export PATH="$TSFILE_HOME/bin:$PATH"
tsfile-cli --version
```

This Formula is **keg-only**: use its `bin` directory rather than `brew link --force`, which can overwrite other packages' headers.
If an older `tsfile` is installed from another tap, plan its migration before following these new-install steps.
The `PATH` assignment only affects the current shell.

<a id="cpp-sdk"></a>

### Linux / macOS: SDK archives

Select `PLATFORM` from the table above. This example uses the Apple Silicon compatibility SDK;
use `manylinux_2_28-x86_64` or `manylinux_2_28-aarch64` on Linux, or `macos12-x86_64` on Intel Macs.
Run in an empty directory. Each archive contains a complete installation prefix; strip its top-level directory when extracting into `tsfile-sdk`.

```bash
VERSION=2.5.0
PLATFORM=macos12-arm64
ARCHIVE="tsfile-sdk-${PLATFORM}-${VERSION}.tar.gz"
URL="https://packages.apache.org/artifactory/tsfile/sdk/release/versions/${VERSION}/${PLATFORM}/${ARCHIVE}"
curl --fail --location "$URL" --output "$ARCHIVE"
SHA256="$(curl --fail --silent --show-error "$URL.sha256")"
if command -v sha256sum >/dev/null 2>&1; then
  printf '%s  %s\n' "$SHA256" "$ARCHIVE" | sha256sum -c - || exit 1
else
  printf '%s  %s\n' "$SHA256" "$ARCHIVE" | shasum -a 256 -c - || exit 1
fi
mkdir tsfile-sdk
tar -xzf "$ARCHIVE" -C tsfile-sdk --strip-components=1
export TSFILE_HOME="$PWD/tsfile-sdk"
export PATH="$TSFILE_HOME/bin:$PATH"
tsfile-cli --version
```

Preserve the relative layout of `bin/`, `lib/`, `include/`, and `share/`; do not copy just the CLI executable.
The standard SDK layout does not require a manual `LD_LIBRARY_PATH` or `DYLD_LIBRARY_PATH` setting.

<a id="cpp-windows"></a>

### Windows (ZIP)

Choose `x86_64` or `arm64` for your architecture and run in PowerShell in an empty directory:

```powershell
$Version = "2.5.0"
$Arch = "x86_64" # Use arm64 for Windows ARM64.
$Archive = "tsfile-$Version-windows-$Arch.zip"
$Url = "https://packages.apache.org/artifactory/tsfile/windows/release/versions/$Version/$Archive"
Invoke-WebRequest -Uri $Url -OutFile $Archive
$Expected = (Invoke-RestMethod -Uri "${Url}.sha256").Trim()
if ((Get-FileHash $Archive -Algorithm SHA256).Hash -ne $Expected) {
    throw "SHA-256 mismatch"
}
Expand-Archive -Path $Archive -DestinationPath .
$env:TSFILE_HOME = (Resolve-Path ".\tsfile-$Version-windows-$Arch").Path
$env:Path = "$env:TSFILE_HOME\bin;$env:Path"
tsfile-cli --version
```

The ZIP includes `bin/tsfile-cli.exe`, `bin/tsfile.dll`, headers, an import library, and CMake configuration;
do not separate the CLI from its DLL. Development requires a matching-architecture MSVC toolchain.
The published Windows library uses the Release `/MT` runtime. CMake consumers should use CMake 3.15+,
configure with `-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded`, and build with `--config Release`.

<a id="cpp-checksums"></a>

### Verification and next steps

Append `.sha256` to a binary download URL to obtain its SHA-256 checksum. The examples verify checksums before installation or extraction;
do not continue after a mismatch. A checksum from the same repository checks integrity and is not an independent PGP signature.
The release table above retains the SHA-512 and ASC links for source distributions.

- [C quick start](../UserGuide/latest/QuickStart/QuickStart-C.md)
- [C++ quick start](../UserGuide/latest/QuickStart/QuickStart-CPP.md)
- [TsFile CLI guide](../UserGuide/latest/Tools/Tsfile-CLI.md)
