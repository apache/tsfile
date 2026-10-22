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

# 发行版本

| 版本    | 依赖引用                                                                        | 源代码                                                                                              | SHA512                                                                                                     | ASC                                                                                                  | 版本特性                                                               | IoTDB 适配版本                                                         |
|-------|-----------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------| ---------------------------------------------------------------- |
| 2.5.0 | [pom](https://central.sonatype.com/artifact/org.apache.tsfile/tsfile/2.5.0) | [源代码](https://www.apache.org/dyn/closer.cgi/tsfile/2.5.0/apache-tsfile-2.5.0-source-release.zip) | [sha512](https://www.apache.org/dyn/closer.cgi/tsfile/2.5.0/apache-tsfile-2.5.0-source-release.zip.sha512) | [asc](https://www.apache.org/dyn/closer.cgi/tsfile/2.5.0/apache-tsfile-2.5.0-source-release.zip.asc) | [Releases](https://dlcdn.apache.org/tsfile/2.5.0/RELEASE_NOTES.md) | IoTDB V2.x |
| 1.1.4 | [pom](https://central.sonatype.com/artifact/org.apache.tsfile/tsfile/1.1.4) | [源代码](https://www.apache.org/dyn/closer.cgi/tsfile/1.1.4/apache-tsfile-1.1.4-source-release.zip) | [sha512](https://www.apache.org/dyn/closer.cgi/tsfile/1.1.4/apache-tsfile-1.1.4-source-release.zip.sha512) | [asc](https://www.apache.org/dyn/closer.cgi/tsfile/1.1.4/apache-tsfile-1.1.4-source-release.zip.asc) | [Releases](https://github.com/apache/tsfile/releases/tag/v1.1.4)   | IoTDB V1.x |

完整历史版本: https://archive.apache.org/dist/tsfile/

# 最新版本

GitHub 仓库地址：https://github.com/apache/tsfile

最新版本是2.5.0。

点击 Maven 仓库地址下载：[Maven central repository](https://search.maven.org/search?q=g:org.apache.tsfile)

将以下依赖添加至 pom :

```
 <dependency>
   <groupId>org.apache.tsfile</groupId>
   <artifactId>tsfile</artifactId>
   <version>2.5.0</version>
 </dependency>
```

该版本的特性可点击查看: https://dlcdn.apache.org/tsfile/2.5.0/RELEASE_NOTES.md

<a id="cpp-packages"></a>

## C/C++ 二进制安装包（2.5.0）

使用预编译包不需要先安装 Maven、JDK 或编译 TsFile。运行库、开发包与 CLI 的区别如下：

| 用途 | Ubuntu DEB | AlmaLinux RPM |
| --- | --- | --- |
| 运行使用 TsFile 的程序 | `tsfile`：共享运行库 | `tsfile`：共享运行库 |
| 开发 C/C++ 应用 | `tsfile` + `tsfile-dev` | `tsfile` + `tsfile-devel` |
| 使用命令行工具 | `tsfile` + `tsfile-tools` | `tsfile` + `tsfile-tools` |

开发包提供头文件、CMake package 和 pkg-config 元数据；`tsfile-tools` 提供 `tsfile-cli`。
Homebrew、SDK 压缩包和 Windows ZIP 已包含运行库、开发文件和 CLI，无需分别安装。
预编译包已打包编译时启用的压缩算法依赖，但仍需要兼容的操作系统运行时。

### 下载入口

| 系统 / 基线 | 架构 | 完整 SDK（含 CLI） |
| --- | --- | --- |
| Linux / glibc 2.28+ | x86_64 | [manylinux_2_28-x86_64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/manylinux_2_28-x86_64/tsfile-sdk-manylinux_2_28-x86_64-2.5.0.tar.gz) |
| Linux / glibc 2.28+ | aarch64 | [manylinux_2_28-aarch64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/manylinux_2_28-aarch64/tsfile-sdk-manylinux_2_28-aarch64-2.5.0.tar.gz) |
| macOS 12+ | Intel x86_64 | [macos12-x86_64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos12-x86_64/tsfile-sdk-macos12-x86_64-2.5.0.tar.gz) |
| macOS 12+ | Apple Silicon arm64 | [macos12-arm64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos12-arm64/tsfile-sdk-macos12-arm64-2.5.0.tar.gz) |
| macOS | Intel x86_64 | [macos-x86_64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos-x86_64/tsfile-sdk-macos-x86_64-2.5.0.tar.gz) |
| macOS | Apple Silicon arm64 | [macos-arm64](https://packages.apache.org/artifactory/tsfile/sdk/release/versions/2.5.0/macos-arm64/tsfile-sdk-macos-arm64-2.5.0.tar.gz) |
| Windows MSVC | x86_64 | [ZIP](https://packages.apache.org/artifactory/tsfile/windows/release/versions/2.5.0/tsfile-2.5.0-windows-x86_64.zip) |
| Windows MSVC | arm64 | [ZIP](https://packages.apache.org/artifactory/tsfile/windows/release/versions/2.5.0/tsfile-2.5.0-windows-arm64.zip) |

`macos12-*` 是以 macOS 12 为部署基线的兼容 SDK；其他 macOS SDK 来自对应原生构建环境。
Linux/macOS SDK 使用下文统一解压命令，不依赖归档内部的顶层目录名。

| 原生安装包 | 架构 | 运行库 | 开发包 | CLI |
| --- | --- | --- | --- | --- |
| Ubuntu 22.04 / 24.04 | amd64 | [tsfile](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-amd64/tsfile_2.5.0-1_amd64.deb) | [tsfile-dev](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-amd64/tsfile-dev_2.5.0-1_amd64.deb) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-amd64/tsfile-tools_2.5.0-1_amd64.deb) |
| Ubuntu 22.04 / 24.04 | arm64 | [tsfile](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-arm64/tsfile_2.5.0-1_arm64.deb) | [tsfile-dev](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-arm64/tsfile-dev_2.5.0-1_arm64.deb) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-debian/pool/release/ubuntu22.04-arm64/tsfile-tools_2.5.0-1_arm64.deb) |
| AlmaLinux 9 | x86_64 | [tsfile](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/x86_64/tsfile-2.5.0-1.el9.x86_64.rpm) | [tsfile-devel](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/x86_64/tsfile-devel-2.5.0-1.el9.x86_64.rpm) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/x86_64/tsfile-tools-2.5.0-1.el9.x86_64.rpm) |
| AlmaLinux 9 | aarch64 | [tsfile](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/aarch64/tsfile-2.5.0-1.el9.aarch64.rpm) | [tsfile-devel](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/aarch64/tsfile-devel-2.5.0-1.el9.aarch64.rpm) | [tsfile-tools](https://packages.apache.org/artifactory/tsfile-rpm/release/el9/aarch64/tsfile-tools-2.5.0-1.el9.aarch64.rpm) |

<a id="cpp-ubuntu"></a>

### Ubuntu 22.04 / 24.04 (DEB)

以下命令在空目录中下载并校验本机架构的三个包，再通过 APT 安装本地文件。
支持 `amd64` 和 `arm64`；Ubuntu 24.04 使用同一套 Ubuntu 22.04 基线的包。
只运行应用时仅安装 `tsfile`；只使用 CLI 时可省略 `tsfile-dev`。
不需要添加软件源或关闭 APT 的签名检查。

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

支持 `x86_64` 和 `aarch64`。以下命令下载并校验本地 RPM，再使用 DNF 安装；
只使用 CLI 时可省略 `tsfile-devel`。这些文件位于 EL9 包目录，不是 YUM 元数据目录；
如需配置软件源，RPM 仓库的元数据根地址是 `https://packages.apache.org/artifactory/tsfile-rpm/`。
以下本地文件安装不修改仓库配置或关闭 DNF 的签名策略。

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

将已发布的 [Formula](https://packages.apache.org/artifactory/tsfile/homebrew/release/versions/2.5.0/Formula/tsfile.rb) 导入本地 tap。
以下是 Homebrew 7 的新安装步骤；它不会假定存在一个已维护的官方 TsFile tap。
已发布的 bottles 为 Apple Silicon 的 `arm64_tahoe`（macOS 26）和 Intel 的 `sequoia`（macOS 15）。
`--force-bottle` 要求可用的二进制包；不匹配的平台请使用下方 SDK，而不是意外触发源码构建。

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

该 Formula 为 **keg-only**：使用其 `bin` 路径，不要 `brew link --force` 覆盖其他软件的头文件。
若已从其他 tap 安装旧版 `tsfile`，应先规划迁移，不要将新安装命令当作无条件升级。
只对当前终端设置 `PATH` 不会修改 shell 启动文件。

<a id="cpp-sdk"></a>

### Linux / macOS：SDK 压缩包

从上表选择 `PLATFORM`。下面以 Apple Silicon 的兼容 SDK 为例；Linux 可改为
`manylinux_2_28-x86_64` 或 `manylinux_2_28-aarch64`，Intel Mac 可改为 `macos12-x86_64`。
在空目录中执行。归档包含完整安装前缀，统一移除其顶层目录后解压到 `tsfile-sdk`。

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

保留 `bin/`、`lib/`、`include/` 和 `share/` 的相对布局；不要只复制 CLI 可执行文件。
标准 SDK 布局下不需要手工设置 `LD_LIBRARY_PATH` 或 `DYLD_LIBRARY_PATH`。

<a id="cpp-windows"></a>

### Windows (ZIP)

根据本机架构选择 `x86_64` 或 `arm64`，在空目录的 PowerShell 中执行：

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

ZIP 包含 `bin/tsfile-cli.exe`、`bin/tsfile.dll`、头文件、导入库和 CMake 配置；
不要分离 CLI 和 DLL。开发应用需要匹配架构的 MSVC 工具链。
发布的 Windows 库采用 Release `/MT` 运行时；CMake 消费者使用 CMake 3.15+，
配置 `-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded` 并以 `--config Release` 构建。

<a id="cpp-checksums"></a>

### 校验与后续使用

每个二进制下载地址后加 `.sha256` 可获取其 SHA-256 校验值。上述示例在安装或解压前检查校验和；
校验失败时不要继续。与二进制来自同一仓库的校验和用于完整性检查，不应当冒充独立的 PGP 签名。
源码发行包的 SHA-512 和 ASC 链接仍见本页发行版本表。

- [C 快速上手](../UserGuide/latest/QuickStart/QuickStart-C.md)
- [C++ 快速上手](../UserGuide/latest/QuickStart/QuickStart-CPP.md)
- [TsFile CLI 使用指南](../UserGuide/latest/Tools/Tsfile-CLI.md)
