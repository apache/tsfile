# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

require "yaml"
require "shellwords"
require "open3"

path = ARGV.fetch(0, File.expand_path("../../.github/workflows/native-packages.yml", __dir__))
workflow = YAML.load_file(path)
triggers = workflow["on"] || workflow[true] # Psych's YAML 1.1 boolean parsing
raise "Workflow must remain manual-only" unless triggers.keys == ["workflow_dispatch"]
raise "Workflow permissions must remain read-only" unless workflow["permissions"] == { "contents" => "read" }

jobs = workflow.fetch("jobs")
raise "Workflow must be dedicated to release artifacts" unless workflow.fetch("name") == "Build native release artifacts"

def run_text(job)
  job.fetch("steps").filter_map { |step| step["run"] }.join("\n")
end

%w[build-deb build-deb-arm64 build-rpm build-rpm-aarch64 build-windows build-windows-arm64].each do |name|
  text = run_text(jobs.fetch(name))
  raise "#{name} must build C++ through Maven" unless text.include?("-Pwith-cpp package")
  raise "#{name} must bundle third-party dependencies" unless text.include?("-Dtsfile.dependency.source=BUNDLED")
  raise "#{name} must enable CPack through Maven" unless text.include?("-Denable.cpack=ON")
  raise "#{name} must use the generated CPack config" unless text.include?("cpp/target/build/CPackConfig.cmake")
  raise "#{name} must stage an SDK" unless text.include?("cmake --install cpp/target/build")
  raise "#{name} must use an absolute SDK prefix" unless text.include?("$PWD/sdk/") || text.include?('Join-Path $PWD "sdk/')
  unless name.start_with?("build-windows")
    raise "#{name} must locate pkg-config without assuming lib" unless text.include?('dirname "$sdk_pkgconfig"')
  end
  raise "#{name} must pass the generated archive version" unless text.include?("-Dtsfile.archive.version")
  raise "#{name} must freeze source versions" unless text.include?("-Dtsfile.version.sync.skip=true")
end

rpm_bootstrap = jobs.fetch("build-rpm").fetch("steps").find { |step| step["name"] == "Install RPM build prerequisites" }
packages = Shellwords.split(rpm_bootstrap.fetch("run").gsub("\\\n", " "))
raise "AlmaLinux 9 curl-minimal conflicts with full curl" if packages.include?("curl")
raise "RPM bootstrap must retain curl-minimal" unless packages.include?("curl-minimal")
raise "AlmaLinux 9 default repositories do not provide ninja-build" if packages.include?("ninja-build")
raise "RPM bootstrap must install make" unless packages.include?("make")
raise "RPM bootstrap must install Java" unless packages.include?("java-17-openjdk-devel")

windows = run_text(jobs.fetch("build-windows"))
raise "Windows ZIP needs a consistent static CRT" unless windows.include?("-Dtsfile.msvc.static.runtime=ON") && windows.include?("-Dtsfile.dependency.source=BUNDLED")
raise "Windows runtime check must be wired through Maven" unless windows.include?("CheckStaticMSVCRuntime.cmake") && windows.include?("-Dtsfile.project.include")
raise "Windows Maven properties must be quoted for PowerShell" unless windows.include?('"-Dcpp.toolchain=msvc"') && windows.include?('"-Dbuild.type=Release"')
raise "Both staged and extracted PE imports must be checked" unless windows.scan("python packaging/scripts/verify_windows_runtime.py").size == 2

raise "Ubuntu ARM64 packages must use a native ARM runner" unless jobs.fetch("build-deb-arm64").fetch("runs-on") == "ubuntu-22.04-arm"
raise "AlmaLinux ARM64 packages must use a native ARM runner" unless jobs.fetch("build-rpm-aarch64").fetch("runs-on") == "ubuntu-24.04-arm"
raise "Windows ARM64 binaries must use the pinned native ARM runner" unless jobs.fetch("build-windows-arm64").fetch("runs-on") == "windows-11-vs2026-arm"
windows_arm = run_text(jobs.fetch("build-windows-arm64"))
raise "Windows ARM64 build must use its Maven toolchain profile" unless windows_arm.include?("-Dcpp.toolchain=msvc-arm64-native")
raise "Windows ARM64 build must put the native compiler first on PATH" unless windows_arm.include?("HostARM64") && windows_arm.include?("Get-Command cl.exe")
raise "Windows ARM64 consumer must use the same Ninja Multi-Config toolchain" unless windows_arm.include?('-G "Ninja Multi-Config"')
raise "Windows ARM64 ZIP must have an architecture-specific name" unless windows_arm.include?("windows-arm64")
raise "Windows ARM64 must not add signed MSIX/AppX packaging" if windows_arm.match?(/msix|appx|signtool/i)
root_pom = File.read(File.expand_path("../../pom.xml", __dir__))
raise "Windows ARM64 Maven profile must use Ninja Multi-Config" unless root_pom.include?("<id>cpp-msvc-arm64-native</id>") && root_pom.include?("<cmake.generator>Ninja Multi-Config</cmake.generator>")

%w[test-deb-arm64 test-rpm-aarch64].each do |name|
  job = jobs.fetch(name)
  raise "#{name} must run on a native ARM runner" unless job.fetch("runs-on").end_with?("-arm")
  runs = run_text(job)
  raise "#{name} must build and run an installed CMake consumer" unless runs.include?("cpp/cmake/tests/projects/InstalledConsumer")
end

unix_python = jobs.fetch("build-python-unix")
raise "Unix wheels must use the prepared build identity" unless unix_python.fetch("needs") == "prepare"
unix_matrix = unix_python.fetch("strategy").fetch("matrix").fetch("include")
expected_unix_platforms = %w[manylinux_2_28-x86_64 manylinux_2_28-aarch64 macos12-x86_64 macos12-arm64]
raise "Unix wheel matrix must retain both architectures on Linux and macOS" unless unix_matrix.map { |row| row.fetch("name") }.sort == expected_unix_platforms.sort
raise "Linux ARM wheels must use a native ARM runner" unless unix_matrix.find { |row| row["name"] == "manylinux_2_28-aarch64" }.fetch("os") == "ubuntu-22.04-arm"
unix_build = unix_python.fetch("steps").find { |step| step["name"] == "Build wheels from one SDK per compatibility baseline" }
unix_env = unix_build.fetch("env")
%w[X86_64 AARCH64].each do |arch|
  raise "Linux wheels must retain manylinux_2_28" unless unix_env.fetch("CIBW_MANYLINUX_#{arch}_IMAGE") == "manylinux_2_28"
end
%w[LINUX MACOS].each do |platform|
  raise "#{platform} must build C++ once per wheel baseline" unless unix_env.fetch("CIBW_BEFORE_ALL_#{platform}").include?("build_python_sdk.sh")
  raise "#{platform} wheels must point at the staged SDK" unless unix_env.fetch("CIBW_ENVIRONMENT_#{platform}").include?("TSFILE_CPP_BUILD=")
end
raise "macOS wheels must retain the 12.0 deployment target" unless unix_env.fetch("MACOSX_DEPLOYMENT_TARGET") == "12.0"
raise "manylinux must export its SDK archive to the host" unless unix_env.fetch("CIBW_ENVIRONMENT_LINUX").include?("TSFILE_PYTHON_SDK_EXPORT=\"/host")

windows_python = jobs.fetch("build-python-windows")
raise "Windows wheels must reuse the native MSVC SDK" unless windows_python.fetch("needs") == "build-windows"
raise "Windows wheels must download the MSVC SDK" unless windows_python.fetch("steps").any? { |step| step["with"].to_h["name"] == "native-sdk-windows-msvc-x86_64" }
windows_python_text = run_text(windows_python)
windows_wheel_build = windows_python.fetch("steps").find { |step| step["run"].to_s.include?("python -m cibuildwheel") }
raise "Windows wheel runtime must come from the SDK" if windows_wheel_build.fetch("env").key?("TSFILE_MSVC_STATIC_RUNTIME")
raise "Windows wheels must adapt installed DLLs into the Python SDK view" unless windows_python_text.include?('Copy-Item "$sdkRoot/bin/*.dll" "$sdkRoot/lib/"')
raise "Windows wheels must point setup.py at the SDK" unless windows_python_text.include?("TSFILE_CPP_BUILD=")
raise "Windows wheel job must not rebuild the C++ core" if windows_python_text.include?("-Pwith-cpp")
raise "Windows wheels must use MSVC rather than MinGW" unless windows_python.fetch("steps").any? { |step| step["uses"].to_s.start_with?("ilammy/msvc-dev-cmd@") }

[unix_python, windows_python].each do |job|
  build = job.fetch("steps").find { |step| step["run"].to_s.include?("python -m cibuildwheel") }
  env = build.fetch("env")
  raise "Wheel matrix must retain CPython 3.9 through 3.14" unless env.fetch("CIBW_BUILD").split == %w[cp39-* cp310-* cp311-* cp312-* cp313-* cp314-*]
  raise "Wheel tests must run on separate installation jobs" unless env.fetch("CIBW_TEST_SKIP") == "*"
  raise "Wheel build jobs must not run package tests" if env.key?("CIBW_TEST_COMMAND")
  raise "Every wheel build must validate runtime-only contents" unless run_text(job).include?("packaging/scripts/check_python_wheel.py --wheelhouse wheelhouse")
end
sdk_builder_path = File.expand_path("../scripts/build_python_sdk.sh", __dir__)
sdk_builder = File.read(sdk_builder_path)
%w[-Pwith-cpp -Dtsfile.version.sync.skip=true -Dtsfile.dependency.source=BUNDLED -Denable.lzma2=ON].each do |option|
  raise "Python SDK builder must use #{option}" unless sdk_builder.include?(option)
end
raise "Python SDK builder must stage installed headers and libraries" unless sdk_builder.include?("cmake --install cpp/target/build")
raise "Python SDK export must support the container host mount" unless sdk_builder.include?("TSFILE_PYTHON_SDK_EXPORT")
raise "Windows SDKs must retain Python wheel codec coverage" unless windows.include?("-Denable.lzma2=ON")
output, status = Open3.capture2e("bash", "-n", sdk_builder_path)
raise "Invalid Python SDK shell script: #{output}" unless status.success?
raise "The old wheel workflow must be retired" if File.exist?(File.expand_path("../../.github/workflows/wheels.yml", __dir__))

homebrew = jobs.fetch("build-homebrew")
raise "Homebrew must use the current repository" unless homebrew.fetch("env").fetch("SOURCE_REPOSITORY") == "${{ github.repository }}"
homebrew_text = run_text(homebrew)
raise "Homebrew must build the release Formula" unless homebrew_text.include?("packaging/homebrew/tsfile.rb.in") && !homebrew_text.include?("tsfile-dev")
expected_bottle_root = "https://packages.apache.org/artifactory/tsfile/homebrew/release/versions/${{ needs.prepare.outputs.homebrew_version }}/bottles"
%w[build-homebrew merge-homebrew].each do |name|
  raise "#{name} must use the release bottle root" unless jobs.fetch(name).fetch("env").fetch("BOTTLE_ROOT_URL") == expected_bottle_root
end
raise "Homebrew bottles and SDKs must validate dylib dependencies" unless homebrew_text.scan("python3 packaging/scripts/verify_macos_runtime.py").size == 2
%w[tsfile.rb.in].each do |name|
  formula = File.read(File.expand_path("../homebrew/#{name}", __dir__))
  raise "#{name} must use bundled dependencies" unless formula.include?("-DTSFILE_DEPENDENCY_SOURCE=BUNDLED")
  dependencies = formula.lines.grep(/^\s*depends_on/)
  raise "#{name} must not install third-party runtime Formulae" unless dependencies.all? { |line| line.include?("=> :build") }
  raise "#{name} must bundle zlib" if formula.include?("uses_from_macos")
end
raise "Homebrew job must stage a macOS SDK from the installed formula" unless homebrew_text.include?("brew --prefix apache/tsfile/tsfile")
raise "Homebrew job must archive the staged macOS SDK" unless homebrew_text.include?("tsfile-sdk-${{ matrix.name }}-$ARCHIVE_VERSION")
raise "Homebrew SDK must be verified from its own staged tree" unless homebrew_text.include?('sdk_root="$PWD/sdk/$sdk_name"')
raise "Homebrew SDK must be archived from the staging root" unless homebrew_text.include?('tar -C sdk -czf "sdk/$sdk_name.tar.gz" "$sdk_name"')
raise "Homebrew prefix capture must tolerate extra stdout lines" unless homebrew_text.include?("brew --prefix apache/tsfile/tsfile | tail -n 1")
raise "Homebrew SDK verification must provide pkg-config" unless homebrew_text.include?("brew install pkgconf")
raise "Homebrew SDK step must keep bash 3.2 compatibility" if homebrew_text.include?("set -euo pipefail")
raise "Homebrew job must upload a macOS SDK artifact" unless homebrew.fetch("steps").any? { |step| step["with"].to_h["name"] == "native-sdk-${{ matrix.name }}" }
raise "Homebrew SDK must drop keg bookkeeping" unless homebrew_text.include?("INSTALL_RECEIPT.json") && homebrew_text.include?("sbom.spdx.json")
raise "Homebrew SDK must normalize the dylib identity" unless homebrew_text.include?("install_name_tool -id")
raise "Homebrew SDK must re-sign the normalized dylib" unless homebrew_text.include?("codesign --force --sign -")

homebrew_merge = run_text(jobs.fetch("merge-homebrew"))
trust = homebrew_merge.index("brew trust apache/tsfile")
merge = homebrew_merge.index("brew bottle --merge")
raise "Homebrew merge must trust its temporary tap before loading the Formula" unless trust && merge && trust < merge

%w[test-deb test-deb-arm64 test-rpm test-rpm-aarch64 build-windows build-windows-arm64].each do |name|
  steps = jobs.fetch(name).fetch("steps")
  raise "#{name} must check out the consumer fixture" unless steps.any? { |step| step["uses"].to_s.start_with?("actions/checkout@") }
  runs = steps.filter_map { |step| step["run"] }.join("\n")
  raise "#{name} must use the installed CMake consumer" unless runs.include?("-S cpp/cmake/tests/projects/InstalledConsumer")
end

prepare_steps = jobs.fetch("prepare").fetch("steps")
prepare_text = run_text(jobs.fetch("prepare"))
%w[--build-date --run-number --run-attempt].each do |option|
  raise "Release versions must not depend on a development build identity" if prepare_text.include?(option)
end
cli_data_upload = prepare_steps.find { |step| step["with"].to_h["name"] == "native-cli-test-data" }.fetch("with")
raise "CLI data must come from the current source checkout" unless cli_data_upload.fetch("path") == "python/tests/resources/*.tsfile"
raise "prepare must use one source checkout for code and CLI data" unless prepare_steps.count { |step| step["uses"].to_s.start_with?("actions/checkout@") } == 1

%w[test-deb test-deb-arm64 test-rpm test-rpm-aarch64 test-windows test-homebrew].each do |name|
  job = jobs.fetch(name)
  steps = job.fetch("steps")
  raise "#{name} must download the Python CLI test data" unless steps.any? { |step| step["with"].to_h["name"] == "native-cli-test-data" }
  raise "#{name} must run the installed CLI acceptance script" unless run_text(job).include?("packaging/tests/cli_package_smoke.py")
  checkout = steps.find { |step| step["uses"].to_s.start_with?("actions/checkout@") }.fetch("with")
  raise "#{name} must check out only its acceptance fixtures" if checkout.fetch("sparse-checkout").include?("cpp/src") || checkout.fetch("sparse-checkout").include?("cpp/tools")
  raise "#{name} must not rebuild TsFile" if run_text(job).include?("-Pwith-cpp")
end

%w[test-homebrew test-compatibility-sdk].each do |name|
  checkout = jobs.fetch(name).fetch("steps").find { |step| step["uses"].to_s.start_with?("actions/checkout@") }
  raise "#{name} must provide the macOS runtime checker for downloaded SDKs" unless checkout.fetch("with").fetch("sparse-checkout").include?("packaging/scripts/verify_macos_runtime.py")
end
raise "Installed Homebrew bottle must validate dylib dependencies" unless run_text(jobs.fetch("test-homebrew")).include?("python3 packaging/scripts/verify_macos_runtime.py")
windows_cli = jobs.fetch("test-windows")
raise "Windows CLI acceptance must wait for both ZIP architectures" unless windows_cli.fetch("needs").sort == %w[build-windows build-windows-arm64].sort
windows_cli_matrix = windows_cli.fetch("strategy").fetch("matrix").fetch("include")
raise "Windows CLI acceptance must cover x86_64 and ARM64" unless windows_cli_matrix.map { |row| row.fetch("name") }.sort == %w[windows-msvc-x86_64 windows-msvc-arm64].sort
raise "Windows CLI acceptance must install the ZIP" unless run_text(windows_cli).include?("Expand-Archive")
homebrew_cli = jobs.fetch("test-homebrew")
raise "Homebrew CLI acceptance must wait for the bottles" unless homebrew_cli.fetch("needs") == "build-homebrew"
raise "Homebrew CLI acceptance must cover both architectures" unless homebrew_cli.fetch("strategy").fetch("matrix").fetch("include").map { |row| row.fetch("name") }.sort == %w[macos-arm64 macos-x86_64].sort
raise "Homebrew CLI acceptance must install a bottle" unless run_text(homebrew_cli).include?("brew install --force-bottle") && run_text(homebrew_cli).include?("poured_from_bottle")
homebrew_install = run_text(homebrew_cli)
raise "Homebrew acceptance must install from its trusted tap" unless homebrew_install.include?("brew install --force-bottle apache/tsfile/tsfile")
raise "Homebrew acceptance must merge bottle checksums" unless homebrew_install.include?("brew bottle --merge --write --no-commit")
raise "Homebrew acceptance must seed the bottle download cache" unless homebrew_install.include?("brew --cache --force-bottle") && homebrew_install.include?('cp "${bottles[0]}" "$cached_bottle"')
raise "Homebrew acceptance must not bypass package path restrictions" if homebrew_install.include?("HOMEBREW_INTERNAL_ALLOW_PACKAGES_FROM_PATHS")
raise "Homebrew CLI acceptance must not build a bottle" if run_text(homebrew_cli).include?("--build-bottle")

# Every native target runs the real API regressions and Go's own tests after
# downloading and installing a package on a separate runner/container.
%w[test-deb test-deb-arm64 test-rpm test-rpm-aarch64 test-windows test-homebrew].each do |name|
  job = jobs.fetch(name)
  text = run_text(job)
  raise "#{name} must compile installed C++ API regressions" unless text.include?("packaging/tests/cpp_installed_tests.py")
  raise "#{name} must run Go's installed-SDK tests" unless text.include?("packaging/tests/go_installed_tests.py")
  raise "#{name} must set up Go" unless job.fetch("steps").any? { |step| step["uses"].to_s.start_with?("actions/setup-go@") }
  checkout = job.fetch("steps").find { |step| step["uses"].to_s.start_with?("actions/checkout@") }.fetch("with").fetch("sparse-checkout")
  %w[cpp/test go packaging/tests packaging/cmake/InstalledGTest].each do |required|
    raise "#{name} must check out #{required}" unless checkout.lines.map(&:strip).include?(required)
  end
end
windows_text = run_text(jobs.fetch("test-windows"))
raise "Windows cgo must use a verified MinGW compiler" unless windows_text.include?("LLVM-MinGW checksum mismatch") && windows_text.include?("--llvm-bin")
raise "Windows C++ tests must use the native MSVC environment" unless jobs.fetch("test-windows").fetch("steps").any? { |step| step["uses"].to_s.start_with?("ilammy/msvc-dev-cmd@") }

sdk_archive_jobs = {
  "test-deb" => "native-sdk-ubuntu22.04-amd64",
  "test-deb-arm64" => "native-sdk-ubuntu22.04-arm64",
  "test-rpm" => "native-sdk-almalinux9-x86_64",
  "test-rpm-aarch64" => "native-sdk-almalinux9-aarch64",
  "test-windows" => "native-sdk-${{ matrix.name }}",
  "test-homebrew" => "native-sdk-${{ matrix.name }}",
}
sdk_archive_jobs.each do |name, artifact|
  job = jobs.fetch(name)
  raise "#{name} must download its standalone SDK" unless job.fetch("steps").any? { |step| step["with"].to_h["name"] == artifact }
  raise "#{name} must test the extracted SDK" unless run_text(job).include?("packaging/tests/sdk_archive_tests.py")
end
compatibility_sdk = jobs.fetch("test-compatibility-sdk")
raise "Compatibility SDK tests must wait for wheel baseline builds" unless compatibility_sdk.fetch("needs").sort == %w[prepare build-python-unix].sort
raise "Compatibility SDK tests must cover every Unix baseline" unless compatibility_sdk.fetch("strategy").fetch("matrix").fetch("include").map { |row| row.fetch("name") }.sort == expected_unix_platforms.sort
raise "Compatibility SDK tests must exercise the extracted archive" unless run_text(compatibility_sdk).include?("packaging/tests/sdk_archive_tests.py")
raise "Compatibility SDK tests must not rebuild the product" if run_text(compatibility_sdk).match?(/with-cpp|build_python_sdk/)
compat_checkout = compatibility_sdk.fetch("steps").find { |step| step["uses"].to_s.start_with?("actions/checkout@") }.fetch("with").fetch("sparse-checkout")
raise "Compatibility SDK tests must exclude product sources" if compat_checkout.include?("cpp/src") || compat_checkout.include?("cpp/tools") || compat_checkout.include?("python/tsfile")

expected_versions = %w[3.9 3.10 3.11 3.12 3.13 3.14]
%w[test-python-unix test-python-windows].each do |name|
  job = jobs.fetch(name)
  expected_build = name == "test-python-unix" ? "build-python-unix" : "build-python-windows"
  raise "#{name} must wait for downloaded wheels" unless job.fetch("needs").sort == ["prepare", expected_build].sort
  raise "#{name} must cover every Python ABI" unless job.fetch("strategy").fetch("matrix").fetch("python-version") == expected_versions
  checkout = job.fetch("steps").find { |step| step["uses"].to_s.start_with?("actions/checkout@") }.fetch("with").fetch("sparse-checkout")
  raise "#{name} must check out Python tests only" unless checkout.lines.map(&:strip).sort == %w[packaging/scripts/check_python_wheel.py packaging/tests/python_installed_tests.py python/tests].sort
  raise "#{name} must run installed-wheel pytest" unless run_text(job).include?("packaging/tests/python_installed_tests.py")
  report = job.fetch("steps").find { |step| step["name"] == "Upload Python installation test results" }
  raise "#{name} must require the pytest report" unless report.fetch("with").fetch("if-no-files-found") == "error"
  raise "#{name} must consume the expected Python package version" unless run_text(job).include?("needs.prepare.outputs.python_version")
  raise "#{name} must download the built wheels" unless job.fetch("steps").any? { |step| step["with"].to_h["name"].to_s.start_with?("native-python-wheel-") }
  raise "#{name} must not compile native code" if run_text(job).match?(/cibuildwheel|with-cpp|build_ext/)
end
python_platforms = jobs.fetch("test-python-unix").fetch("strategy").fetch("matrix").fetch("platform")
raise "Independent Unix pytest must cover the four wheel platforms" unless python_platforms.map { |row| row.fetch("name") }.sort == expected_unix_platforms.sort
raise "Windows pytest must run on Windows" unless jobs.fetch("test-python-windows").fetch("runs-on") == "windows-2022"
cpp_test_entry = File.read(File.expand_path("../../cpp/test/InstalledSDKTests.cmake", __dir__))
raise "C++ regressions must link the installed CMake target" unless cpp_test_entry.include?("find_package(TsFile CONFIG REQUIRED)") && cpp_test_entry.include?("TsFile::tsfile")

assemble = jobs.fetch("assemble")
needs = Array(assemble.fetch("needs"))
%w[test-deb test-deb-arm64 test-rpm test-rpm-aarch64 test-windows test-homebrew test-compatibility-sdk test-python-unix test-python-windows build-python-unix build-python-windows merge-homebrew build-windows build-windows-arm64].each do |name|
  raise "assemble must depend on #{name}" unless needs.include?(name)
end
assemble_text = run_text(assemble)
%w[native-deb-ubuntu22.04-arm64 native-rpm-almalinux9-aarch64 native-windows-msvc-arm64 native-sdk-ubuntu22.04-amd64 native-sdk-ubuntu22.04-arm64 native-sdk-almalinux9-x86_64 native-sdk-almalinux9-aarch64 native-sdk-windows-msvc-x86_64 native-sdk-windows-msvc-arm64 native-sdk-macos-arm64 native-sdk-macos-x86_64 native-python-wheel-manylinux_2_28-x86_64 native-python-wheel-manylinux_2_28-aarch64 native-python-wheel-macos12-x86_64 native-python-wheel-macos12-arm64 native-python-wheel-windows-msvc-x86_64].each do |name|
  raise "assemble must download #{name}" unless assemble.fetch("steps").any? { |step| step["with"].to_h["name"] == name }
end
expected_unix_platforms.each do |platform|
  raise "assemble must download the #{platform} SDK" unless assemble.fetch("steps").any? { |step| step["with"].to_h["name"] == "native-sdk-#{platform}" }
end
raise "assemble must verify the bundle" unless assemble_text.include?("--verify-bundle")
raise "workflow must not hard-code a fork repository" if File.read(path).include?("ColinLeeo/tsfile")

jobs.each_value do |job|
  job.fetch("steps").each do |step|
    artifact = step["with"].to_h["name"].to_s
    raise "artifact names must not repeat the macOS matrix prefix" if artifact.include?("macos-macos")
    next unless step["run"] && step.fetch("shell", "bash") == "bash"
    output, status = Open3.capture2e("bash", "-n", stdin_data: step["run"])
    raise "Invalid shell in #{step['name']}: #{output}" unless status.success?
  end
end
puts "PASS: manual/read-only Maven build hierarchy, artifact consumers, workflow contracts, and shell syntax"
