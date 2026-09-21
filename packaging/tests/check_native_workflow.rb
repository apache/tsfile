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

def run_text(job)
  job.fetch("steps").filter_map { |step| step["run"] }.join("\n")
end

%w[build-deb build-rpm build-windows].each do |name|
  text = run_text(jobs.fetch(name))
  raise "#{name} must build C++ through Maven" unless text.include?("-Pwith-cpp package")
  raise "#{name} must enable CPack through Maven" unless text.include?("-Denable.cpack=ON")
  raise "#{name} must use the generated CPack config" unless text.include?("cpp/target/build/CPackConfig.cmake")
  raise "#{name} must stage an SDK" unless text.include?("cmake --install cpp/target/build")
  raise "#{name} must use an absolute SDK prefix" unless text.include?("$PWD/sdk/") || text.include?('Join-Path $PWD "sdk/')
  if name != "build-windows"
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

go_job = jobs.fetch("test-go-linux")
raise "Go test must consume the Ubuntu SDK" unless go_job.fetch("needs") == "build-deb"
go_text = run_text(go_job)
raise "Go test must download the SDK artifact" unless go_job.fetch("steps").any? { |step| step["with"].to_h["name"] == "native-sdk-ubuntu22.04-amd64" }
raise "Go test must run go test" unless go_text.include?("go test ./...")
raise "Go SDK extraction must not fail on SIGPIPE" if go_text.include?("tar -tzf") && go_text.include?("head -n 1")

python_job = jobs.fetch("build-python-linux")
raise "Python wheel must consume the Ubuntu SDK" unless python_job.fetch("needs") == "build-deb"
python_text = run_text(python_job)
raise "Python wheel must use with-python-only" unless python_text.include?("-Pwith-python-only package")
raise "Python wheel must point at the downloaded SDK" unless python_text.include?("-Dtsfile.cpp.build")
raise "Python wheel must freeze source versions" unless python_text.include?("-Dtsfile.version.sync.skip=true")
raise "Python SDK extraction must not fail on SIGPIPE" if python_text.include?("tar -tzf") && python_text.include?("head -n 1")

homebrew = jobs.fetch("build-homebrew")
raise "Homebrew must use the current repository" unless homebrew.fetch("env").fetch("SOURCE_REPOSITORY") == "${{ github.repository }}"
homebrew_text = run_text(homebrew)
raise "Homebrew job must stage a macOS SDK from the installed formula" unless homebrew_text.include?("brew --prefix apache/tsfile-dev/tsfile-dev")
raise "Homebrew job must archive the staged macOS SDK" unless homebrew_text.include?("tsfile-sdk-${{ matrix.name }}-$ARCHIVE_VERSION")
raise "Homebrew SDK must be verified from its own staged tree" unless homebrew_text.include?('sdk_root="$PWD/sdk/$sdk_name"')
raise "Homebrew SDK must be archived from the staging root" unless homebrew_text.include?('tar -C sdk -czf "sdk/$sdk_name.tar.gz" "$sdk_name"')
raise "Homebrew prefix capture must tolerate extra stdout lines" unless homebrew_text.include?("brew --prefix apache/tsfile-dev/tsfile-dev | tail -n 1")
raise "Homebrew SDK verification must provide pkg-config" unless homebrew_text.include?("brew install pkgconf")
raise "Homebrew SDK step must keep bash 3.2 compatibility" if homebrew_text.include?("set -euo pipefail")
raise "Homebrew job must upload a macOS SDK artifact" unless homebrew.fetch("steps").any? { |step| step["with"].to_h["name"] == "native-sdk-macos-${{ matrix.name }}" }

homebrew_merge = run_text(jobs.fetch("merge-homebrew"))
trust = homebrew_merge.index("brew trust apache/tsfile-dev")
merge = homebrew_merge.index("brew bottle --merge")
raise "Homebrew merge must trust its temporary tap before loading the Formula" unless trust && merge && trust < merge

%w[test-deb test-rpm build-windows].each do |name|
  steps = jobs.fetch(name).fetch("steps")
  raise "#{name} must check out the consumer fixture" unless steps.any? { |step| step["uses"].to_s.start_with?("actions/checkout@") }
  runs = steps.filter_map { |step| step["run"] }.join("\n")
  raise "#{name} must use the installed CMake consumer" unless runs.include?("-S cpp/cmake/tests/projects/InstalledConsumer")
end

assemble = jobs.fetch("assemble")
needs = Array(assemble.fetch("needs"))
%w[test-deb test-rpm test-go-linux build-python-linux merge-homebrew build-windows].each do |name|
  raise "assemble must depend on #{name}" unless needs.include?(name)
end
assemble_text = run_text(assemble)
%w[native-sdk-ubuntu22.04-amd64 native-sdk-almalinux9-x86_64 native-sdk-windows-msvc-x86_64 native-sdk-macos-arm64 native-sdk-macos-x86_64 native-python-wheel-ubuntu22.04-x86_64].each do |name|
  raise "assemble must download #{name}" unless assemble.fetch("steps").any? { |step| step["with"].to_h["name"] == name }
end
raise "assemble must verify the bundle" unless assemble_text.include?("--verify-bundle")
raise "workflow must not hard-code a fork repository" if File.read(path).include?("ColinLeeo/tsfile")

jobs.each_value do |job|
  job.fetch("steps").each do |step|
    next unless step["run"] && step.fetch("shell", "bash") == "bash"
    output, status = Open3.capture2e("bash", "-n", stdin_data: step["run"])
    raise "Invalid shell in #{step['name']}: #{output}" unless status.success?
  end
end
puts "PASS: manual/read-only Maven build hierarchy, artifact consumers, workflow contracts, and shell syntax"
