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

require "yaml"
require "shellwords"
require "open3"

path = ARGV.fetch(0, File.expand_path("../../.github/workflows/native-packages.yml", __dir__))
workflow = YAML.load_file(path)
triggers = workflow["on"] || workflow[true] # Psych's YAML 1.1 boolean parsing
raise "Workflow must remain manual-only" unless triggers.keys == ["workflow_dispatch"]
raise "Workflow permissions must remain read-only" unless workflow["permissions"] == { "contents" => "read" }

jobs = workflow.fetch("jobs")
bootstrap = jobs.fetch("build-rpm").fetch("steps").find { |step| step["name"] == "Install RPM build prerequisites" }
packages = Shellwords.split(bootstrap.fetch("run").gsub("\\\n", " "))
raise "AlmaLinux 9 curl-minimal conflicts with full curl" if packages.include?("curl")
raise "RPM bootstrap must retain curl-minimal" unless packages.include?("curl-minimal")
raise "AlmaLinux 9 default repositories do not provide ninja-build" if packages.include?("ninja-build")
raise "RPM bootstrap must install make" unless packages.include?("make")

rpm_build = jobs.fetch("build-rpm").fetch("steps").filter_map { |step| step["run"] }.join("\n")
raise "RPM build must not require Ninja" if rpm_build.include?("-G Ninja")

windows = jobs.fetch("build-windows").fetch("steps").filter_map { |step| step["run"] }.join("\n")
raise "Windows ZIP needs a consistent static CRT" unless windows.include?("-DTSFILE_MSVC_STATIC_RUNTIME=ON") && windows.include?("-DTSFILE_DEPENDENCY_SOURCE=BUNDLED")
raise "All native target runtimes must be checked" unless windows.include?("CheckStaticMSVCRuntime.cmake")
raise "Both staged and extracted PE imports must be checked" unless windows.scan("python packaging/scripts/verify_windows_runtime.py").size == 2

%w[test-deb test-rpm build-windows].each do |name|
  steps = jobs.fetch(name).fetch("steps")
  raise "#{name} must check out the consumer fixture" unless steps.any? { |step| step["uses"].to_s.start_with?("actions/checkout@") }
  runs = steps.filter_map { |step| step["run"] }.join("\n")
  raise "#{name} must use the installed CMake consumer" unless runs.include?("-S cpp/cmake/tests/projects/InstalledConsumer")
end

jobs.each_value do |job|
  job.fetch("steps").each do |step|
    next unless step["run"] && step.fetch("shell", "bash") == "bash"
    output, status = Open3.capture2e("bash", "-n", stdin_data: step["run"])
    raise "Invalid shell in #{step['name']}: #{output}" unless status.success?
  end
end
puts "PASS: manual/read-only workflow, AlmaLinux bootstrap, Windows CRT checks, shared consumers, and Bash syntax"
