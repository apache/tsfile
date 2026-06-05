#!/bin/bash
#
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
#

set -euxo pipefail
if command -v yum >/dev/null 2>&1; then
  yum install -y wget tar gzip pkgconfig libuuid-devel libblkid-devel
elif command -v dnf >/dev/null 2>&1; then
  dnf install -y wget tar gzip pkgconfig libuuid-devel libblkid-devel
else
  echo "No supported package manager found (expected yum or dnf)." ; exit 1
fi
if (command -v dnf >/dev/null 2>&1 && dnf install -y java-17-openjdk-devel) \
   || (command -v yum >/dev/null 2>&1 && yum install -y java-17-openjdk-devel); then
  export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(command -v javac)")")")"
else
  ARCH="$(uname -m)"
  mkdir -p /opt/java
  if [ "$ARCH" = "x86_64" ]; then
    JDK_URL="https://download.oracle.com/java/17/archive/jdk-17.0.12_linux-x64_bin.tar.gz"
  else
    # aarch64
    JDK_URL="https://download.oracle.com/java/17/archive/jdk-17.0.12_linux-aarch64_bin.tar.gz"
  fi
  curl -L -o /tmp/jdk17.tar.gz "$JDK_URL"
  tar -xzf /tmp/jdk17.tar.gz -C /opt/java
  export JAVA_HOME=$(echo /opt/java/jdk-17.0.12*)
fi
export PATH="$JAVA_HOME/bin:$PATH"
java -version

chmod +x mvnw || true
./mvnw -Pwith-cpp clean package \
  -DskipTests -Dbuild.test=OFF \
  -Dspotless.check.skip=true -Dspotless.apply.skip=true
test -d cpp/target/build/lib && test -d cpp/target/build/include
