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

build_type=Debug
env_for_cyber=0
use_cpp11=1

if [[ ${env_for_cyber} -eq 1 ]]
then
  export PATH=$PATH:~/dev/gcc-linaro-5.5.0-2017.10-x86_64_arm-linux-gnueabi/bin
  export CROSS_COMPILE=arm-linux-gnueabi-
  export ARCH=arm
  export CC=${CROSS_COMPILE}gcc
  export CXX=${CROSS_COMPILE}g++
  echo "set up gcc for cyber"
fi


if [ ${build_type} = "Debug" ]
then
  mkdir -p build/Debug
  cd build/Debug
  use_sdk_debug=1
else
  mkdir -p build/Release
  cd build/Release
  use_sdk_debug=0
fi

echo "use_sdk_debug=${use_sdk_debug}"
cmake ../../  \
  -DUSE_SDK_DEBUG=$use_sdk_debug \
  -DUSE_CPP11=$use_cpp11
make
