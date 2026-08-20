# typed: strict
# frozen_string_literal: true

#
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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Formula for the Apache TsFile C++ library.
class Tsfile < Formula
  desc "Columnar storage library for time series data"
  homepage "https://tsfile.apache.org/"
  # Replace this development snapshot with the ASF source archive and its
  # release checksum when the first native TsFile release is published.
  url "https://github.com/apache/tsfile/archive/d4c3c94690cf9af819bc26bec7114a0de7900359.tar.gz"
  version "2.3.2"
  sha256 "369cf43742601fe6107b299647d2c01f705bfc9ff111402d7b5ddb9b50f40f16"
  license "Apache-2.0"
  head "https://github.com/apache/tsfile.git", branch: "develop"

  depends_on "cmake" => :build
  depends_on "lz4"
  depends_on "simde"
  depends_on "snappy"
  depends_on "utf8cpp"
  depends_on "zstd"

  uses_from_macos "zlib"

  def install
    args = %W[
      -DBUILD_TEST=OFF
      -DBUILD_TOOLS=ON
      -DENABLE_LZMA2=OFF
      -DTSFILE_DEPENDENCY_SOURCE=AUTO
      -DTSFILE_ENABLE_NATIVE_ARCH=OFF
      -DCMAKE_INSTALL_RPATH=#{rpath}
    ]

    system "cmake", "-S", "cpp", "-B", "build", *args, *std_cmake_args
    system "cmake", "--build", "build"
    system "cmake", "--install", "build"
  end

  test do
    (testpath / "test.cpp").write <<~CPP
      #include <tsfile/cwrapper/tsfile_cwrapper.h>

      int main() {
        return TS_DATATYPE_INT32 == 1 ? 0 : 1;
      }
    CPP

    system ENV.cxx, "-std=c++11", "test.cpp", "-I#{include}", "-L#{lib}",
           "-Wl,-rpath,#{lib}", "-ltsfile", "-o", "test"
    system "./test"
    system bin / "tsfile-cli", "--version"
  end
end
