/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef TSFILE_CLI_STAGED_OUTPUT_H
#define TSFILE_CLI_STAGED_OUTPUT_H

#include <cstdio>
#include <ostream>
#include <streambuf>

namespace tsfile_cli {

// Disk-backed output transaction: publish only after the input is fully read.
class StagedOutput : private std::streambuf {
   public:
    StagedOutput();
    ~StagedOutput() override;
    StagedOutput(const StagedOutput&) = delete;
    StagedOutput& operator=(const StagedOutput&) = delete;

    bool is_open() const { return file_ != nullptr; }
    std::ostream& stream() { return stream_; }
    bool publish(std::ostream& out);

   private:
    int sync() override;
    int_type overflow(int_type ch) override;

    std::FILE* file_;
    char buffer_[64 * 1024];
    std::ostream stream_;
};

}  // namespace tsfile_cli

#endif  // TSFILE_CLI_STAGED_OUTPUT_H
