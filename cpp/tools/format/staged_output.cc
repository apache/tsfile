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

#include "format/staged_output.h"

#include <iterator>

namespace tsfile_cli {

StagedOutput::StagedOutput() : file_(std::tmpfile()), stream_(this) {
    setp(buffer_, std::end(buffer_));
    if (file_ == nullptr) stream_.setstate(std::ios::badbit);
}

StagedOutput::~StagedOutput() {
    if (file_ != nullptr) std::fclose(file_);
}

int StagedOutput::sync() {
    if (file_ == nullptr) return -1;
    const size_t size = static_cast<size_t>(pptr() - pbase());
    if (std::fwrite(buffer_, 1, size, file_) != size) return -1;
    setp(buffer_, std::end(buffer_));
    return 0;
}

StagedOutput::int_type StagedOutput::overflow(int_type ch) {
    if (sync() != 0) return traits_type::eof();
    if (!traits_type::eq_int_type(ch, traits_type::eof())) {
        *pptr() = traits_type::to_char_type(ch);
        pbump(1);
    }
    return traits_type::not_eof(ch);
}

bool StagedOutput::publish(std::ostream& out) {
    if (file_ == nullptr) return false;
    stream_.flush();
    if (!stream_.good() || std::fflush(file_) != 0 ||
        std::fseek(file_, 0, SEEK_SET) != 0) {
        return false;
    }
    size_t size = 0;
    while ((size = std::fread(buffer_, 1, sizeof(buffer_), file_)) != 0) {
        out.write(buffer_, static_cast<std::streamsize>(size));
        if (!out.good()) return false;
    }
    if (std::ferror(file_)) return false;
    out.flush();
    return out.good();
}

}  // namespace tsfile_cli
