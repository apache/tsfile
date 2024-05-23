/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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
#ifndef ENCODING_DICTIONARY_DECODER_H
#define ENCODING_DICTIONARY_DECODER_H

#include <map>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "encoding/bitpack_decoder.h"

namespace storage {

class DictionaryDecoder {
   private:
    BitPackDecoder value_decoder_;
    std::vector<std::string> entry_index_;

   public:
    void init() { value_decoder_.init(); }

    void reset() {
        value_decoder_.reset();
        entry_index_.clear();
    }

    std::string read_string(common::ByteStream &buffer) {
        if (entry_index_.empty()) {
            init_map(buffer);
        }
        int code = value_decoder_.read_int(buffer);
        return entry_index_[code];
    }

    bool has_next(common::ByteStream &buffer) {
        if (entry_index_.empty()) {
            init_map(buffer);
        }
        return value_decoder_.has_next(buffer);
    }

    int init_map(common::ByteStream &buffer) {
        int ret = common::E_OK;
        int length = 0;
        if (RET_FAIL(common::SerializationUtil::read_var_int(length, buffer))) {
            return common::E_PARTIAL_READ;
        }
        for (int i = 0; i < length; i++) {
            std::string str;
            if (RET_FAIL(common::SerializationUtil::read_str(str, buffer))) {
                return common::E_PARTIAL_READ;
            }
            entry_index_.push_back(str);
        }
        return ret;
    }
};

}  // end namespace storage
#endif  // ENCODING_DICTIONARY_DECODER_H
