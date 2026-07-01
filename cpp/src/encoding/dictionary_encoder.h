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

#ifndef ENCODING_DICTIONARY_ENCODER_H
#define ENCODING_DICTIONARY_ENCODER_H

#include <map>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoder.h"
#include "encoding/int32_rle_encoder.h"

namespace storage {

class DictionaryEncoder : public Encoder {
   private:
    std::map<std::string, int> entry_index_;
    std::vector<std::string> index_entry_;
    Int32RleEncoder values_encoder_;
    int map_size_;

   public:
    DictionaryEncoder() {}
    ~DictionaryEncoder() override {}

    int encode(bool value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int32_t value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(int64_t value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(float value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(double value, common::ByteStream &out_stream) override {
        return common::E_TYPE_NOT_MATCH;
    }
    int encode(common::String value, common::ByteStream &out_stream) override {
        encode(value.to_std_string(), out_stream);
        return common::E_OK;
    }

    void init() {
        map_size_ = 0;
        values_encoder_.init();
    }

    void destroy() override {}

    void reset() override {
        entry_index_.clear();
        index_entry_.clear();
        map_size_ = 0;
        values_encoder_.reset();
    }

    int encode(const char *value, common::ByteStream &out) {
        return encode(std::string(value), out);
    }

    int encode(std::string value, common::ByteStream &out) {
        if (entry_index_.count(value) == 0) {
            index_entry_.push_back(value);
            map_size_ = map_size_ + value.length();
            entry_index_[value] = entry_index_.size();
        }
        values_encoder_.encode(entry_index_[value], out);
        return common::E_OK;
    }

    int flush(common::ByteStream &out) override {
        int ret = common::E_OK;
        ret = write_map(out);
        if (ret != common::E_OK) {
            return ret;
        } else {
            write_encoded_data(out);
        }
        return ret;
    }

    int write_map(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_var_int(
                (int)index_entry_.size(), out))) {
            return ret;
        } else {
            for (int i = 0; i < (int)index_entry_.size(); i++) {
                if (RET_FAIL(common::SerializationUtil::write_var_str(
                        index_entry_[i], out))) {
                    return common::E_FILE_WRITE_ERR;
                }
            }
        }
        return common::E_OK;
    }

    void write_encoded_data(common::ByteStream &out) {
        values_encoder_.flush(out);
    }

    int get_max_byte_size() override {
        // 4 bytes for storing dictionary size
        return 4 + map_size_ + values_encoder_.get_max_byte_size();
    }
};

}  // end namespace storage
#endif  // ENCODING_DICTIONARY_ENCODER_H