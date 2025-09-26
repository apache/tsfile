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

#ifndef SPRINTZ_ENCODER_H
#define SPRINTZ_ENCODER_H

#include <sstream>
#include <string>

#include "decoder.h"

namespace storage {
class SprintzEncoder : public Encoder {
   public:
    virtual ~SprintzEncoder() override = default;

    void set_predict_method(const std::string& method) {
        predict_method_ = method;
    }

    virtual void reset() {
        byte_cache_.reset();
        is_first_cached_ = false;
        group_num_ = 0;
    }

    virtual int get_one_item_max_size() = 0;

    virtual void bit_pack() = 0;

   protected:
    SprintzEncoder()
        : block_size_(8),
          group_max_(16),
          group_num_(0),
          bit_width_(0),
          byte_cache_(1024, common::MOD_ENCODER_OBJ),
          is_first_cached_(false),
          predict_method_("fire") {}

   protected:
    int block_size_;  // Size of each compressed block, default 8
    int group_max_;   // Maximum number of groups, default 16
    int group_num_;   // Current group count
    int bit_width_;   // Current bit width being used
    common::ByteStream byte_cache_{};
    std::string
        predict_method_{};  // Prediction method, e.g. "delta", "fire", etc.
    bool is_first_cached_;  // Whether the first value has been cached
};
}  // namespace storage

#endif  // SPRINTZ_ENCODER_H
