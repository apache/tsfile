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

#ifndef SPRINTZ_DECODER_H
#define SPRINTZ_DECODER_H

#include <cstdint>
#include <iostream>
#include <sstream>
#include <memory>
#include <string>
#include <vector>
#include <stdexcept>
#include <istream>

#include "decoder.h"

namespace storage {

class SprintzDecoder : public Decoder {
public:
    ~SprintzDecoder() override = default;

    // 重置解码状态
    void reset() override {
        is_block_readed_ = false;
        current_count_ = 0;
    }

    // 解码一个压缩块（由子类实现）
    virtual void decode_block(common::ByteStream &in) = 0;

    // 根据已解码数据更新预测器（由子类实现）
    virtual void recalculate() = 0;

protected:
    SprintzDecoder()
        : bit_width_(0),
          block_size_(8),
          is_block_readed_(false),
          current_count_(0),
          decode_size_(0) {
    }

protected:
    int bit_width_;         // 当前使用的比特宽度
    int block_size_;        // 默认 8
    bool is_block_readed_;  // 当前块是否已读取
    int current_count_;     // 当前解码的位置
    int decode_size_;       // 当前解码块中有效数据个数
};

} // namespace storage

#endif // SPRINTZ_DECODER_H
