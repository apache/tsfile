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

#include "bit_map.h"

#include "common/allocator/alloc_base.h"

namespace common {

BitMap::~BitMap() {
    if (bitmap_ && (size_ > 0)) {
        mem_free(bitmap_);
        bitmap_ = nullptr;
        size_ = 0;
    }
}

int BitMap::init(uint32_t item_size, bool init_as_zero) {
    uint32_t size = (item_size + 7) / 8;
    bitmap_ = static_cast<char*>(mem_alloc(size, MOD_TSBLOCK));
    // need set to 0, otherwise there will be wrong data
    const char initial_char = init_as_zero ? 0x00 : 0xFF;
    memset(bitmap_, initial_char, size);
    size_ = size;
    init_as_zero_ = init_as_zero;
    return common::E_OK;
}

}  // namespace common
