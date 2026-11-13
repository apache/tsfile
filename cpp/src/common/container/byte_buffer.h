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
#ifndef COMMON_CONTAINER_BYTE_BUFFER_H
#define COMMON_CONTAINER_BYTE_BUFFER_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
#include <cstring>

#include "common/allocator/alloc_base.h"
#include "common/global.h"
#include "utils/util_define.h"

namespace common {
class ByteBuffer {
   public:
    ByteBuffer()
        : data_(nullptr),
          variable_type_len_(sizeof(uint32_t)),
          real_data_size_(0),
          reserved_size_(0) {}

    ~ByteBuffer() {
        // A zero-sized buffer still owns the allocator's header block, so the
        // decision to free must key off the pointer, not the reserved size.
        if (data_ != nullptr) {
            mem_free(data_);
            data_ = nullptr;
            real_data_size_ = 0;
            reserved_size_ = 0;
        }
    }

    FORCE_INLINE void init(uint32_t size) {
        data_ = static_cast<char*>(mem_alloc(size, MOD_TSBLOCK));
        reserved_size_ = size;
    }

    FORCE_INLINE void reset() { real_data_size_ = 0; }

    FORCE_INLINE int extend_memory(uint32_t new_size) {
        ASSERT(new_size > reserved_size_);
        char* new_data = static_cast<char*>(mem_realloc(data_, new_size));
        if (UNLIKELY(new_data == nullptr)) {
            return E_OOM;
        }
        data_ = new_data;
        reserved_size_ = new_size;
        return E_OK;
    }

    FORCE_INLINE int append_variable_value(const char* value, uint32_t len) {
        // dynamic growth
        if (UNLIKELY((real_data_size_ + len + variable_type_len_) >
                     reserved_size_)) {
            // extreme scenarios, when encountering very long string
            uint32_t growth_size =
                g_config_value_.tsblock_mem_inc_step_size_ > len
                    ? g_config_value_.tsblock_mem_inc_step_size_
                    : (len + 1);
            if (UNLIKELY(extend_memory(reserved_size_ + growth_size) != E_OK)) {
                return E_OOM;
            }
        }

        ASSERT(data_);
        // append len
        memcpy(&data_[real_data_size_], reinterpret_cast<char*>(&len),
               variable_type_len_);
        real_data_size_ += variable_type_len_;
        if (len > 0) {
            // append data
            memcpy(&data_[real_data_size_], value, len);
            real_data_size_ += len;
        }
        return E_OK;
    }

    FORCE_INLINE int append_fixed_value(const char* value, uint32_t len) {
        // dynamic growth
        if (UNLIKELY(real_data_size_ + len > reserved_size_)) {
            // extreme scenarios, when encountering very long string
            uint32_t growth_size =
                g_config_value_.tsblock_mem_inc_step_size_ > len
                    ? g_config_value_.tsblock_mem_inc_step_size_
                    : (len + 1);
            if (UNLIKELY(extend_memory(reserved_size_ + growth_size) != E_OK)) {
                return E_OOM;
            }
        }

        ASSERT(data_);
        memcpy(&data_[real_data_size_], value, len);
        real_data_size_ += len;
        return E_OK;
    }

    // for fixed len value
    FORCE_INLINE char* read(uint32_t offset, uint32_t len) {
        ASSERT((offset + len) <= real_data_size_);
        char* p = &data_[offset];
        return p;
    }

    // for variable len value
    FORCE_INLINE char* read(uint32_t offset, uint32_t* len) {
        ASSERT(offset + variable_type_len_ <= real_data_size_);
        uint32_t tmp;
        std::memcpy(&tmp, data_ + offset, sizeof(tmp));
        *len = tmp;
        ASSERT(offset + variable_type_len_ + *len <= real_data_size_);
        char* p = &data_[offset + variable_type_len_];
        return p;
    }

    FORCE_INLINE char* get_data() { return data_; }

    FORCE_INLINE uint32_t get_data_size() const { return real_data_size_; }

    // Append @p count copies of one variable-length value. A block's TAG column
    // repeats the same value on every row; writing the pattern once and then
    // doubling the filled region beats @p count individual append calls.
    FORCE_INLINE int fill_repeated_variable(const char* value, uint32_t len,
                                            uint32_t count) {
        if (count == 0) {
            return E_OK;
        }
        // The stored length prefix is copied from a uint32_t, so the width
        // must not exceed it (the constructor fixes it to sizeof(uint32_t)).
        if (variable_type_len_ > sizeof(uint32_t)) {
            return E_INVALID_ARG;
        }
        const uint64_t elem_size =
            static_cast<uint64_t>(variable_type_len_) + len;
        // elem_size * count can overflow uint64_t (both factors are 32-bit),
        // which would wrap `need` to a small value and bypass the capacity
        // checks before a huge payload copy. Reject anything that cannot fit
        // in the 32-bit buffer size first.
        const uint64_t available =
            static_cast<uint64_t>(0xFFFFFFFFULL) - real_data_size_;
        if (count > available / elem_size) {
            return E_OOM;
        }
        const uint64_t need =
            static_cast<uint64_t>(real_data_size_) + elem_size * count;
        if (need > reserved_size_) {
            if (extend_memory(static_cast<uint32_t>(need)) != E_OK) {
                return E_OOM;
            }
        }
        char* dst = data_ + real_data_size_;
        std::memcpy(dst, &len, variable_type_len_);
        if (len > 0) {
            std::memcpy(dst + variable_type_len_, value, len);
        }
        uint32_t filled = 1;
        while (filled < count) {
            const uint32_t copy = std::min(filled, count - filled);
            std::memcpy(dst + static_cast<size_t>(filled) * elem_size, dst,
                        static_cast<size_t>(copy) * elem_size);
            filled += copy;
        }
        real_data_size_ = static_cast<uint32_t>(need);
        return E_OK;
    }

    // Fixed-width counterpart of fill_repeated_variable().
    FORCE_INLINE int fill_repeated_fixed(const char* value, uint32_t elem_size,
                                         uint32_t count) {
        if (count == 0) {
            return E_OK;
        }
        const uint64_t available =
            static_cast<uint64_t>(0xFFFFFFFFULL) - real_data_size_;
        if (elem_size != 0 && count > available / elem_size) {
            return E_OOM;
        }
        const uint64_t need = static_cast<uint64_t>(real_data_size_) +
                              static_cast<uint64_t>(elem_size) * count;
        if (need > reserved_size_) {
            if (extend_memory(static_cast<uint32_t>(need)) != E_OK) {
                return E_OOM;
            }
        }
        char* dst = data_ + real_data_size_;
        std::memcpy(dst, value, elem_size);
        uint32_t filled = 1;
        while (filled < count) {
            const uint32_t copy = std::min(filled, count - filled);
            std::memcpy(dst + static_cast<size_t>(filled) * elem_size, dst,
                        static_cast<size_t>(copy) * elem_size);
            filled += copy;
        }
        real_data_size_ = static_cast<uint32_t>(need);
        return E_OK;
    }

    // Declare bytes that were written directly through get_data() without going
    // through append_*(). Used when a caller decodes straight into this buffer
    // at a known offset.
    FORCE_INLINE void set_data_size(uint32_t size) {
        ASSERT(size <= reserved_size_);
        real_data_size_ = size;
    }

   private:
    char* data_;
    uint8_t variable_type_len_;
    uint32_t real_data_size_;
    uint32_t reserved_size_;  // malloc memory size from system
};

}  // namespace common
#endif  // COMMON_CONTAINER_BYTE_BUFFER_H
