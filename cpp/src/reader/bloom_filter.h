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
#ifndef READER_BLOOM_FILTER_H
#define READER_BLOOM_FILTER_H

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "common/allocator/my_string.h"
#include "common/container/murmur_hash3.h"

namespace storage {

class HashFunction {
   public:
    HashFunction() : cap_(0), seed_(0) {}
    void init(int32_t cap, int32_t seed) {
        ASSERT(cap > 1);
        cap_ = cap;
        seed_ = seed;
    }
    FORCE_INLINE int32_t hash(common::String buf) {
        int res = common::Murmur128Hash::hash(buf, seed_);
        if (res == INT32_MIN) {
            res = 0;
        }
        res = (res > 0 ? res : (0 - res)) % cap_;
        return res;
    }

   private:
    int32_t cap_;
    int32_t seed_;
};

class BitSet {
   public:
    BitSet() : words_(nullptr), word_count_(0) {}
    int init(int32_t size) {
        ASSERT(size > 1);
        word_count_ = (size - 1) / 64 + 1;
        int32_t alloc_size = word_count_ * sizeof(uint64_t);
        words_ =
            (uint64_t *)common::mem_alloc(alloc_size, common::MOD_BLOOM_FILTER);
        if (IS_NULL(words_)) {
            return common::E_OOM;
        }
        memset(words_, 0, alloc_size);
        return common::E_OK;
    }
    void destroy() {
        if (!IS_NULL(words_)) {
            common::mem_free(words_);
            words_ = nullptr;
        }
    }
    void set(int32_t pos) {
        int32_t word_idx = pos / 64;
        int32_t word_offset = pos % 64;
        words_[word_idx] |= (1ul << word_offset);
    }
    int32_t get_words_in_use() const {
        for (int32_t i = word_count_ - 1; i >= 0; i--) {
            if (words_[i] != 0) {
                return i + 1;
            }
        }
        return 0;
    }
    void to_bytes(uint8_t *&ret_bytes, int32_t &ret_len) const;
    void revert_bytes(uint8_t *bytes) { common::mem_free(bytes); }
    int from_bytes(uint8_t *filter_data, uint32_t filter_data_bytes_len);

   private:
    uint64_t *words_;
    int32_t word_count_;
};

class BloomFilter {
   public:
    static const int32_t MAX_HASH_FUNC_COUNT = 8;
    static const int32_t MIN_SIZE = 256;
    // static const non-integer variable can not be intialiazed in class
    static const double MIN_BF_ERROR_RATE;
    static const double MAX_BF_ERROR_RATE;
    static const int32_t SEEDS[MAX_HASH_FUNC_COUNT];

   public:
    BloomFilter() : size_(0), hash_func_count_(0), bitset_() {}
    ~BloomFilter() { destroy(); }
    int init(double error_percent, int entry_count);
    void destroy() { bitset_.destroy(); }
    int add_path_entry(const common::String &device_name,
                       const common::String &measurement_name);
    int serialize_to(common::ByteStream &out);
    int deserialize_from(common::ByteStream &in);

   private:
    common::String get_entry_string(const common::String &device_name,
                                    const common::String &measurement_name);
    FORCE_INLINE void free_entry_buf(char *entry_buf) {
        common::mem_free(entry_buf);
    }

   private:
    uint32_t size_;
    uint32_t hash_func_count_;
    HashFunction hash_func_arr_[MAX_HASH_FUNC_COUNT];
    BitSet bitset_;
};

}  // end namespace storage
#endif  // READER_BLOOM_FILTER_H
