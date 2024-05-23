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
#ifndef COMMON_CONTAINER_HASH_NODE_H
#define COMMON_CONTAINER_HASH_NODE_H

#include "utils/util_define.h"

namespace common {

template <class KeyType, class ValueType>
class HashNode {
   public:
    HashNode() {
        is_valid_ = false;
        rehash_ = 0;
        next_ = nullptr;
    }

    HashNode(const KeyType &key, const ValueType &value)
        : key_(key),
          value_(value),
          is_valid_(true),
          rehash_(0),
          next_(nullptr) {}

    ~HashNode() { is_valid_ = false; }

    FORCE_INLINE KeyType get_key() const { return key_; }

    FORCE_INLINE ValueType get_value() const { return value_; }

    FORCE_INLINE void set_key(const KeyType &key) { key_ = key; }

    FORCE_INLINE void set_value(const ValueType &value) { value_ = value; }

    FORCE_INLINE HashNode *get_next() const { return next_; }

    FORCE_INLINE void set_next(HashNode *next) { next_ = next; }

    FORCE_INLINE bool get_valid() const { return is_valid_; }

    FORCE_INLINE void set_valid(bool vd) { is_valid_ = vd; }

    FORCE_INLINE uint64_t get_rehashid() const { return rehash_; }

    FORCE_INLINE void set_rehashid(uint64_t id) { rehash_ = id; }

   public:
    bool is_valid_;
    uint64_t rehash_;
    KeyType key_;
    ValueType value_;
    HashNode *next_;  // pointer to the next node (same bucket)
};

}  // end namespace common
#endif  // COMMON_CONTAINER_HASH_NODE_H