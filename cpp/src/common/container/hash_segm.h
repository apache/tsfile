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
#ifndef COMMON_CONTAINER_HASH_SEGM_H
#define COMMON_CONTAINER_HASH_SEGM_H

#include <stddef.h>

#include "common/container/hash_node.h"
#include "common/mutex/mutex.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

#define SEGMENT_CAPACITY 100

namespace common {

template <class KeyType, class ValueType>
class HashSegm {
   public:
    HashSegm() {}
    ~HashSegm() {}

    FORCE_INLINE HashNode<KeyType, ValueType>& operator[](size_t idx) {
        if (idx >= SEGMENT_CAPACITY) {
            // log_err("index %lu is out of range.", idx);
            ASSERT(idx < SEGMENT_CAPACITY);
        }
        return segm_pointer_[idx];
    }

   public:
    Mutex mutexes_[SEGMENT_CAPACITY];
    HashNode<KeyType, ValueType> segm_pointer_[SEGMENT_CAPACITY];
};

}  // end namespace common
#endif  // COMMON_CONTAINER_HASH_SEGM_H