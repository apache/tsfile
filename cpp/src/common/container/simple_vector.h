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
#ifndef COMMON_CONTAINER_VECTOR_H
#define COMMON_CONTAINER_VECTOR_H

#include "utils/util_define.h"

namespace common {

template <typename T>
class SimpleVector {
   public:
    SimpleVector() : std_vec_(), size_(0) {}
    void push_back(T t) {
        if (LIKELY(size_ < STACKED_ITEM_COUNT)) {
            stacked_items_[size_] = t;
        } else {
            std_vec_.push_back(t);
        }
        size_++;
    }
    size_t size() { return size_; }
    T operator[](int index) {
        if (UNLIKELY((size_t)index > size_)) {
            abort();
        }
        if (index < STACKED_ITEM_COUNT) {
            return stacked_items_[index];
        } else {
            return std_vec_[index - STACKED_ITEM_COUNT];
        }
    }

   private:
    static const int STACKED_ITEM_COUNT = 16;
    T stacked_items_[STACKED_ITEM_COUNT];
    std::vector<T> std_vec_;
    size_t size_;
};

}  // end namespace common
#endif
