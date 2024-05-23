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
#ifndef READER_FILTER_BASIC_BINARY_FILTER_H
#define READER_FILTER_BASIC_BINARY_FILTER_H

#include "filter.h"
#include "filter_type.h"

namespace storage {
class BinaryFilter : public Filter {
   public:
    BinaryFilter() : Filter() {}
    BinaryFilter(Filter *left, Filter *right)
        : Filter(), left_(left), right_(right) {}
    virtual ~BinaryFilter() {}

    void set_left(Filter *left) { left_ = left; }
    void set_right(Filter *right) { right_ = right; }
    Filter get_left() { return *left_; }
    Filter get_right() { return *right_; }

   protected:
    Filter *left_;
    Filter *right_;
};

}  // namespace storage

#endif  // READER_FILTER_BASIC_BINARY_FILTER_H
