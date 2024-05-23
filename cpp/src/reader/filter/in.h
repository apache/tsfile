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
#ifndef READER_FILTER_OPERATOR_IN_H
#define READER_FILTER_OPERATOR_IN_H

#include <vector>

#include "filter/binary_filter.h"

namespace storage {
template <typename T>
class In : public Filter {
   public:
    In() {}
    In(std::vector<T> &values, FilterType type, bool not_in)
        : values_(values), type_(type), not_(not_in) {}
    virtual ~In() {}

    bool satisfy(Statistic *statistic) { return true; }

    bool satisfy(long time, Object value) {
        Object v = (filterType == TIME_FILTER ? time : value);
        std::vector<T>::iterator it = find(values_.begin(), values_.end(), v);
        bool result = (it != values_.end() ? true : false);
        return result != not_;
    }

    bool satisfy_start_end_time(long start_time, long end_time) { return true; }

    bool contain_start_end_time(long start_time, long end_time) { return true; }

   protected:
    std::vector<T> values_;
    FilterType type_;
    bool not_;
};
}  // namespace storage

#endif  // READER_FILTER_OPERATOR_IN_H
