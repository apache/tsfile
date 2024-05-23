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
#ifndef READER_FILTER_TIME_FILTER_H
#define READER_FILTER_TIME_FILTER_H

#include <stdint.h>

#include <limits>
#include <vector>

namespace storage {

class TimeEq;
class TimeGt;
class TimeGtEq;
class TimeLt;
class TimeLtEq;
class TimeNotEq;
class TimeIn;
class TimeBetween;

class TimeFilter {
   public:
    static TimeEq* eq(int64_t value);
    static TimeGt* gt(int64_t value);
    static TimeGtEq* gt_eq(int64_t value);
    static TimeLt* lt(int64_t value);
    static TimeLtEq* lt_eq(int64_t value);
    static TimeNotEq* not_eqt(int64_t value);
    static TimeIn* in(std::vector<int64_t>& values, bool not_filter);
    static TimeBetween* between(int64_t value1, int64_t value2,
                                bool not_filter);
};

}  // namespace storage

#endif  // READER_FILTER_TIME_FILTER_H