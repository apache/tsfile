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
#include "time_filter.h"

#include "time_operator.h"

namespace storage {

TimeEq* TimeFilter::eq(int64_t value) { return new TimeEq(value); }

TimeGt* TimeFilter::gt(int64_t value) { return new TimeGt(value); }

TimeGtEq* TimeFilter::gt_eq(int64_t value) { return new TimeGtEq(value); }

TimeLt* TimeFilter::lt(int64_t value) { return new TimeLt(value); }

TimeLtEq* TimeFilter::lt_eq(int64_t value) { return new TimeLtEq(value); }

TimeNotEq* TimeFilter::not_eqt(int64_t value) { return new TimeNotEq(value); }

TimeIn* TimeFilter::in(std::vector<int64_t>& values, bool not_filter) {
    return new TimeIn(values, not_filter);
}

TimeBetween* TimeFilter::between(int64_t value1, int64_t value2,
                                 bool not_filter) {
    return new TimeBetween(value1, value2, not_filter);
}

}  // namespace storage
