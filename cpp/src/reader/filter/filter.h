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
#ifndef READER_FILTER_BASIC_FILTER_H
#define READER_FILTER_BASIC_FILTER_H

#include <vector>

#include "common/db_common.h"

namespace storage {

struct TimeRange;
class Statistic;

class Filter {
   public:
    Filter() {}
    virtual ~Filter() {}

    virtual bool satisfy(Statistic* statistic) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfy(long time, int64_t value) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfy_start_end_time(long start_time, long end_time) {
        ASSERT(false);
        return false;
    }
    virtual bool contain_start_end_time(long start_time, long end_time) {
        ASSERT(false);
        return false;
    }
    virtual std::vector<TimeRange*>* get_time_ranges() {
        ASSERT(false);
        return nullptr;
    }
};

}  // namespace storage

#endif  // READER_FILTER_BASIC_FILTER_H
