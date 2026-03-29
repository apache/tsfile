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

#include "common/allocator/my_string.h"
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
    virtual bool satisfy(int64_t time, int64_t value) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfy(int64_t time, common::String value) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfy_start_end_time(int64_t start_time, int64_t end_time) {
        ASSERT(false);
        return false;
    }
    virtual bool contain_start_end_time(int64_t start_time, int64_t end_time) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfyRow(int time,
                            std::vector<std::string*> segments) const {
        ASSERT(false);
        return false;
    }
    virtual std::vector<TimeRange*>* get_time_ranges() {
        ASSERT(false);
        return nullptr;
    }

    // Batch time filter: evaluate time filter on an array of timestamps.
    // Writes true/false into @mask for each element.
    // Returns the number of elements that passed (mask[i] == true).
    // Default: scalar fallback using satisfy_start_end_time.
    virtual int satisfy_batch_time(const int64_t* times, int count,
                                   bool* mask) {
        int pass = 0;
        for (int i = 0; i < count; ++i) {
            mask[i] = satisfy_start_end_time(times[i], times[i]);
            if (mask[i]) ++pass;
        }
        return pass;
    }
};

}  // namespace storage

#endif  // READER_FILTER_BASIC_FILTER_H
