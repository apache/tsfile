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
#ifndef READER_FILTER_OPERATOR_OR_FILTER_H
#define READER_FILTER_OPERATOR_OR_FILTER_H

#include "binary_filter.h"
// #include "storage/storage_utils.h"

namespace storage {

class OrFilter : public BinaryFilter {
   public:
    OrFilter() {}
    OrFilter(Filter *left, Filter *right) : BinaryFilter(left, right) {}
    ~OrFilter() {}

    FORCE_INLINE bool satisfy(Statistic *statistic) {
        return left_->satisfy(statistic) || right_->satisfy(statistic);
    }

    FORCE_INLINE bool satisfy(long time, int64_t value) {
        return left_->satisfy(time, value) || right_->satisfy(time, value);
    }

    FORCE_INLINE bool satisfy_start_end_time(long start_time, long end_time) {
        return left_->satisfy_start_end_time(start_time, end_time) ||
               right_->satisfy_start_end_time(start_time, end_time);
    }

    FORCE_INLINE bool contain_start_end_time(long start_time, long end_time) {
        return left_->contain_start_end_time(start_time, end_time) ||
               right_->contain_start_end_time(start_time, end_time);
    }

    std::vector<TimeRange *> *get_time_ranges() {
        std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
        std::vector<TimeRange *> *left_time_ranges = left_->get_time_ranges();
        std::vector<TimeRange *> *right_time_ranges = right_->get_time_ranges();

        int left_index = 0, right_index = 0;
        int left_size = left_time_ranges->size();
        int right_size = right_time_ranges->size();
        TimeRange *range = choose_next_range(
            left_time_ranges, right_time_ranges, left_index, right_index);
        while (left_index < left_size || right_index < right_size) {
            TimeRange *choosen_range = choose_next_range(
                left_time_ranges, right_time_ranges, left_index, right_index);
            if (choosen_range->start_time_ > range->end_time_) {
                result->push_back(
                    new TimeRange(range->start_time_, range->end_time_));
                range = choosen_range;
            } else {
                range->end_time_ =
                    std::max(range->end_time_, choosen_range->end_time_);
            }
        }
        result->push_back(new TimeRange(range->start_time_, range->end_time_));
        return result;
    }

   private:
    TimeRange *choose_next_range(std::vector<TimeRange *> *left_time_ranges,
                                 std::vector<TimeRange *> *right_time_ranges,
                                 int &left_index, int &right_index) {
        int left_size = left_time_ranges->size();
        int right_size = right_time_ranges->size();
        if (left_index < left_size && right_index < right_size) {
            TimeRange *left_range = left_time_ranges->at(left_index);
            TimeRange *right_range = right_time_ranges->at(right_index);
            // Choose the range with the smaller minimum start time
            if (left_range->start_time_ <= right_range->start_time_) {
                left_index++;
                return left_range;
            } else {
                right_index++;
                return right_range;
            }
        } else if (left_index < left_size) {
            return left_time_ranges->at(left_index++);
        } else {
            return right_time_ranges->at(right_index++);
        }
    }
};

}  // namespace storage

#endif  // READER_FILTER_OPERATOR_OR_FILTER_H
