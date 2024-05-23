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
#ifndef READER_FILTER_OPERATOR_GT_H
#define READER_FILTER_OPERATOR_GT_H

#include "storage/read/filter/unary_filter.h"

namespace storage {
template <typename T>
class Gt : public UnaryFilter<T> {
   public:
    Gt() : UnaryFilter() {}
    Gt(T value, FilterType type) { UnaryFilter(value, type); }
    virtual ~Gt() {}

    bool satisfy(Statistic *statistic) {
        if (type_ == TIME_FILTER) {
            return value_ < statistic.end_time_;
        } else {
            if (statistic->get_type() == common::TEXT ||
                statistic->get_type() == common::BOOLEAN) {
                return true;
            } else {
                // todo value filter
                return true;
            }
        }
    }

    bool satisfy(long time, Object value) {
        Object v = (type_ == TIME_FILTER ? time : value);
        return value_ < v;
    }

    bool satisfy_start_end_time(long start_time, long end_time) {
        if (type_ == TIME_FILTER) {
            return value_ < end_time;
        } else {
            return true;
        }
    }

    bool contain_start_end_time(long start_time, long end_time) {
        if (type_ == TIME_FILTER) {
            return value_ < start_time;
        } else {
            return true;
        }
    }
};
}  // namespace storage

#endif  // READER_FILTER_OPERATOR_GT_H