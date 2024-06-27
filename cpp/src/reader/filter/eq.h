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
#ifndef READER_FILTER_OPERATOR_EQ_H
#define READER_FILTER_OPERATOR_EQ_H

#include "reader/filter/object.h"
#include "reader/filter/unary_filter.h"

namespace storage {
template <typename T>
class Eq : public UnaryFilter<T> {
   public:
    Eq() : UnaryFilter<T>() {}
    Eq(T value, FilterType type) : UnaryFilter<T>(value, type) {}

    virtual ~Eq() {}

    bool satisfy(Statistic *statistic) {
        if (this->type_ == TIME_FILTER) {
            return this->value_ >= statistic->start_time_ &&
                   this->value_ <= statistic->end_time_;
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
        Object v = (this->type_ == TIME_FILTER ? time : value);
        return this->value_.equals(v);
    }

    bool satisfy_start_end_time(long start_time, long end_time) {
        if (this->type_ == TIME_FILTER) {
            return this->value_ <= end_time && this->value_ >= start_time;
        } else {
            return true;
        }
    }

    bool contain_start_end_time(long start_time, long end_time) {
        if (this->type_ == TIME_FILTER) {
            return this->value_ == start_time && this->value_ == end_time;
        } else {
            return true;
        }
    }
};
}  // namespace storage

#endif  // READER_FILTER_OPERATOR_EQ_H