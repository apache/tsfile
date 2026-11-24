/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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
#ifndef UTILS_DATE_UTILS_H
#define UTILS_DATE_UTILS_H

#include <stdint.h>

namespace common {

// March-based Gregorian years put the leap day at the end of the year.
// One 400-year cycle contains 146097 days. Day 719468 is 1970-01-01
// when counted from 0000-03-01.
inline int32_t YYYYMMDDToDaysSinceEpoch(int32_t yyyymmdd) {
    int year = yyyymmdd / 10000;
    const int month = yyyymmdd / 100 % 100;
    const int day = yyyymmdd % 100;
    if (month <= 2) --year;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const int year_of_era = year - era * 400;
    const int march_month = month + (month > 2 ? -3 : 9);
    const int day_of_year = (153 * march_month + 2) / 5 + day - 1;
    return era * 146097 + year_of_era * 365 + year_of_era / 4 -
           year_of_era / 100 + day_of_year - 719468;
}

inline int32_t DaysSinceEpochToYYYYMMDD(int32_t days) {
    const int64_t absolute_day = static_cast<int64_t>(days) + 719468;
    const int64_t era =
        (absolute_day >= 0 ? absolute_day : absolute_day - 146096) / 146097;
    const int day_of_era = static_cast<int>(absolute_day - era * 146097);
    const int year_of_era = (day_of_era - day_of_era / 1460 +
                             day_of_era / 36524 - day_of_era / 146096) /
                            365;
    const int day_of_year =
        day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    const int march_month = (5 * day_of_year + 2) / 153;
    const int day = day_of_year - (153 * march_month + 2) / 5 + 1;
    const int month = march_month + (march_month < 10 ? 3 : -9);
    const int64_t year = era * 400 + year_of_era + (month <= 2);
    return static_cast<int32_t>(year * 10000 + month * 100 + day);
}

}  // namespace common

#endif  // UTILS_DATE_UTILS_H
