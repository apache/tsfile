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

#include <ctime>

namespace common {

// Convert TsFile YYYYMMDD integer to days since Unix epoch (1970-01-01).
inline int32_t YYYYMMDDToDaysSinceEpoch(int32_t yyyymmdd) {
    int year = yyyymmdd / 10000;
    int month = (yyyymmdd % 10000) / 100;
    int day = yyyymmdd % 100;

    std::tm date = {};
    date.tm_year = year - 1900;
    date.tm_mon = month - 1;
    date.tm_mday = day;
    date.tm_hour = 12;
    date.tm_isdst = -1;

    std::tm epoch = {};
    epoch.tm_year = 70;
    epoch.tm_mon = 0;
    epoch.tm_mday = 1;
    epoch.tm_hour = 12;
    epoch.tm_isdst = -1;

    time_t t1 = mktime(&date);
    time_t t2 = mktime(&epoch);
    return static_cast<int32_t>((t1 - t2) / (60 * 60 * 24));
}

// Convert days since Unix epoch back to YYYYMMDD integer format.
inline int32_t DaysSinceEpochToYYYYMMDD(int32_t days) {
    std::tm epoch = {};
    epoch.tm_year = 70;
    epoch.tm_mon = 0;
    epoch.tm_mday = 1;
    epoch.tm_hour = 12;
    epoch.tm_isdst = -1;
    time_t epoch_t = mktime(&epoch);
    time_t target_t = epoch_t + static_cast<time_t>(days) * 24 * 60 * 60;
    std::tm* d = localtime(&target_t);
    return (d->tm_year + 1900) * 10000 + (d->tm_mon + 1) * 100 + d->tm_mday;
}

}  // namespace common

#endif  // UTILS_DATE_UTILS_H
