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

#ifndef COMMON_DATATYPE_DATE_CONVERTER_H
#define COMMON_DATATYPE_DATE_CONVERTER_H

#include <cstdint>
#include <ctime>

#include "utils/errno_define.h"

namespace common {
class DateConverter {
   public:
    static int date_to_int(const std::tm& tm_date, int32_t& out_int) {
        if (tm_date.tm_year == -1 || tm_date.tm_mon == -1 ||
            tm_date.tm_mday == -1) {
            return E_INVALID_ARG;
        }

        const int year = tm_date.tm_year + 1900;
        const int month = tm_date.tm_mon + 1;
        const int day = tm_date.tm_mday;

        if (year < 1000 || year > 9999 || month < 1 || month > 12 || day < 1 ||
            day > 31) {
            return E_INVALID_ARG;
        }

        // Normalize the tm structure and validate the date
        std::tm tmp = tm_date;
        tmp.tm_hour = 12;
        tmp.tm_isdst = -1;
        if (std::mktime(&tmp) == -1) {
            return E_INVALID_ARG;
        }

        if (tmp.tm_year != tm_date.tm_year || tmp.tm_mon != tm_date.tm_mon ||
            tmp.tm_mday != tm_date.tm_mday) {
            return E_INVALID_ARG;
        }

        const int64_t result =
            static_cast<int64_t>(year) * 10000 + month * 100 + day;
        if (result > INT32_MAX || result < INT32_MIN) {
            return E_OUT_OF_RANGE;
        }

        out_int = static_cast<int32_t>(result);
        return E_OK;
    }

    static bool is_tm_ymd_equal(const std::tm& tm1, const std::tm& tm2) {
        return tm1.tm_year == tm2.tm_year && tm1.tm_mon == tm2.tm_mon &&
               tm1.tm_mday == tm2.tm_mday;
    }

    static int int_to_date(int32_t date_int, std::tm& out_tm) {
        if (date_int == 0) {
            out_tm.tm_year = out_tm.tm_mon = out_tm.tm_mday = -1;
            return E_INVALID_ARG;
        }

        int year = date_int / 10000;
        int month = (date_int % 10000) / 100;
        int day = date_int % 100;

        if (year < 1000 || year > 9999 || month < 1 || month > 12 || day < 1 ||
            day > 31) {
            return E_INVALID_ARG;
        }

        out_tm = {0};
        out_tm.tm_year = year - 1900;
        out_tm.tm_mon = month - 1;
        out_tm.tm_mday = day;
        out_tm.tm_hour = 12;
        out_tm.tm_isdst = -1;

        if (std::mktime(&out_tm) == -1) {
            return E_INVALID_ARG;
        }
        if (out_tm.tm_year != year - 1900 || out_tm.tm_mon != month - 1 ||
            out_tm.tm_mday != day) {
            return E_INVALID_ARG;
        }

        return E_OK;
    }
};
}  // namespace common
#endif  // COMMON_DATATYPE_DATE_CONVERTER_H
