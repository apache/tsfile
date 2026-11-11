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

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/global.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"

using namespace common;
using namespace storage;

#ifndef TSFILE_CPP_SOURCE_DIR
#define TSFILE_CPP_SOURCE_DIR "."
#endif

// The fixture was written by the Java implementation with
// allowNonDecreasingTimestamps enabled: a single non-aligned series
// root.sg.d1.s1 (INT32, PLAIN, default TS_2DIFF time encoding) holding
// timestamps {0, 1, 1, 2}.  The C++ reader must return the duplicate
// timestamps as separate rows and in order.
TEST(DuplicateTimestampsRoundTripTest, ReadsJavaWrittenDuplicateTimestamps) {
    libtsfile_init();
    const std::string fixture =
        std::string(TSFILE_CPP_SOURCE_DIR) +
        "/test/reader/fixtures/duplicate_timestamps.tsfile";
    TsFileReader reader;
    ASSERT_EQ(common::E_OK, reader.open(fixture));

    ResultSet* result = nullptr;
    std::vector<std::string> paths{"root.sg.d1.s1"};
    ASSERT_EQ(common::E_OK, reader.query(paths, INT64_MIN, INT64_MAX, result));
    ASSERT_NE(result, nullptr);

    const int64_t expected[] = {0, 1, 1, 2};
    int64_t cur_row = 0;
    bool has_next = false;
    while (result->next(has_next) == common::E_OK && has_next) {
        ASSERT_LT(cur_row, static_cast<int64_t>(4));
        RowRecord* rec = result->get_row_record();
        ASSERT_NE(rec, nullptr);
        EXPECT_EQ(expected[cur_row], rec->get_timestamp());
        ++cur_row;
    }
    EXPECT_EQ(cur_row, 4);
    reader.destroy_query_data_set(result);
    ASSERT_EQ(common::E_OK, reader.close());
}
