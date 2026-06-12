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

#include "common/csv_utils.h"

#include <gtest/gtest.h>

#include "common/db_common.h"

TEST(CsvUtilsTest, SplitTsvKeepsQuotesAsData) {
    std::vector<std::string> f = common::split_csv_line("a\t\"b\"\tc", '\t',
                                                        /*csv_quotes=*/false);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "\"b\"");
}

TEST(CsvUtilsTest, SplitCsvHonorsQuotedDelimiterAndEscapedQuote) {
    std::vector<std::string> f =
        common::split_csv_line("1,\"a,b\",\"she \"\"hi\"\"\"", ',', true);
    ASSERT_EQ(f.size(), 3u);
    EXPECT_EQ(f[1], "a,b");
    EXPECT_EQ(f[2], "she \"hi\"");
}

TEST(CsvUtilsTest, EscapeOnlyQuotesWhenNeeded) {
    EXPECT_EQ(common::csv_escape("plain"), "plain");
    EXPECT_EQ(common::csv_escape("a,b"), "\"a,b\"");
    EXPECT_EQ(common::csv_escape("she \"hi\""), "\"she \"\"hi\"\"\"");
    EXPECT_EQ(common::csv_escape("line\nbreak"), "\"line\nbreak\"");
    // A field containing a tab needs no quoting for comma-delimited CSV.
    EXPECT_EQ(common::csv_escape("a\tb"), "a\tb");
    // ...but does when the delimiter is the tab.
    EXPECT_EQ(common::csv_escape("a\tb", '\t'), "\"a\tb\"");
}

TEST(DbCommonTest, ParseDataTypeNameCaseInsensitiveAllTypes) {
    common::TSDataType t;
    EXPECT_TRUE(common::parse_data_type_name("boolean", t));
    EXPECT_EQ(t, common::BOOLEAN);
    EXPECT_TRUE(common::parse_data_type_name("Int32", t));
    EXPECT_EQ(t, common::INT32);
    EXPECT_TRUE(common::parse_data_type_name("TIMESTAMP", t));
    EXPECT_EQ(t, common::TIMESTAMP);
    EXPECT_TRUE(common::parse_data_type_name("date", t));
    EXPECT_EQ(t, common::DATE);
    EXPECT_TRUE(common::parse_data_type_name("BLOB", t));
    EXPECT_EQ(t, common::BLOB);
    EXPECT_FALSE(common::parse_data_type_name("nope", t));
    EXPECT_FALSE(common::parse_data_type_name("", t));
}
