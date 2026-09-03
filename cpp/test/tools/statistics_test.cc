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

#include "commands/statistics.h"

#include <gtest/gtest.h>

#include "common/statistic.h"

TEST(StatisticsTest, Int32StatisticSupportsAllValueSummaries) {
    storage::Int32Statistic st;
    st.update(1, static_cast<int32_t>(10));
    st.update(3, static_cast<int32_t>(30));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values,
              std::vector<std::string>({"10", "30", "10", "30", "40"}));
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({false, false, false, false, false}));
}

TEST(StatisticsTest, Int64StatisticLeavesSumNull) {
    storage::Int64Statistic st;
    st.update(1, static_cast<int64_t>(10));
    st.update(3, static_cast<int64_t>(30));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values[0], "10");
    EXPECT_EQ(cells.values[1], "30");
    EXPECT_EQ(cells.values[2], "10");
    EXPECT_EQ(cells.values[3], "30");
    EXPECT_TRUE(cells.values[4].empty());
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({false, false, false, false, true}));
}

TEST(StatisticsTest, DateAndTimestampStatisticsLeaveSumNull) {
    storage::DateStatistic date;
    date.update(1, static_cast<int32_t>(19700101));
    tsfile_cli::StatisticCells date_cells =
        tsfile_cli::statistic_value_cells(&date);
    EXPECT_EQ(date_cells.values[0], "1970-01-01");
    EXPECT_EQ(date_cells.values[3], "1970-01-01");
    EXPECT_TRUE(date_cells.is_null[4]);

    storage::TimestampStatistics timestamp;
    timestamp.update(1, static_cast<int64_t>(1700000000000));
    tsfile_cli::StatisticCells timestamp_cells =
        tsfile_cli::statistic_value_cells(&timestamp);
    EXPECT_EQ(timestamp_cells.values[0], "1700000000000");
    EXPECT_EQ(timestamp_cells.values[3], "1700000000000");
    EXPECT_TRUE(timestamp_cells.is_null[4]);
}

TEST(StatisticsTest, BooleanStatisticLeavesMinMaxNull) {
    storage::BooleanStatistic st;
    st.update(1, true);
    st.update(2, false);
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_TRUE(cells.is_null[0]);
    EXPECT_TRUE(cells.is_null[1]);
    EXPECT_EQ(cells.values[2], "true");
    EXPECT_EQ(cells.values[3], "false");
    EXPECT_EQ(cells.values[4], "1");
}

TEST(StatisticsTest, DoubleStatisticPreservesRoundTripPrecision) {
    storage::DoubleStatistic st;
    st.update(1, 1.2345678901234567);
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values[0], "1.2345678901234567");
    EXPECT_EQ(cells.values[4], "1.2345678901234567");
}

TEST(StatisticsTest, FloatStatisticSupportsAllValueSummaries) {
    storage::FloatStatistic st;
    st.update(1, 1.25F);
    st.update(2, 2.5F);
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values,
              std::vector<std::string>({"1.25", "2.5", "1.25", "2.5", "3.75"}));
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({false, false, false, false, false}));
}

TEST(StatisticsTest, StringStatisticOmitsOnlySum) {
    storage::StringStatistic st;
    st.update(1, common::String("beta"));
    st.update(2, common::String("alpha"));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values[0], "alpha");
    EXPECT_EQ(cells.values[1], "beta");
    EXPECT_EQ(cells.values[2], "beta");
    EXPECT_EQ(cells.values[3], "alpha");
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({false, false, false, false, true}));
}

TEST(StatisticsTest, TextStatisticOnlyProvidesFirstAndLast) {
    storage::TextStatistic st;
    st.update(1, common::String("first"));
    st.update(2, common::String("last"));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values[2], "first");
    EXPECT_EQ(cells.values[3], "last");
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({true, true, false, false, true}));
}

TEST(StatisticsTest, BlobStatisticProvidesNoValueSummaries) {
    storage::BlobStatistic st;
    st.update(1, common::String("blob"));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.is_null, std::vector<bool>({true, true, true, true, true}));
}
