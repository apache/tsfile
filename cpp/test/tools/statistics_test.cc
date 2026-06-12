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

TEST(StatisticsTest, Int64StatisticCellsContainValueSummaries) {
    storage::Int64Statistic st;
    st.update(1, static_cast<int64_t>(10));
    st.update(3, static_cast<int64_t>(30));
    tsfile_cli::StatisticCells cells = tsfile_cli::statistic_value_cells(&st);
    EXPECT_EQ(cells.values[0], "10");
    EXPECT_EQ(cells.values[1], "30");
    EXPECT_EQ(cells.values[2], "10");
    EXPECT_EQ(cells.values[3], "30");
    EXPECT_EQ(cells.values[4], "40");
    EXPECT_EQ(cells.is_null,
              std::vector<bool>({false, false, false, false, false}));
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
