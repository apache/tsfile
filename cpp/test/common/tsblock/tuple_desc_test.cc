/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include "common/tsblock/tuple_desc.h"

#include <gtest/gtest.h>

#include "common/db_common.h"
#include "utils/db_utils.h"

namespace common {

TEST(TupleDescTest, Initialization) {
    TupleDesc td;
    EXPECT_EQ(td.get_column_count(), 0);
}

TEST(TupleDescTest, PushBackAndGet) {
    TupleDesc td;
    ColumnDesc col1;
    ColumnDesc col2;

    td.push_back(col1);
    td.push_back(col2);

    EXPECT_EQ(td.get_column_count(), 2);
    EXPECT_EQ(td.get_column_desc(0), col1);
    EXPECT_EQ(td.get_column_desc(1), col2);
}

TEST(TupleDescTest, RemoveColumn) {
    TupleDesc td;
    ColumnDesc col1;
    ColumnDesc col2;

    td.push_back(col1);
    td.push_back(col2);
    td.remove_column(0);

    EXPECT_EQ(td.get_column_count(), 1);
    EXPECT_EQ(td.get_column_desc(0), col2);
}

TEST(TupleDescTest, CloneFrom) {
    TupleDesc td1;
    ColumnDesc col1;
    ColumnDesc col2;

    td1.push_back(col1);
    td1.push_back(col2);

    TupleDesc td2;
    td2.clone_from(&td1);

    EXPECT_EQ(td2.get_column_count(), 2);
    EXPECT_EQ(td2.get_column_desc(0), col1);
    EXPECT_EQ(td2.get_column_desc(1), col2);
}

TEST(TupleDescTest, EqualTo) {
    TupleDesc td1;
    ColumnDesc col1;
    ColumnDesc col2;

    td1.push_back(col1);
    td1.push_back(col2);

    TupleDesc td2;
    td2.push_back(col1);
    td2.push_back(col2);

    EXPECT_TRUE(td1.equal_to(td2));
    EXPECT_TRUE(td2.equal_to(td1));
}

TEST(TupleDescTest, LargeQuantities) {
    TupleDesc td;
    ColumnDesc cols[10000];
    for (int i = 0; i < 10000; i++) {
        cols[i].column_name_ = std::to_string(i);
        td.push_back(cols[i]);
    }

    for (int i = 0; i < 10000; i++) {
        EXPECT_EQ(td.get_column_desc(i), cols[i]);
    }
}

}  // namespace common