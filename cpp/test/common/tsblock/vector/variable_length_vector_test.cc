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
#include "common/tsblock/vector/variable_length_vector.h"

#include <gtest/gtest.h>

namespace common {

TEST(VariableLengthVectorTest, Constructor) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10;
    VariableLengthVector vlv(common::TSDataType::INT32, max_row_num, type_size,
                             nullptr);

    EXPECT_EQ(vlv.get_vector_type(), common::TSDataType::INT32);
}

TEST(VariableLengthVectorTest, Reset) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10;
    VariableLengthVector vlv(common::TSDataType::INT32, max_row_num, type_size,
                             nullptr);

    vlv.append("test", type_size);
    vlv.reset();
    EXPECT_EQ(vlv.get_row_num(), 0);
}

TEST(VariableLengthVectorTest, AppendAndRead) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10000;
    VariableLengthVector vlv(common::TSDataType::INT32, max_row_num, type_size,
                             nullptr);

    const char* value = "test";
    vlv.append(value, type_size);
    uint32_t len;
    bool null;
    char* result = vlv.read(&len, &null, 0);
    EXPECT_EQ(len, type_size);
    EXPECT_FALSE(null);
    EXPECT_EQ(memcmp(result, value, type_size), 0);
}

TEST(VariableLengthVectorTest, ReadWithLen) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10000;
    VariableLengthVector vlv(common::TSDataType::INT32, max_row_num, type_size,
                             nullptr);

    const char* value = "test";
    vlv.append(value, type_size);
    uint32_t len;
    char* result = vlv.read(&len);
    EXPECT_EQ(len, type_size);
    EXPECT_EQ(memcmp(result, value, type_size), 0);
}

}  // namespace common
