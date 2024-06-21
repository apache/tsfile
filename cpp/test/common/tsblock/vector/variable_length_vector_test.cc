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
