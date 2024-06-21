#include "common/tsblock/vector/fixed_length_vector.h"

#include <gtest/gtest.h>

namespace common {

TEST(FixedLengthVectorTest, Constructor) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10;
    FixedLengthVector flv(common::TSDataType::INT32, max_row_num, type_size,
                          nullptr);

    EXPECT_EQ(flv.get_vector_type(), common::TSDataType::INT32);
}

TEST(FixedLengthVectorTest, Reset) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10;
    FixedLengthVector flv(common::TSDataType::INT32, max_row_num, type_size,
                          nullptr);

    flv.append("test", type_size);
    flv.reset();
    EXPECT_EQ(flv.get_row_num(), 0);
}

TEST(FixedLengthVectorTest, AppendAndRead) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10000;
    FixedLengthVector flv(common::TSDataType::INT32, max_row_num, type_size,
                          nullptr);

    const char* value = "test";
    flv.append(value, type_size);
    uint32_t len;
    bool null;
    char* result = flv.read(&len, &null, 0);
    EXPECT_EQ(len, type_size);
    EXPECT_FALSE(null);
    EXPECT_EQ(memcmp(result, value, type_size), 0);
}

TEST(FixedLengthVectorTest, ReadWithLen) {
    uint32_t type_size = 4;
    uint32_t max_row_num = 10000;
    FixedLengthVector flv(common::TSDataType::INT32, max_row_num, type_size,
                          nullptr);

    const char* value = "test";
    flv.append(value, type_size);
    uint32_t len;
    char* result = flv.read(&len);
    EXPECT_EQ(len, type_size);
    EXPECT_EQ(memcmp(result, value, type_size), 0);
}

}  // namespace common
