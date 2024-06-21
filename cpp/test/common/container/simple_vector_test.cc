#include "common/container/simple_vector.h"

#include <gtest/gtest.h>

namespace common {

TEST(SimpleVectorTest, PushBackAndSize) {
    SimpleVector<int> vec;
    EXPECT_EQ(vec.size(), 0);

    vec.push_back(10);
    EXPECT_EQ(vec.size(), 1);

    vec.push_back(20);
    EXPECT_EQ(vec.size(), 2);

    for (int i = 0; i < 16; ++i) {
        vec.push_back(i);
    }
    EXPECT_EQ(vec.size(), 18);
}

TEST(SimpleVectorTest, AccessVector) {
    SimpleVector<int> vec;
    for (int i = 0; i < 20; ++i) {
        vec.push_back(i);
    }

    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(vec[i], i);
    }
}

}  // namespace common
