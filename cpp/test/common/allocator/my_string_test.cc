#include "common/allocator/my_string.h"

#include <gtest/gtest.h>

namespace {

class StringTest : public ::testing::Test {
   public:
    common::PageArena arena_;
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(StringTest, DefaultConstructorAndIsNull) {
    common::String s;
    EXPECT_TRUE(s.is_null());
    EXPECT_EQ(s.len_, 0u);
    EXPECT_EQ(s.buf_, nullptr);
}

TEST_F(StringTest, DupFromString) {
    std::string input = "Hello";
    common::String s;
    int result = s.dup_from(input, arena_);

    EXPECT_EQ(result, common::E_OK);
    EXPECT_EQ(s.len_, input.size());
    EXPECT_NE(s.buf_, nullptr);
    EXPECT_EQ(memcmp(s.buf_, input.c_str(), s.len_), 0);
}

TEST_F(StringTest, DupFromStringObject) {
    common::String s1("World", 5);
    common::String s2;
    int result = s2.dup_from(s1, arena_);

    EXPECT_EQ(result, common::E_OK);
    EXPECT_EQ(s2.len_, s1.len_);
    EXPECT_NE(s2.buf_, nullptr);
    EXPECT_EQ(memcmp(s2.buf_, s1.buf_, s2.len_), 0);
}

TEST_F(StringTest, BuildFromStringObjects) {
    common::String s1("Hello", 5);
    common::String s2("World", 5);
    common::String result;
    int build_result = result.build_from(s1, s2, arena_);

    EXPECT_EQ(build_result, common::E_OK);
    EXPECT_EQ(result.len_, s1.len_ + s2.len_);
    EXPECT_NE(result.buf_, nullptr);
    EXPECT_EQ(memcmp(result.buf_, s1.buf_, s1.len_), 0);
    EXPECT_EQ(memcmp(result.buf_ + s1.len_, s2.buf_, s2.len_), 0);
}

TEST_F(StringTest, EqualToStringObjects) {
    common::String s1("Hello", 5);
    common::String s2("Hello", 5);
    common::String s3("World", 5);

    EXPECT_TRUE(s1.equal_to(s2));
    EXPECT_FALSE(s1.equal_to(s3));
}

TEST_F(StringTest, LessThanStringObjects) {
    common::String s1("Hello", 5);
    common::String s2("World", 5);
    common::String s3("Hell", 4);

    EXPECT_TRUE(s1.less_than(s2));
    EXPECT_FALSE(s2.less_than(s1));
    EXPECT_TRUE(s3.less_than(s1));
}

TEST_F(StringTest, CompareStringObjects) {
    common::String s1("Hello", 5);
    common::String s2("World", 5);
    common::String s3("Hello", 5);
    common::String s4("Hell", 4);

    EXPECT_EQ(s1.compare(s3), 0);
    EXPECT_GT(s2.compare(s1), 0);
    EXPECT_LT(s4.compare(s1), 0);
    EXPECT_EQ(s1.compare(s3), 0);
}

}  // namespace
