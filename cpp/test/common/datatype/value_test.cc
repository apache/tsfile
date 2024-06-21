#include "common/datatype/value.h"

#include <gtest/gtest.h>
#include <limits.h>

namespace common {

class ValueTest : public ::testing::Test {};

TEST_F(ValueTest, ConstructorDestructor) {
    Value* v = new Value(INT64);
    EXPECT_EQ(v->type_, INT64);
    delete v;
}

TEST_F(ValueTest, SetValue) {
    Value v(INT64);

    int64_t int64_val = 123456789;
    v.set_value(INT64, &int64_val);
    EXPECT_EQ(v.type_, INT64);
    EXPECT_EQ(v.value_.lval_, int64_val);

    double double_val = 123.456;
    v.set_value(DOUBLE, &double_val);
    EXPECT_EQ(v.type_, DOUBLE);
    EXPECT_EQ(v.value_.dval_, double_val);

    char text_val[] = "hello";
    v.set_value(TEXT, text_val);
    EXPECT_EQ(v.type_, TEXT);
    EXPECT_STREQ(v.value_.sval_, text_val);
    v.free_memory();
}

TEST_F(ValueTest, IsTypeAndIsLiteral) {
    Value v1(INT64);
    EXPECT_TRUE(v1.is_type(INT64));
    EXPECT_TRUE(v1.is_literal());

    Value v2(DOUBLE);
    EXPECT_TRUE(v2.is_type(DOUBLE));
    EXPECT_TRUE(v2.is_literal());

    Value v3(TEXT);
    EXPECT_TRUE(v3.is_type(TEXT));
    EXPECT_TRUE(v3.is_literal());

    Value v4(NULL_TYPE);
    EXPECT_TRUE(v4.is_type(NULL_TYPE));
    EXPECT_TRUE(v4.is_literal());
}

TEST_F(ValueTest, MakeFunctions) {
    Value* v1 = make(INT64);
    EXPECT_EQ(v1->type_, INT64);
    delete v1;

    Value* v2 = make_literal((int64_t)123456789);
    EXPECT_EQ(v2->type_, INT64);
    EXPECT_EQ(v2->value_.lval_, 123456789LL);
    delete v2;

    Value* v3 = make_literal(123.456);
    EXPECT_EQ(v3->type_, DOUBLE);
    EXPECT_EQ(v3->value_.dval_, 123.456);
    delete v3;

    char text_val[] = "hello";
    Value* v4 = make_literal(text_val);
    EXPECT_EQ(v4->type_, TEXT);
    EXPECT_STREQ(v4->value_.sval_, text_val);
    delete v4;

    Value* v5 = make_literal(true);
    EXPECT_EQ(v5->type_, BOOLEAN);
    EXPECT_TRUE(v5->value_.bval_);
    delete v5;

    Value* v6 = make_null_literal();
    EXPECT_EQ(v6->type_, NULL_TYPE);
    delete v6;
}

TEST_F(ValueTest, ValueToString) {
    Value v1(INT64);
    v1.value_.lval_ = 123456789LL;
    EXPECT_EQ(value_to_string(&v1), "123456789");

    Value v2(DOUBLE);
    v2.value_.dval_ = 123.456;
    EXPECT_EQ(value_to_string(&v2), "123.456");

    Value v3(TEXT);
    v3.value_.sval_ = strdup("hello");
    EXPECT_EQ(value_to_string(&v3), "hello");

    Value v4(BOOLEAN);
    v4.value_.bval_ = true;
    EXPECT_EQ(value_to_string(&v4), "true");

    Value v5(NULL_TYPE);
    EXPECT_EQ(value_to_string(&v5), "NULL");
}

TEST_F(ValueTest, GetTypedDataFromValue) {
    Value v1(INT64);
    v1.value_.lval_ = 123456789LL;
    int64_t int64_data;
    EXPECT_EQ(get_typed_data_from_value(&v1, int64_data), E_OK);
    EXPECT_EQ(int64_data, 123456789LL);

    Value v2(DOUBLE);
    v2.value_.dval_ = 123.456;
    double double_data;
    EXPECT_EQ(get_typed_data_from_value(&v2, double_data), E_OK);
    EXPECT_EQ(double_data, 123.456);

    Value v3(BOOLEAN);
    v3.value_.bval_ = true;
    bool bool_data;
    EXPECT_EQ(get_typed_data_from_value(&v3, bool_data), E_OK);
    EXPECT_TRUE(bool_data);

    Value v4(TEXT);
    v4.value_.sval_ = strdup("hello");
    std::string text_data = value_to_string(&v4);
    EXPECT_EQ(text_data, "hello");
    v4.free_memory();
}

}  // namespace common