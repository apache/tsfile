#include "common/seq_tvlist.h"

#include <gtest/gtest.h>

namespace storage {

template <typename Type>
class SeqTVListTest : public ::testing::Test {
   protected:
    SeqTVList<Type> list;

    void SetUp() override { list.init(10, 10000, false); }

    void TearDown() override { list.destroy(); }
};

typedef ::testing::Types<int, double> MyTypes;
TYPED_TEST_SUITE(SeqTVListTest, MyTypes);

TYPED_TEST(SeqTVListTest, Init) {
    EXPECT_EQ(this->list.get_total_count(), 0);
    EXPECT_FALSE(this->list.is_immutable());
}

TYPED_TEST(SeqTVListTest, Push) {
    EXPECT_EQ(this->list.push(1, TypeParam(10)), common::E_OK);
    EXPECT_EQ(this->list.get_total_count(), 1);
}

TYPED_TEST(SeqTVListTest, PushLargeQuantities) {
    for (int i = 0; i < 10000; i++) {
        EXPECT_EQ(this->list.push(i, TypeParam(i)), common::E_OK);
    }
    EXPECT_EQ(this->list.get_total_count(), 10000);
}

TYPED_TEST(SeqTVListTest, PushOutOfOrder) {
    EXPECT_EQ(this->list.push(1, TypeParam(10)), common::E_OK);
    EXPECT_EQ(this->list.push(0, TypeParam(20)), common::E_OUT_OF_ORDER);
}

TYPED_TEST(SeqTVListTest, PushOverflow) {
    this->list.init(2, 4, false);
    EXPECT_EQ(this->list.push(1, TypeParam(10)), common::E_OK);
    EXPECT_EQ(this->list.push(2, TypeParam(20)), common::E_OK);
    EXPECT_EQ(this->list.push(3, TypeParam(30)), common::E_OK);
    EXPECT_EQ(this->list.push(4, TypeParam(40)), common::E_OK);
    EXPECT_EQ(this->list.push(5, TypeParam(50)), common::E_OVERFLOW);
}

TYPED_TEST(SeqTVListTest, ScanWithoutLock) {
    this->list.push(1, TypeParam(10));
    this->list.push(2, TypeParam(20));

    auto iter = this->list.scan_without_lock();
    typename SeqTVList<TypeParam>::TV tv;
    EXPECT_EQ(iter.next(tv), common::E_OK);
    EXPECT_EQ(tv.time_, 1);
    EXPECT_EQ(tv.value_, TypeParam(10));

    EXPECT_EQ(iter.next(tv), common::E_OK);
    EXPECT_EQ(tv.time_, 2);
    EXPECT_EQ(tv.value_, TypeParam(20));

    EXPECT_EQ(iter.next(tv), common::E_NO_MORE_DATA);
}

TYPED_TEST(SeqTVListTest, ScanWithoutLockTimeRange) {
    this->list.push(1, TypeParam(10));
    this->list.push(2, TypeParam(20));
    this->list.push(3, TypeParam(30));

    auto iter = this->list.scan_without_lock(2, 3);
    typename SeqTVList<TypeParam>::TV tv;
    EXPECT_EQ(iter.next(tv), common::E_OK);
    EXPECT_EQ(tv.time_, 2);
    EXPECT_EQ(tv.value_, TypeParam(20));

    EXPECT_EQ(iter.next(tv), common::E_NO_MORE_DATA);
}

TYPED_TEST(SeqTVListTest, MarkImmutable) {
    EXPECT_FALSE(this->list.is_immutable());
    this->list.mark_immutable();
    EXPECT_TRUE(this->list.is_immutable());
}

TYPED_TEST(SeqTVListTest, IteratorInit) {
    typename SeqTVList<TypeParam>::Iterator iter;
    iter.init(&this->list, 0, 2);
    EXPECT_EQ(iter.host_list_, &this->list);
    EXPECT_EQ(iter.read_idx_, 0);
    EXPECT_EQ(iter.end_idx_, 2);
}

}  // namespace storage
