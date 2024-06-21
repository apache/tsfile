#include "common/container/murmur_hash3.h"

#include <gtest/gtest.h>

namespace common {

class Murmur128HashTest : public ::testing::Test {
   protected:
    struct TestData {
        std::string string_val;
        uint32_t seed;
        int32_t expected_string_hash;

        TestData(std::string stringVal, uint32_t seed,
                 int32_t expectedStringHash)
            : string_val(std::move(stringVal)),
              seed(seed),
              expected_string_hash(expectedStringHash) {}
    };

    // Generated using Java Murmur128Hash
    TestData test_data_[3] = {
        TestData("testString", 12345, -1444917689),
        TestData("Special characters: @#$%^&*()_+", 24680, 580454411),
        TestData(
            "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do "
            "eiusmod tempor incididunt ut labore et dolore magna aliqua.",
            54321, -207787044)};
};

TEST_F(Murmur128HashTest, HashString) {
    for (const auto& data : test_data_) {
        std::string str(data.string_val);
        int32_t hash_result = Murmur128Hash::hash(str, data.seed);
        EXPECT_EQ(hash_result, data.expected_string_hash);
    }
}

}  // namespace common
