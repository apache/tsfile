#include "encoding/int32_sprintz_encoder.h"
#include "encoding/int32_sprintz_decoder.h"
#include "encoding/int64_sprintz_encoder.h"
#include "encoding/int64_sprintz_decoder.h"
#include <gtest/gtest.h>
#include <random>

class SprintzCodecTest : public ::testing::Test {
public:
    static void hex_dump(const uint8_t* buffer, size_t len) {
        std::cout << std::hex << std::setfill('0');
        for (size_t i = 0; i < len; ++i) {
            std::cout << std::setw(2) << static_cast<int>(buffer[i]) << " ";
            if ((i + 1) % 16 == 0) {  // 每16字节换行
                std::cout << std::endl;
            }
        }
        std::cout << std::dec << std::endl;  // 恢复十进制输出
    }
};

TEST_F(SprintzCodecTest, Int32BoundaryValueEncoding) {
    storage::Int32SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    std::vector<int32_t> encoded_nums = {INT32_MIN, INT32_MAX, -1, 0, 1};
    for (auto num : encoded_nums) {
        EXPECT_EQ(encoder.encode(num, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    storage::Int32SprintzDecoder decoder;
    for (int value : encoded_nums) {
        ASSERT_TRUE(decoder.has_next(stream));
        int32_t decoded;
        EXPECT_EQ(decoder.read_int32(decoded, stream), common::E_OK);
        EXPECT_EQ(decoded, value);
    }
}

TEST_F(SprintzCodecTest, EncodeDecodeRepeatedValues) {
    // Test encoding and decoding of repeated identical values.
    storage::Int32SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);

    std::vector<int32_t> values(128, 42); // 128 identical values
    for (int32_t val : values) {
        EXPECT_EQ(encoder.encode(val, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    // Decode and verify. used for debug
    // uint32_t len = stream.total_size();
    // uint8_t buffer[len];
    // uint32_t read_len = 0;
    // stream.read_buf(reinterpret_cast<char*>(buffer), len, read_len);
    // stream.wrap_from(reinterpret_cast<char*>(buffer), static_cast<int32_t>(read_len));

    storage::Int32SprintzDecoder decoder;
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(decoder.has_next(stream));
        int32_t decoded;
        EXPECT_EQ(decoder.read_int32(decoded, stream), common::E_OK);
        EXPECT_EQ(decoded, 42);
    }
}

TEST_F(SprintzCodecTest, EncodeDecodeAlternatingValues) {
    // Test encoding of a predictable alternating pattern (0, 1, 0, 1, ...)
    storage::Int32SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);

    std::vector<int32_t> values;
    for (int i = 0; i < 128; ++i) {
        int32_t val = (i % 2 == 0) ? 0 : 1;
        values.push_back(val);
        EXPECT_EQ(encoder.encode(val, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    // Decode and verify
    uint32_t len = stream.total_size();
    uint8_t buffer[len];
    uint32_t read_len = 0;
    stream.read_buf(reinterpret_cast<char*>(buffer), len, read_len);
    stream.wrap_from(reinterpret_cast<char*>(buffer), static_cast<int32_t>(read_len));

    storage::Int32SprintzDecoder decoder;
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(decoder.has_next(stream));
        int32_t decoded;
        EXPECT_EQ(decoder.read_int32(decoded, stream), common::E_OK);
        EXPECT_EQ(decoded, values[i]);
    }
}


TEST_F(SprintzCodecTest, AllZeroFlushTwice) {
    storage::Int32SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    int32_t value = 0;
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.flush(stream);
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.flush(stream);

    for (int i = 0; i < 2; ++i) {
        storage::Int32SprintzDecoder decoder;

        for (int j = 0; j < 3; ++j) {
            ASSERT_TRUE(decoder.has_next(stream));
            int32_t decoded_val;
            decoder.read_int32(decoded_val, stream);
            ASSERT_EQ(decoded_val, value);
        }
    }
}

TEST_F(SprintzCodecTest, Int64AllZeroFlushTwice) {
    storage::Int64SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    int64_t value = 0;
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.flush(stream);
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.encode(value, stream);
    encoder.flush(stream);

    for (int i = 0; i < 2; ++i) {
        storage::Int64SprintzDecoder decoder;

        for (int j = 0; j < 3; ++j) {
            ASSERT_TRUE(decoder.has_next(stream));
            int64_t decoded_val;
            decoder.read_int64(decoded_val, stream);
            ASSERT_EQ(decoded_val, value);
        }
    }
}

TEST_F(SprintzCodecTest, Int64BoundaryValueEncoding) {
    storage::Int64SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    std::vector<int64_t> encoded_nums = {INT64_MIN, INT64_MAX, -1, 0, 1};
    for (auto num : encoded_nums) {
        EXPECT_EQ(encoder.encode(num, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    storage::Int64SprintzDecoder decoder;
    for (int value : encoded_nums) {
        ASSERT_TRUE(decoder.has_next(stream));
        int64_t decoded;
        EXPECT_EQ(decoder.read_int64(decoded, stream), common::E_OK);
        EXPECT_EQ(decoded, value);
    }
}

TEST_F(SprintzCodecTest, Int64EncodeDecodeRepeatedValues) {
    // Test encoding and decoding of repeated identical values.
    storage::Int64SprintzEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);

    std::vector<int64_t> values(9, 42); // 128 identical values
    for (int64_t val : values) {
        EXPECT_EQ(encoder.encode(val, stream), common::E_OK);
    }
    EXPECT_EQ(encoder.flush(stream), common::E_OK);

    // Decode and verify. used for debug
    uint32_t len = stream.total_size();
    uint8_t buffer[len];
    uint32_t read_len = 0;
    stream.read_buf(reinterpret_cast<char*>(buffer), len, read_len);
    hex_dump(buffer, len);
    stream.wrap_from(reinterpret_cast<char*>(buffer), static_cast<int32_t>(read_len));

    storage::Int64SprintzDecoder decoder;
    for (size_t i = 0; i < values.size(); ++i) {
        ASSERT_TRUE(decoder.has_next(stream));
        int64_t decoded;
        EXPECT_EQ(decoder.read_int64(decoded, stream), common::E_OK);
        EXPECT_EQ(decoded, 42);
    }
}

TEST_F(SprintzCodecTest, Int64Simple) {
    std::vector<int> iterations = {1, 2, 8, 9, 16, 32, 100};
    for (int num : iterations) {
        storage::Int64SprintzEncoder encoder;
        common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
        int64_t value = 7;
        for (int i = 0; i < num; ++i) {
            encoder.encode(value + 2 * i, stream);
        }
        encoder.flush(stream);

        storage::Int64SprintzDecoder decoder;
        for (int i = 0; i < num; ++i) {
            ASSERT_TRUE(decoder.has_next(stream)) << "Decoder has no more values at i=" << i;
            int64_t decoded_val = 0;
            EXPECT_EQ(decoder.read_int64(decoded_val, stream), common::E_OK);
            EXPECT_EQ(decoded_val, value + 2 * i);
        }
        EXPECT_FALSE(decoder.has_next(stream));
    }
}








