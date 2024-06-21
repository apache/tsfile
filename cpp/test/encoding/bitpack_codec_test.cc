#include <gtest/gtest.h>

#include "encoding/bitpack_decoder.h"
#include "encoding/bitpack_encoder.h"

namespace storage {

class BitPackEncoderTest : public ::testing::Test {};

TEST_F(BitPackEncoderTest, EncodeInt32) {
    BitPackEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);

    int test_data[] = {5, 5, 5, 6, 6, 6, 6, 7, 7, 8, 9, 9, 9, 9, 9};
    for (int value : test_data) {
        encoder.encode(value, stream);
    }
    encoder.encode_flush(stream);

    EXPECT_EQ(stream.total_size(), 12);

    uint32_t want_len = 12, read_len;
    uint8_t real_buf[12] = {};
    stream.read_buf(real_buf, want_len, read_len);
    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    uint8_t expected_buf[12] = {11,  4,   5,   7,   85,  86,
                                102, 103, 120, 153, 153, 144};
    for (int i = 0; i < 12; i++) {
        EXPECT_EQ(real_buf[i], expected_buf[i]);
    }
}

TEST_F(BitPackEncoderTest, EncodeInt64) {
    BitPackEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);

    int64_t test_data[] = {10L, 20L, 10L, 10L, 30L,       30L,      30L,
                           20L, 20L, 10L, 10L, INT64_MAX, INT64_MIN};
    for (int64_t value : test_data) {
        encoder.encode(value, stream);
    }
    encoder.encode_flush(stream);

    EXPECT_EQ(stream.total_size(), 133);

    uint32_t want_len = 133, read_len;
    uint8_t real_buf[133] = {0};
    stream.read_buf(real_buf, want_len, read_len);

    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    int8_t expected_buf[133] = {
        -125, 1,  64, 5,  5, 0, 0, 0,  0,   0,  0,  0,  10, 0,  0,  0,  0,
        0,    0,  0,  20, 0, 0, 0, 0,  0,   0,  0,  10, 0,  0,  0,  0,  0,
        0,    0,  10, 0,  0, 0, 0, 0,  0,   0,  30, 0,  0,  0,  0,  0,  0,
        0,    30, 0,  0,  0, 0, 0, 0,  0,   30, 0,  0,  0,  0,  0,  0,  0,
        20,   0,  0,  0,  0, 0, 0, 0,  20,  0,  0,  0,  0,  0,  0,  0,  10,
        0,    0,  0,  0,  0, 0, 0, 10, 127, -1, -1, -1, -1, -1, -1, -1, -128,
        0,    0,  0,  0,  0, 0, 0, 0,  0,   0,  0,  0,  0,  0,  0,  0,  0,
        0,    0,  0,  0,  0, 0, 0, 0,  0,   0,  0,  0,  0,  0};
    for (int i = 0; i < 123; i++) {
        EXPECT_EQ(real_buf[i], (uint8_t)expected_buf[i]);
    }
}

TEST_F(BitPackEncoderTest, EncodeInt64Num1024) {
    BitPackEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);

    for (int64_t i = 0; i < 1024; i++) {
        encoder.encode(i, stream);
    }
    encoder.encode_flush(stream);

    EXPECT_EQ(stream.total_size(), 1289);

    uint32_t want_len = 1289, read_len;
    uint8_t real_buf[1289] = {0};
    stream.read_buf(real_buf, want_len, read_len);

    EXPECT_EQ(want_len, read_len);
    // Generated using Java Edition
    int8_t expected_buf[1289] = {
        -121, 10,   10,   127,  8,    0,    0,    16,   8,    3,    1,    0,
        80,   24,   7,    2,    0,    -112, 40,   11,   3,    0,    -48,  56,
        15,   4,    1,    16,   72,   19,   5,    1,    80,   88,   23,   6,
        1,    -112, 104,  27,   7,    1,    -48,  120,  31,   8,    2,    16,
        -120, 35,   9,    2,    80,   -104, 39,   10,   2,    -112, -88,  43,
        11,   2,    -48,  -72,  47,   12,   3,    16,   -56,  51,   13,   3,
        80,   -40,  55,   14,   3,    -112, -24,  59,   15,   3,    -48,  -8,
        63,   16,   4,    17,   8,    67,   17,   4,    81,   24,   71,   18,
        4,    -111, 40,   75,   19,   4,    -47,  56,   79,   20,   5,    17,
        72,   83,   21,   5,    81,   88,   87,   22,   5,    -111, 104,  91,
        23,   5,    -47,  120,  95,   24,   6,    17,   -120, 99,   25,   6,
        81,   -104, 103,  26,   6,    -111, -88,  107,  27,   6,    -47,  -72,
        111,  28,   7,    17,   -56,  115,  29,   7,    81,   -40,  119,  30,
        7,    -111, -24,  123,  31,   7,    -47,  -8,   127,  32,   8,    18,
        8,    -125, 33,   8,    82,   24,   -121, 34,   8,    -110, 40,   -117,
        35,   8,    -46,  56,   -113, 36,   9,    18,   72,   -109, 37,   9,
        82,   88,   -105, 38,   9,    -110, 104,  -101, 39,   9,    -46,  120,
        -97,  40,   10,   18,   -120, -93,  41,   10,   82,   -104, -89,  42,
        10,   -110, -88,  -85,  43,   10,   -46,  -72,  -81,  44,   11,   18,
        -56,  -77,  45,   11,   82,   -40,  -73,  46,   11,   -110, -24,  -69,
        47,   11,   -46,  -8,   -65,  48,   12,   19,   8,    -61,  49,   12,
        83,   24,   -57,  50,   12,   -109, 40,   -53,  51,   12,   -45,  56,
        -49,  52,   13,   19,   72,   -45,  53,   13,   83,   88,   -41,  54,
        13,   -109, 104,  -37,  55,   13,   -45,  120,  -33,  56,   14,   19,
        -120, -29,  57,   14,   83,   -104, -25,  58,   14,   -109, -88,  -21,
        59,   14,   -45,  -72,  -17,  60,   15,   19,   -56,  -13,  61,   15,
        83,   -40,  -9,   62,   15,   -109, -24,  -5,   63,   15,   -45,  -8,
        -1,   64,   16,   20,   9,    3,    65,   16,   84,   25,   7,    66,
        16,   -108, 41,   11,   67,   16,   -44,  57,   15,   68,   17,   20,
        73,   19,   69,   17,   84,   89,   23,   70,   17,   -108, 105,  27,
        71,   17,   -44,  121,  31,   72,   18,   20,   -119, 35,   73,   18,
        84,   -103, 39,   74,   18,   -108, -87,  43,   75,   18,   -44,  -71,
        47,   76,   19,   20,   -55,  51,   77,   19,   84,   -39,  55,   78,
        19,   -108, -23,  59,   79,   19,   -44,  -7,   63,   80,   20,   21,
        9,    67,   81,   20,   85,   25,   71,   82,   20,   -107, 41,   75,
        83,   20,   -43,  57,   79,   84,   21,   21,   73,   83,   85,   21,
        85,   89,   87,   86,   21,   -107, 105,  91,   87,   21,   -43,  121,
        95,   88,   22,   21,   -119, 99,   89,   22,   85,   -103, 103,  90,
        22,   -107, -87,  107,  91,   22,   -43,  -71,  111,  92,   23,   21,
        -55,  115,  93,   23,   85,   -39,  119,  94,   23,   -107, -23,  123,
        95,   23,   -43,  -7,   127,  96,   24,   22,   9,    -125, 97,   24,
        86,   25,   -121, 98,   24,   -106, 41,   -117, 99,   24,   -42,  57,
        -113, 100,  25,   22,   73,   -109, 101,  25,   86,   89,   -105, 102,
        25,   -106, 105,  -101, 103,  25,   -42,  121,  -97,  104,  26,   22,
        -119, -93,  105,  26,   86,   -103, -89,  106,  26,   -106, -87,  -85,
        107,  26,   -42,  -71,  -81,  108,  27,   22,   -55,  -77,  109,  27,
        86,   -39,  -73,  110,  27,   -106, -23,  -69,  111,  27,   -42,  -7,
        -65,  112,  28,   23,   9,    -61,  113,  28,   87,   25,   -57,  114,
        28,   -105, 41,   -53,  115,  28,   -41,  57,   -49,  116,  29,   23,
        73,   -45,  117,  29,   87,   89,   -41,  118,  29,   -105, 105,  -37,
        119,  29,   -41,  121,  -33,  120,  30,   23,   -119, -29,  121,  30,
        87,   -103, -25,  122,  30,   -105, -87,  -21,  123,  30,   -41,  -71,
        -17,  124,  31,   23,   -55,  -13,  125,  31,   87,   -39,  -9,   127,
        8,    126,  31,   -105, -23,  -5,   127,  31,   -41,  -7,   -1,   -128,
        32,   24,   10,   3,    -127, 32,   88,   26,   7,    -126, 32,   -104,
        42,   11,   -125, 32,   -40,  58,   15,   -124, 33,   24,   74,   19,
        -123, 33,   88,   90,   23,   -122, 33,   -104, 106,  27,   -121, 33,
        -40,  122,  31,   -120, 34,   24,   -118, 35,   -119, 34,   88,   -102,
        39,   -118, 34,   -104, -86,  43,   -117, 34,   -40,  -70,  47,   -116,
        35,   24,   -54,  51,   -115, 35,   88,   -38,  55,   -114, 35,   -104,
        -22,  59,   -113, 35,   -40,  -6,   63,   -112, 36,   25,   10,   67,
        -111, 36,   89,   26,   71,   -110, 36,   -103, 42,   75,   -109, 36,
        -39,  58,   79,   -108, 37,   25,   74,   83,   -107, 37,   89,   90,
        87,   -106, 37,   -103, 106,  91,   -105, 37,   -39,  122,  95,   -104,
        38,   25,   -118, 99,   -103, 38,   89,   -102, 103,  -102, 38,   -103,
        -86,  107,  -101, 38,   -39,  -70,  111,  -100, 39,   25,   -54,  115,
        -99,  39,   89,   -38,  119,  -98,  39,   -103, -22,  123,  -97,  39,
        -39,  -6,   127,  -96,  40,   26,   10,   -125, -95,  40,   90,   26,
        -121, -94,  40,   -102, 42,   -117, -93,  40,   -38,  58,   -113, -92,
        41,   26,   74,   -109, -91,  41,   90,   90,   -105, -90,  41,   -102,
        106,  -101, -89,  41,   -38,  122,  -97,  -88,  42,   26,   -118, -93,
        -87,  42,   90,   -102, -89,  -86,  42,   -102, -86,  -85,  -85,  42,
        -38,  -70,  -81,  -84,  43,   26,   -54,  -77,  -83,  43,   90,   -38,
        -73,  -82,  43,   -102, -22,  -69,  -81,  43,   -38,  -6,   -65,  -80,
        44,   27,   10,   -61,  -79,  44,   91,   26,   -57,  -78,  44,   -101,
        42,   -53,  -77,  44,   -37,  58,   -49,  -76,  45,   27,   74,   -45,
        -75,  45,   91,   90,   -41,  -74,  45,   -101, 106,  -37,  -73,  45,
        -37,  122,  -33,  -72,  46,   27,   -118, -29,  -71,  46,   91,   -102,
        -25,  -70,  46,   -101, -86,  -21,  -69,  46,   -37,  -70,  -17,  -68,
        47,   27,   -54,  -13,  -67,  47,   91,   -38,  -9,   -66,  47,   -101,
        -22,  -5,   -65,  47,   -37,  -6,   -1,   -64,  48,   28,   11,   3,
        -63,  48,   92,   27,   7,    -62,  48,   -100, 43,   11,   -61,  48,
        -36,  59,   15,   -60,  49,   28,   75,   19,   -59,  49,   92,   91,
        23,   -58,  49,   -100, 107,  27,   -57,  49,   -36,  123,  31,   -56,
        50,   28,   -117, 35,   -55,  50,   92,   -101, 39,   -54,  50,   -100,
        -85,  43,   -53,  50,   -36,  -69,  47,   -52,  51,   28,   -53,  51,
        -51,  51,   92,   -37,  55,   -50,  51,   -100, -21,  59,   -49,  51,
        -36,  -5,   63,   -48,  52,   29,   11,   67,   -47,  52,   93,   27,
        71,   -46,  52,   -99,  43,   75,   -45,  52,   -35,  59,   79,   -44,
        53,   29,   75,   83,   -43,  53,   93,   91,   87,   -42,  53,   -99,
        107,  91,   -41,  53,   -35,  123,  95,   -40,  54,   29,   -117, 99,
        -39,  54,   93,   -101, 103,  -38,  54,   -99,  -85,  107,  -37,  54,
        -35,  -69,  111,  -36,  55,   29,   -53,  115,  -35,  55,   93,   -37,
        119,  -34,  55,   -99,  -21,  123,  -33,  55,   -35,  -5,   127,  -32,
        56,   30,   11,   -125, -31,  56,   94,   27,   -121, -30,  56,   -98,
        43,   -117, -29,  56,   -34,  59,   -113, -28,  57,   30,   75,   -109,
        -27,  57,   94,   91,   -105, -26,  57,   -98,  107,  -101, -25,  57,
        -34,  123,  -97,  -24,  58,   30,   -117, -93,  -23,  58,   94,   -101,
        -89,  -22,  58,   -98,  -85,  -85,  -21,  58,   -34,  -69,  -81,  -20,
        59,   30,   -53,  -77,  -19,  59,   94,   -37,  -73,  -18,  59,   -98,
        -21,  -69,  -17,  59,   -34,  -5,   -65,  -16,  60,   31,   11,   -61,
        -15,  60,   95,   27,   -57,  -14,  60,   -97,  43,   -53,  -13,  60,
        -33,  59,   -49,  -12,  61,   31,   75,   -45,  -11,  61,   95,   91,
        -41,  -10,  61,   -97,  107,  -37,  -9,   61,   -33,  123,  -33,  -8,
        62,   31,   -117, -29,  -7,   62,   95,   -101, -25,  -6,   62,   -97,
        -85,  -21,  -5,   62,   -33,  -69,  -17,  5,    8,    -4,   63,   31,
        -53,  -13,  -3,   63,   95,   -37,  -9,   -2,   63,   -97,  -21,  -5,
        -1,   63,   -33,  -5,   -1};
    for (int i = 0; i < 1289; i++) {
        ASSERT_EQ(real_buf[i], (uint8_t)expected_buf[i]);
    }
}

TEST_F(BitPackEncoderTest, EncodeFlush) {
    BitPackEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    encoder.encode(1, stream);
    encoder.encode_flush(stream);
    EXPECT_GT(stream.total_size(), 0);
}

TEST_F(BitPackEncoderTest, GetIntMaxBitWidth) {
    BitPackEncoder encoder;
    std::vector<int64_t> values = {1, 2, 3, 4, 5, 255};
    int bit_width = encoder.get_int_max_bit_width(values);
    EXPECT_EQ(bit_width, 8);
}

TEST_F(BitPackEncoderTest, ClearBuffer) {
    BitPackEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    encoder.encode(1, stream);
    encoder.clear_buffer();
}

TEST_F(BitPackEncoderTest, Flush) {
    BitPackEncoder encoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    encoder.encode(1, stream);
    encoder.flush(stream);
    EXPECT_GT(stream.total_size(), 0);
}

class BitPackDecoderTest : public ::testing::Test {};

TEST_F(BitPackDecoderTest, HasNext) {
    BitPackDecoder decoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    bool result = decoder.has_next(stream);
    EXPECT_FALSE(result);
}

TEST_F(BitPackDecoderTest, HasNextPackage) {
    BitPackDecoder decoder;
    bool result = decoder.has_next_package();
    EXPECT_FALSE(result);
}

TEST_F(BitPackDecoderTest, ReadInt64) {
    BitPackDecoder decoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    BitPackEncoder encoder;
    for (int64_t i = 0; i < 1024; i++) {
        encoder.encode(i, stream);
    }
    encoder.encode_flush(stream);

    decoder.init();
    for (int64_t i = 0; i < 1024; i++) {
        ASSERT_EQ(i, decoder.read_int(stream));
    }
}

TEST_F(BitPackDecoderTest, ReadInt64LargeQuantities) {
    BitPackDecoder decoder;
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    BitPackEncoder encoder;
    for (int64_t i = 0; i < 10000; i++) {
        encoder.encode(i, stream);
    }
    encoder.encode_flush(stream);

    decoder.init();
    for (int64_t i = 0; i < 10000; i++) {
        ASSERT_EQ(i, decoder.read_int(stream));
    }
}

TEST_F(BitPackDecoderTest, Destroy) {
    BitPackDecoder decoder;
    decoder.destroy();
}

}  // namespace storage
