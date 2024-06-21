#include <gtest/gtest.h>

#include "encoding/plain_decoder.h"
#include "encoding/plain_encoder.h"

namespace storage {

TEST(PlainEncoderDecoderTest, EncodeDecodeBool) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    bool original = true;
    bool decoded = false;

    encoder.encode(original, stream);
    decoder.read_boolean(decoded, stream);

    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeInt32) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int32_t original = 12345;
    int32_t decoded = 0;

    encoder.encode(original, stream);
    decoder.read_int32(decoded, stream);

    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeInt64) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    int64_t original = 123456789012345LL;
    int64_t decoded = 0;

    encoder.encode(original, stream);
    decoder.read_int64(decoded, stream);

    EXPECT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeFloat) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    float original = 123.45f;
    float decoded = 0.0f;

    encoder.encode(original, stream);
    decoder.read_float(decoded, stream);

    EXPECT_FLOAT_EQ(original, decoded);
}

TEST(PlainEncoderDecoderTest, EncodeDecodeDouble) {
    PlainEncoder encoder;
    PlainDecoder decoder;
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    double original = 123.456789;
    double decoded = 0.0;

    encoder.encode(original, stream);
    decoder.read_double(decoded, stream);

    EXPECT_DOUBLE_EQ(original, decoded);
}

}  // end namespace storage