/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#include <gtest/gtest.h>

#include <random>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/float_rle_decoder.h"
#include "encoding/float_rle_encoder.h"

namespace storage {

namespace {

template <typename EncoderT, typename T>
std::vector<char> encode_rle_stream(const std::vector<T>& values) {
    common::ByteStream stream(1024, common::MOD_ENCODER_OBJ);
    EncoderT encoder;
    for (T value : values) {
        encoder.encode(value, stream);
    }
    encoder.flush(stream);
    std::vector<char> out;
    if (stream.total_size() == 0) {
        return out;
    }
    char* bytes = common::get_bytes_from_bytestream(stream);
    out.assign(bytes, bytes + stream.total_size());
    common::mem_free(bytes);
    return out;
}

int read_one(FloatRLEDecoder& decoder, float& value, common::ByteStream& in) {
    return decoder.read_float(value, in);
}

int read_one(DoubleRLEDecoder& decoder, double& value, common::ByteStream& in) {
    return decoder.read_double(value, in);
}

int read_batch(FloatRLEDecoder& decoder, float* out, int capacity, int& actual,
               common::ByteStream& in) {
    return decoder.read_batch_float(out, capacity, actual, in);
}

int read_batch(DoubleRLEDecoder& decoder, double* out, int capacity,
               int& actual, common::ByteStream& in) {
    return decoder.read_batch_double(out, capacity, actual, in);
}

// Decodes with either the scalar read (capacity == 0) or the batch read.
template <typename DecoderT, typename T>
std::vector<T> decode_rle_stream(const std::vector<char>& bytes, bool wrapped,
                                 int capacity) {
    std::vector<T> out;
    common::ByteStream owning(1024, common::MOD_DECODER_OBJ);
    common::ByteStream wrapped_stream;
    common::ByteStream* in = &owning;
    if (wrapped) {
        wrapped_stream.wrap_from(const_cast<char*>(bytes.data()),
                                 static_cast<int32_t>(bytes.size()));
        in = &wrapped_stream;
    } else {
        owning.write_buf(reinterpret_cast<const uint8_t*>(bytes.data()),
                         static_cast<uint32_t>(bytes.size()));
    }
    DecoderT decoder;
    decoder.reset();
    if (capacity == 0) {
        while (decoder.has_remaining(*in)) {
            T value = T();
            const int ret = read_one(decoder, value, *in);
            if (ret != common::E_OK) {
                break;
            }
            out.push_back(value);
        }
    } else {
        std::vector<T> buf(static_cast<size_t>(capacity));
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            const int ret =
                read_batch(decoder, buf.data(), capacity, actual, *in);
            if (ret != common::E_OK) {
                break;
            }
            if (actual == 0) {
                break;
            }
            out.insert(out.end(), buf.begin(), buf.begin() + actual);
        }
    }
    return out;
}

std::vector<float> float_samples() {
    std::mt19937 gen(20261109);
    std::vector<float> values;
    for (int i = 0; i < 1000; ++i) {
        const int32_t cents = static_cast<int32_t>(gen() % 200000) - 100000;
        values.push_back(static_cast<float>(cents) / 100.0f);
    }
    values.insert(values.end(), 300, 0.0f);
    values.insert(values.end(), 300, -1.25f);
    return values;
}

std::vector<double> double_samples() {
    std::mt19937_64 gen(20261110);
    std::vector<double> values;
    for (int i = 0; i < 1000; ++i) {
        const int64_t cents = static_cast<int64_t>(gen() % 2000000) - 1000000;
        values.push_back(static_cast<double>(cents) / 100.0);
    }
    values.insert(values.end(), 300, 0.0);
    values.insert(values.end(), 300, -3.14);
    return values;
}

void check_float_case(const std::vector<float>& input) {
    const std::vector<char> bytes =
        encode_rle_stream<FloatRLEEncoder, float>(input);
    for (bool wrapped : {false, true}) {
        const std::vector<float> scalar =
            decode_rle_stream<FloatRLEDecoder, float>(bytes, wrapped, 0);
        ASSERT_EQ(scalar, input) << "wrapped=" << wrapped;
        for (int capacity : {1, 3, 7, 64, 129, 1024}) {
            const std::vector<float> batch =
                decode_rle_stream<FloatRLEDecoder, float>(bytes, wrapped,
                                                          capacity);
            ASSERT_EQ(batch, scalar)
                << "wrapped=" << wrapped << " capacity=" << capacity;
        }
    }
}

void check_double_case(const std::vector<double>& input) {
    const std::vector<char> bytes =
        encode_rle_stream<DoubleRLEEncoder, double>(input);
    for (bool wrapped : {false, true}) {
        const std::vector<double> scalar =
            decode_rle_stream<DoubleRLEDecoder, double>(bytes, wrapped, 0);
        ASSERT_EQ(scalar, input) << "wrapped=" << wrapped;
        for (int capacity : {1, 3, 7, 64, 129, 1024}) {
            const std::vector<double> batch =
                decode_rle_stream<DoubleRLEDecoder, double>(bytes, wrapped,
                                                            capacity);
            ASSERT_EQ(batch, scalar)
                << "wrapped=" << wrapped << " capacity=" << capacity;
        }
    }
}

}  // namespace

TEST(FloatRleCodecTest, BatchDecodeMatchesScalar) {
    check_float_case(float_samples());
}

TEST(DoubleRleCodecTest, BatchDecodeMatchesScalar) {
    check_double_case(double_samples());
}

TEST(FloatRleCodecTest, BatchDecodeSmallPageResetBetweenPages) {
    // Two independent pages decoded with the same decoder object.
    FloatRLEDecoder decoder;
    const std::vector<float> first = float_samples();
    const std::vector<char> first_bytes =
        encode_rle_stream<FloatRLEEncoder, float>(first);
    common::ByteStream first_in;
    first_in.wrap_from(const_cast<char*>(first_bytes.data()),
                       static_cast<int32_t>(first_bytes.size()));
    std::vector<float> got(first.size());
    int actual = 0;
    decoder.reset();
    ASSERT_EQ(decoder.read_batch_float(got.data(), static_cast<int>(got.size()),
                                       actual, first_in),
              common::E_OK);
    ASSERT_EQ(actual, static_cast<int>(first.size()));
    ASSERT_EQ(got, first);

    std::vector<float> second;
    for (int i = 0; i < 37; ++i) {
        second.push_back(static_cast<float>(i) / 4.0f);
    }
    const std::vector<char> second_bytes =
        encode_rle_stream<FloatRLEEncoder, float>(second);
    common::ByteStream second_in;
    second_in.wrap_from(const_cast<char*>(second_bytes.data()),
                        static_cast<int32_t>(second_bytes.size()));
    got.assign(second.size(), 0.0f);
    actual = 0;
    decoder.reset();
    ASSERT_EQ(decoder.read_batch_float(got.data(), static_cast<int>(got.size()),
                                       actual, second_in),
              common::E_OK);
    ASSERT_EQ(actual, static_cast<int>(second.size()));
    ASSERT_EQ(got, second);
}

}  // namespace storage
