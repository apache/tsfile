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

// SPRINTZ decode throughput: the batch overrides against the per-value
// loop they replace, for all four value types.
//
//   ./sprintz_batch_decode_benchmark [values] [reps] [batch]
//
// The per-value arm calls storage::Decoder::read_batch_* qualified, which is
// exactly the loop that ran before the SPRINTZ overrides existed (it reads one
// value per virtual call). Built from a revision without the overrides, this
// file measures the old path unchanged, which isolates the per-block
// allocation removal from the batch loop itself.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "encoding/double_sprintz_decoder.h"
#include "encoding/double_sprintz_encoder.h"
#include "encoding/float_sprintz_decoder.h"
#include "encoding/float_sprintz_encoder.h"
#include "encoding/int32_sprintz_decoder.h"
#include "encoding/int32_sprintz_encoder.h"
#include "encoding/int64_sprintz_decoder.h"
#include "encoding/int64_sprintz_encoder.h"

namespace {

using Clock = std::chrono::high_resolution_clock;

template <typename T>
struct BatchApi;

template <>
struct BatchApi<int32_t> {
    typedef storage::Int32SprintzEncoder Encoder;
    typedef storage::Int32SprintzDecoder Decoder;

    static int Fast(Decoder& decoder, int32_t* out, int capacity, int& actual,
                    common::ByteStream& in) {
        return decoder.read_batch_int32(out, capacity, actual, in);
    }

    static int PerValue(Decoder& decoder, int32_t* out, int capacity,
                        int& actual, common::ByteStream& in) {
        return decoder.Decoder::read_batch_int32(out, capacity, actual, in);
    }
};

template <>
struct BatchApi<int64_t> {
    typedef storage::Int64SprintzEncoder Encoder;
    typedef storage::Int64SprintzDecoder Decoder;

    static int Fast(Decoder& decoder, int64_t* out, int capacity, int& actual,
                    common::ByteStream& in) {
        return decoder.read_batch_int64(out, capacity, actual, in);
    }

    static int PerValue(Decoder& decoder, int64_t* out, int capacity,
                        int& actual, common::ByteStream& in) {
        return decoder.Decoder::read_batch_int64(out, capacity, actual, in);
    }
};

template <>
struct BatchApi<float> {
    typedef storage::FloatSprintzEncoder Encoder;
    typedef storage::FloatSprintzDecoder Decoder;

    static int Fast(Decoder& decoder, float* out, int capacity, int& actual,
                    common::ByteStream& in) {
        return decoder.read_batch_float(out, capacity, actual, in);
    }

    static int PerValue(Decoder& decoder, float* out, int capacity, int& actual,
                        common::ByteStream& in) {
        return decoder.Decoder::read_batch_float(out, capacity, actual, in);
    }
};

template <>
struct BatchApi<double> {
    typedef storage::DoubleSprintzEncoder Encoder;
    typedef storage::DoubleSprintzDecoder Decoder;

    static int Fast(Decoder& decoder, double* out, int capacity, int& actual,
                    common::ByteStream& in) {
        return decoder.read_batch_double(out, capacity, actual, in);
    }

    static int PerValue(Decoder& decoder, double* out, int capacity,
                        int& actual, common::ByteStream& in) {
        return decoder.Decoder::read_batch_double(out, capacity, actual, in);
    }
};

template <typename T>
std::vector<uint8_t> EncodeAll(const std::vector<T>& values,
                               uint64_t& encoded_bytes) {
    typedef typename BatchApi<T>::Encoder Encoder;
    common::ByteStream stream(1 << 16, common::MOD_ENCODER_OBJ);
    Encoder encoder;
    for (size_t i = 0; i < values.size(); ++i) {
        encoder.encode(values[i], stream);
    }
    if (encoder.flush(stream) != common::E_OK) {
        std::cerr << "encode failed" << std::endl;
        std::exit(1);
    }
    std::vector<uint8_t> bytes;
    bytes.reserve(static_cast<size_t>(stream.total_size()));
    common::ByteStream::BufferIterator iter = stream.init_buffer_iterator();
    while (true) {
        common::ByteStream::Buffer buffer = iter.get_next_buf();
        if (buffer.buf_ == nullptr || buffer.len_ == 0) {
            break;
        }
        const uint8_t* begin = reinterpret_cast<const uint8_t*>(buffer.buf_);
        bytes.insert(bytes.end(), begin, begin + buffer.len_);
    }
    encoded_bytes = bytes.size();
    return bytes;
}

// One timed decode of the whole stream on the given arm.
template <typename T>
double DecodeOnce(const std::vector<uint8_t>& bytes, int batch,
                  int (*read)(typename BatchApi<T>::Decoder&, T*, int, int&,
                              common::ByteStream&),
                  uint64_t& decoded) {
    typedef typename BatchApi<T>::Decoder Decoder;
    common::ByteStream stream(1 << 16, common::MOD_DECODER_OBJ);
    stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                     static_cast<uint32_t>(bytes.size()));
    Decoder decoder;
    std::vector<T> buffer(static_cast<size_t>(batch));
    decoded = 0;
    const Clock::time_point start = Clock::now();
    while (decoder.has_remaining(stream)) {
        int actual = 0;
        if (read(decoder, buffer.data(), batch, actual, stream) !=
            common::E_OK) {
            std::cerr << "decode failed" << std::endl;
            std::exit(1);
        }
        if (actual <= 0) {
            break;
        }
        decoded += static_cast<uint64_t>(actual);
    }
    const Clock::time_point end = Clock::now();
    return std::chrono::duration_cast<std::chrono::duration<double>>(end -
                                                                     start)
        .count();
}

template <typename T>
bool DecodeEqual(const std::vector<uint8_t>& bytes, int batch,
                 std::vector<T>& fast_out, std::vector<T>& per_value_out) {
    typedef typename BatchApi<T>::Decoder Decoder;
    // Decode with each arm into scratch streams and compare value by value.
    common::ByteStream fast_stream(1 << 16, common::MOD_DECODER_OBJ);
    common::ByteStream slow_stream(1 << 16, common::MOD_DECODER_OBJ);
    fast_stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                          static_cast<uint32_t>(bytes.size()));
    slow_stream.write_buf(reinterpret_cast<const char*>(bytes.data()),
                          static_cast<uint32_t>(bytes.size()));
    Decoder fast_decoder;
    Decoder slow_decoder;
    fast_out.clear();
    per_value_out.clear();
    std::vector<T> a(static_cast<size_t>(batch));
    std::vector<T> b(static_cast<size_t>(batch));
    while (fast_decoder.has_remaining(fast_stream)) {
        int fast_actual = 0;
        int slow_actual = 0;
        if (BatchApi<T>::Fast(fast_decoder, a.data(), batch, fast_actual,
                              fast_stream) != common::E_OK ||
            BatchApi<T>::PerValue(slow_decoder, b.data(), batch, slow_actual,
                                  slow_stream) != common::E_OK) {
            return false;
        }
        if (fast_actual != slow_actual) {
            return false;
        }
        if (fast_actual <= 0) {
            break;
        }
        fast_out.insert(fast_out.end(), a.begin(), a.begin() + fast_actual);
        per_value_out.insert(per_value_out.end(), b.begin(),
                             b.begin() + slow_actual);
    }
    if (fast_out.size() != per_value_out.size()) {
        return false;
    }
    return std::memcmp(fast_out.data(), per_value_out.data(),
                       fast_out.size() * sizeof(T)) == 0;
}

template <typename T>
void RunPattern(const std::string& type_name, const std::string& pattern,
                const std::vector<T>& values, int batch, int reps) {
    typedef typename BatchApi<T>::Decoder Decoder;
    uint64_t encoded_bytes = 0;
    std::vector<uint8_t> bytes = EncodeAll(values, encoded_bytes);

    std::vector<T> fast_out;
    std::vector<T> slow_out;
    if (!DecodeEqual<T>(bytes, batch, fast_out, slow_out)) {
        std::cout << type_name << " " << pattern
                  << ": VALUE MISMATCH between arms" << std::endl;
    }

    double best_fast = 1e9;
    double best_slow = 1e9;
    uint64_t decoded_fast = 0;
    uint64_t decoded_slow = 0;
    for (int rep = 0; rep < reps; ++rep) {
        best_fast = std::min(
            best_fast,
            DecodeOnce<T>(bytes, batch, &BatchApi<T>::Fast, decoded_fast));
        best_slow = std::min(
            best_slow,
            DecodeOnce<T>(bytes, batch, &BatchApi<T>::PerValue, decoded_slow));
    }
    if (decoded_fast != decoded_slow) {
        std::cout << type_name << " " << pattern
                  << ": COUNT MISMATCH between arms" << std::endl;
    }

    const double fast_rates = decoded_fast / best_fast;
    const double slow_rates = decoded_slow / best_slow;
    std::cout << std::left << std::setw(8) << type_name << std::setw(12)
              << pattern << std::right << std::setw(10) << decoded_fast
              << std::setw(12) << encoded_bytes << std::setw(16) << std::fixed
              << std::setprecision(1) << slow_rates << std::setw(16)
              << fast_rates << std::setw(10) << std::setprecision(2)
              << (fast_rates / slow_rates) << "x" << std::endl;
}

template <typename T>
std::vector<T> SmoothValues(int count, uint32_t seed);

template <>
std::vector<int32_t> SmoothValues<int32_t>(int count, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<int32_t> out;
    out.reserve(static_cast<size_t>(count));
    int32_t value = 20000;
    for (int i = 0; i < count; ++i) {
        value += static_cast<int32_t>(rng() % 11) - 5;
        out.push_back(value);
    }
    return out;
}

template <>
std::vector<int64_t> SmoothValues<int64_t>(int count, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<int64_t> out;
    out.reserve(static_cast<size_t>(count));
    int64_t value = 1732000000000LL;  // epoch milliseconds
    for (int i = 0; i < count; ++i) {
        value += 1000 + static_cast<int64_t>(rng() % 7) - 3;
        out.push_back(value);
    }
    return out;
}

template <>
std::vector<float> SmoothValues<float>(int count, uint32_t seed) {
    std::mt19937 rng(seed);
    std::vector<float> out;
    out.reserve(static_cast<size_t>(count));
    float value = 20.0f;
    for (int i = 0; i < count; ++i) {
        value += 0.01f * static_cast<float>(static_cast<int>(rng() % 21) - 10);
        out.push_back(value);
    }
    return out;
}

template <>
std::vector<double> SmoothValues<double>(int count, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::vector<double> out;
    out.reserve(static_cast<size_t>(count));
    double value = 1000000.0;
    for (int i = 0; i < count; ++i) {
        value += 0.001 * static_cast<double>(static_cast<int>(rng() % 21) - 10);
        out.push_back(value);
    }
    return out;
}

template <typename T>
std::vector<T> HighEntropyValues(int count, uint32_t seed);

template <>
std::vector<int32_t> HighEntropyValues<int32_t>(int count, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> dist(-250000000, 250000000);
    std::vector<int32_t> out;
    out.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        out.push_back(dist(rng));
    }
    return out;
}

template <>
std::vector<int64_t> HighEntropyValues<int64_t>(int count, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> dist(-1000000000000000LL,
                                                1000000000000000LL);
    std::vector<int64_t> out;
    out.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        out.push_back(dist(rng));
    }
    return out;
}

template <>
std::vector<float> HighEntropyValues<float>(int count, uint32_t seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(1.0f, 8.0f);
    std::vector<float> out;
    out.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        out.push_back(dist(rng));
    }
    return out;
}

template <>
std::vector<double> HighEntropyValues<double>(int count, uint32_t seed) {
    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> dist(1.0, 8.0);
    std::vector<double> out;
    out.reserve(static_cast<size_t>(count));
    for (int i = 0; i < count; ++i) {
        out.push_back(dist(rng));
    }
    return out;
}

template <typename T>
void RunType(const std::string& type_name, int count, int batch, int reps) {
    RunPattern<T>(type_name, "smooth", SmoothValues<T>(count, 1), batch, reps);
    RunPattern<T>(type_name, "entropy", HighEntropyValues<T>(count, 2), batch,
                  reps);
    RunPattern<T>(type_name, "constant", std::vector<T>(count, T(0)), batch,
                  reps);
}

}  // namespace

int main(int argc, char** argv) {
    int count = argc > 1 ? std::atoi(argv[1]) : 4000000;
    int reps = argc > 2 ? std::atoi(argv[2]) : 5;
    int batch = argc > 3 ? std::atoi(argv[3]) : 129;

    std::cout << "SPRINTZ decode, " << count << " values, best of " << reps
              << ", batch capacity " << batch << std::endl;
    std::cout << std::left << std::setw(8) << "type" << std::setw(12)
              << "pattern" << std::right << std::setw(10) << "values"
              << std::setw(12) << "bytes" << std::setw(16) << "per-value/s"
              << std::setw(16) << "batch/s" << std::setw(10) << "speedup"
              << std::endl;

    RunType<int32_t>("INT32", count, batch, reps);
    RunType<int64_t>("INT64", count, batch, reps);
    RunType<float>("FLOAT", count, batch, reps);
    RunType<double>("DOUBLE", count, batch, reps);
    return 0;
}
