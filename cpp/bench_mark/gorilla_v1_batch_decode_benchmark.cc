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

// Compares the per-value GORILLA_V1 decode loop (the pre-batch behavior, which
// Decoder::read_batch_double/int32 still performs) with the raw-pointer batch
// decoder added in gorilla_v1_decoder.h, on the same encoded stream.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/gorilla_v1_decoder.h"
#include "encoding/gorilla_v1_encoder.h"
#include "utils/errno_define.h"

namespace {

using Clock = std::chrono::high_resolution_clock;

template <typename T>
struct BenchCodec;

template <>
struct BenchCodec<float> {
    typedef storage::FloatGorillaV1Encoder Encoder;
    typedef storage::FloatGorillaV1Decoder Decoder;
    static int read_batch(Decoder& decoder, float* out, int capacity,
                          int& actual, common::ByteStream& in) {
        return decoder.read_batch_float(out, capacity, actual, in);
    }
    // The pre-batch implementation: qualified call, no virtual dispatch to the
    // override, so Decoder::read_batch_float's per-value loop runs.
    static int read_batch_per_value(Decoder& decoder, float* out, int capacity,
                                    int& actual, common::ByteStream& in) {
        return decoder.Decoder::read_batch_float(out, capacity, actual, in);
    }
    static uint64_t bits(float value) {
        return static_cast<uint64_t>(
            static_cast<uint32_t>(common::float_to_int(value)));
    }
    static uint64_t nan_bits() { return 0x7FC00000ULL; }
};

template <>
struct BenchCodec<double> {
    typedef storage::DoubleGorillaV1Encoder Encoder;
    typedef storage::DoubleGorillaV1Decoder Decoder;
    static int read_batch(Decoder& decoder, double* out, int capacity,
                          int& actual, common::ByteStream& in) {
        return decoder.read_batch_double(out, capacity, actual, in);
    }
    static int read_batch_per_value(Decoder& decoder, double* out, int capacity,
                                    int& actual, common::ByteStream& in) {
        return decoder.Decoder::read_batch_double(out, capacity, actual, in);
    }
    static uint64_t bits(double value) {
        return static_cast<uint64_t>(common::double_to_long(value));
    }
    static uint64_t nan_bits() { return 0x7FF8000000000000ULL; }
};

template <typename T>
std::vector<uint8_t> EncodeV1(const std::vector<T>& values) {
    typename BenchCodec<T>::Encoder encoder;
    common::ByteStream stream(65536, common::MOD_DEFAULT);
    for (size_t i = 0; i < values.size(); i++) {
        encoder.encode(values[i], stream);
    }
    encoder.flush(stream);
    std::vector<uint8_t> bytes(static_cast<size_t>(stream.total_size()));
    uint32_t read_len = 0;
    stream.read_buf(bytes.data(), static_cast<uint32_t>(bytes.size()),
                    read_len);
    bytes.resize(read_len);
    return bytes;
}

template <typename T>
bool DecodeAll(int mode, const std::vector<uint8_t>& bytes, uint64_t count,
               std::vector<T>& out, double& elapsed_ms) {
    typename BenchCodec<T>::Decoder decoder;
    common::ByteStream in(common::MOD_DEFAULT);
    in.wrap_from(reinterpret_cast<const char*>(bytes.data()),
                 static_cast<int32_t>(bytes.size()));
    out.assign(static_cast<size_t>(count), static_cast<T>(0));
    const Clock::time_point start = Clock::now();
    int ret = common::E_OK;
    int decoded = 0;
    while (decoded < static_cast<int>(count)) {
        int actual = 0;
        ret = mode == 0 ? BenchCodec<T>::read_batch_per_value(
                              decoder, out.data() + decoded,
                              static_cast<int>(count) - decoded, actual, in)
                        : BenchCodec<T>::read_batch(
                              decoder, out.data() + decoded,
                              static_cast<int>(count) - decoded, actual, in);
        if (ret != common::E_OK) {
            break;
        }
        if (actual == 0) {
            break;
        }
        decoded += actual;
    }
    const Clock::time_point end = Clock::now();
    elapsed_ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
            end - start)
            .count();
    return ret == common::E_OK && decoded == static_cast<int>(count);
}

template <typename T>
std::vector<T> MakeSmoothSeries(uint64_t count) {
    std::vector<T> values(static_cast<size_t>(count));
    double level = 100.0;
    uint64_t noise = 0x9E3779B97F4A7C15ULL;
    for (uint64_t i = 0; i < count; i++) {
        noise = noise * 6364136223846793005ULL + 1442695040888963407ULL;
        const double jitter =
            (static_cast<double>(noise >> 40) / 16777216.0 - 0.5) * 0.01;
        level += std::sin(static_cast<double>(i) / 500.0) * 0.05 + jitter;
        values[static_cast<size_t>(i)] = static_cast<T>(level);
    }
    return values;
}

template <typename T>
std::vector<T> MakeHighEntropy(uint64_t count) {
    std::vector<T> values(static_cast<size_t>(count));
    uint64_t state = 0x243F6A8885A308D3ULL;
    for (uint64_t i = 0; i < count; i++) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const uint64_t bits = state ^ (state >> 31);
        if (sizeof(T) == sizeof(float)) {
            const uint32_t narrow = static_cast<uint32_t>(bits);
            float value = 0.0f;
            std::memcpy(&value, &narrow, sizeof(value));
            if (std::isnan(value)) {
                value = static_cast<float>(i % 1024) * 0.001f;
            }
            values[static_cast<size_t>(i)] = value;
        } else {
            double value = 0.0;
            std::memcpy(&value, &bits, sizeof(value));
            if (std::isnan(value)) {
                value = static_cast<double>(i % 1024) * 0.001;
            }
            values[static_cast<size_t>(i)] = value;
        }
    }
    return values;
}

template <typename T>
void RunCase(const char* type_name, const std::string& pattern,
             const std::vector<T>& values, int repetitions) {
    const std::vector<uint8_t> bytes = EncodeV1(values);
    const uint64_t count = values.size();

    std::vector<T> old_out;
    std::vector<T> new_out;
    double best_old_ms = std::numeric_limits<double>::max();
    double best_new_ms = std::numeric_limits<double>::max();
    bool correct = true;
    for (int rep = 0; rep < repetitions; rep++) {
        double ms = 0.0;
        if (!DecodeAll<T>(0, bytes, count, old_out, ms)) {
            correct = false;
        }
        best_old_ms = std::min(best_old_ms, ms);
        if (!DecodeAll<T>(1, bytes, count, new_out, ms)) {
            correct = false;
        }
        best_new_ms = std::min(best_new_ms, ms);
    }
    for (uint64_t i = 0; i < count; i++) {
        if (BenchCodec<T>::bits(old_out[static_cast<size_t>(i)]) !=
            BenchCodec<T>::bits(new_out[static_cast<size_t>(i)])) {
            correct = false;
            break;
        }
    }

    const double values_per_sec_old =
        best_old_ms > 0.0 ? static_cast<double>(count) / (best_old_ms / 1000.0)
                          : 0.0;
    const double values_per_sec_new =
        best_new_ms > 0.0 ? static_cast<double>(count) / (best_new_ms / 1000.0)
                          : 0.0;
    const double speedup = best_new_ms > 0.0 ? best_old_ms / best_new_ms : 0.0;
    std::cout << std::left << std::setw(8) << type_name << std::setw(14)
              << pattern << std::right << std::setw(12) << count
              << std::setw(14) << bytes.size() << std::setw(12) << std::fixed
              << std::setprecision(3) << best_old_ms << std::setw(12)
              << best_new_ms << std::setw(16) << std::setprecision(0)
              << values_per_sec_old << std::setw(16) << values_per_sec_new
              << std::setw(10) << std::setprecision(2) << speedup
              << std::setw(8) << (correct ? "yes" : "no") << std::endl;
}

template <typename T>
void RunAll(const char* type_name, uint64_t count, int repetitions) {
    std::cout << "==== " << type_name << " ====" << std::endl;
    std::cout << std::left << std::setw(8) << "type" << std::setw(14)
              << "pattern" << std::right << std::setw(12) << "values"
              << std::setw(14) << "encoded(B)" << std::setw(12) << "old(ms)"
              << std::setw(12) << "new(ms)" << std::setw(16) << "old val/s"
              << std::setw(16) << "new val/s" << std::setw(10) << "speedup"
              << std::setw(8) << "ok" << std::endl;
    RunCase<T>(type_name, "smooth", MakeSmoothSeries<T>(count), repetitions);
    RunCase<T>(type_name, "high-entropy", MakeHighEntropy<T>(count),
               repetitions);
    RunCase<T>(type_name, "constant",
               std::vector<T>(static_cast<size_t>(count), static_cast<T>(3.5)),
               repetitions);
}

}  // namespace

int main(int argc, char** argv) {
    uint64_t count = 4 * 1024 * 1024;
    int repetitions = 5;
    if (argc > 1) {
        count = static_cast<uint64_t>(std::max(1, atoi(argv[1])));
    }
    if (argc > 2) {
        repetitions = std::max(1, atoi(argv[2]));
    }
    std::cout << "GORILLA_V1 per-value vs batch decode, " << count
              << " values, best of " << repetitions << " runs" << std::endl;
    RunAll<double>("DOUBLE", count, repetitions);
    RunAll<float>("FLOAT", count, repetitions);
    return 0;
}
