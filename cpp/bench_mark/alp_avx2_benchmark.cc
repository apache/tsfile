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

// Micro-benchmark for the ALP SIMD kernels: SIMDe (portable fallback) versus
// the runtime-dispatched native AVX2 copies. Build with the default flags
// (no -mavx2) so the SIMDe side is what a portable build ships:
//
//   cmake -DBUILD_BENCHMARK=ON -DBUILD_TEST=OFF ..
//   ./alp_avx2_benchmark
//
// When ALP AVX2 dispatch is not compiled (non-x86-64 targets) or the CPU has no
// AVX2, only the SIMDe column is measured.

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "encoding/alp/alp_scalar.h"

#ifdef ENABLE_SIMD
#include "encoding/alp/alp_simde.h"
#ifdef TSFILE_X86_AVX2_DISPATCH
#include "encoding/alp/alp_avx2.h"
#endif
#endif

namespace {

using Clock = std::chrono::steady_clock;
using storage::alp::AlpChooseFactorExponent;
using storage::alp::AlpPackBits;
using storage::alp::AlpSimdeKernel;
using storage::alp::AlpSimdeUnpackKernel;
using storage::alp::AlpTypeTraits;
#ifdef ALP_AVX2_DISPATCH
using storage::alp::avx2_detail::AlpAvx2Kernel;
using storage::alp::avx2_detail::AlpAvx2UnpackKernel;
#endif

const double kTargetBatchMs = 150.0;

// Keeps the timed kernels from being optimized away when the AVX2 column is
// not compiled in: the kernels write these buffers, and main consumes the
// checksum.
volatile uint64_t g_checksum_sink = 0;

template <typename T>
void Consume(const std::vector<T>& values) {
    uint64_t checksum = 0;
    for (size_t i = 0; i < values.size(); ++i) {
        uint64_t bits = 0;
        std::memcpy(&bits, &values[i], sizeof(T));
        checksum = checksum * 1099511628211ULL + bits;
    }
    g_checksum_sink ^= checksum;
}

template <typename Fn>
double MeasureMs(Fn fn, int repetitions) {
    const Clock::time_point start = Clock::now();
    for (int i = 0; i < repetitions; ++i) {
        fn();
    }
    const Clock::time_point end = Clock::now();
    return std::chrono::duration_cast<
               std::chrono::duration<double, std::milli>>(end - start)
               .count() /
           repetitions;
}

// Runs one timing sample, calibrating the repetition count so a sample takes
// roughly kTargetBatchMs. Returns nanoseconds per value.
template <typename Fn>
double TimeNsPerValue(Fn fn, uint32_t count) {
    if (count == 0) {
        return 0.0;
    }
    fn();  // warm up
    const double single_ms = MeasureMs(fn, 1);
    int repetitions = 1;
    if (single_ms > 0.0) {
        repetitions = static_cast<int>(kTargetBatchMs / single_ms);
    }
    repetitions = std::max(1, std::min(repetitions, 100000));
    const double ms = MeasureMs(fn, repetitions);
    return ms * 1.0e6 / static_cast<double>(count);
}

template <typename T>
std::vector<T> MakeDecimalData(uint64_t count, double scale, double offset) {
    std::vector<T> values(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        const double raw = static_cast<double>(i % 100000) * scale + offset;
        values[static_cast<size_t>(i)] = static_cast<T>(raw);
    }
    return values;
}

template <typename T>
std::vector<T> MakeRandomData(uint64_t count) {
    std::vector<T> values(static_cast<size_t>(count));
    uint64_t state = 0x9e3779b97f4a7c15ULL;
    for (uint64_t i = 0; i < count; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        const uint64_t bits = state ^ (state >> 29);
        if (sizeof(T) == sizeof(float)) {
            uint32_t fbits = static_cast<uint32_t>(bits);
            float v = 0.0f;
            std::memcpy(&v, &fbits, sizeof(v));
            if (!std::isfinite(v)) {
                v = static_cast<float>(i % 1024) * 0.001f;
            }
            values[static_cast<size_t>(i)] = v;
        } else {
            double v = 0.0;
            std::memcpy(&v, &bits, sizeof(v));
            if (!std::isfinite(v)) {
                v = static_cast<double>(i % 1024) * 0.001;
            }
            values[static_cast<size_t>(i)] = v;
        }
    }
    return values;
}

template <typename T>
std::vector<T> MakeConstantData(uint64_t count) {
    return std::vector<T>(static_cast<size_t>(count),
                          static_cast<T>(3.141592653589793));
}

void PrintHeader(const char* title) {
    std::cout << std::endl << "==== " << title << " ====" << std::endl;
    std::cout << std::left << std::setw(10) << "pattern" << std::setw(8)
              << "pair" << std::right << std::setw(16) << "simde(M/s)"
              << std::setw(16) << "avx2(M/s)" << std::setw(12) << "speedup"
              << std::setw(10) << "ok" << std::endl;
}

void PrintRow(const std::string& pattern, const std::string& pair,
              double simde_mps, double avx2_mps, bool ok) {
    std::cout << std::left << std::setw(10) << pattern << std::setw(8) << pair
              << std::right << std::setw(16) << std::fixed
              << std::setprecision(2) << simde_mps;
    if (avx2_mps > 0.0) {
        std::cout << std::setw(16) << avx2_mps << std::setw(12)
                  << std::setprecision(2) << (avx2_mps / simde_mps);
    } else {
        std::cout << std::setw(16) << "-" << std::setw(12) << "-";
    }
    std::cout << std::setw(10) << (ok ? "yes" : "no") << std::endl;
}

#ifdef ENABLE_SIMD

template <typename T>
void BenchEncode(const std::string& pattern, const std::vector<T>& values) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    const uint32_t count = static_cast<uint32_t>(values.size());
    uint8_t factor = 0;
    uint8_t exponent = 0;
    if (!AlpChooseFactorExponent(&values[0], count, factor, exponent)) {
        return;
    }

    std::vector<Encoded> simde_encoded(count, 0);
    std::vector<Encoded> avx2_encoded(count, 0);
    std::vector<uint8_t> simde_bitmap((count + 7) / 8, 0);
    std::vector<uint8_t> avx2_bitmap((count + 7) / 8, 0);
    std::vector<T> simde_exceptions(count, T());
    std::vector<T> avx2_exceptions(count, T());
    uint32_t simde_exception_count = 0;
    uint32_t avx2_exception_count = 0;
#ifndef ALP_AVX2_DISPATCH
    (void)avx2_exception_count;
#endif
    const double simde_ns = TimeNsPerValue(
        [&]() {
            simde_exception_count = 0;
            std::fill(simde_bitmap.begin(), simde_bitmap.end(), 0);
            AlpSimdeKernel<T>::EncodeValues(
                &values[0], count, factor, exponent, &simde_encoded[0],
                &simde_bitmap[0], &simde_exceptions[0], &simde_exception_count);
        },
        count);

    double avx2_ns = 0.0;
    bool ok = true;
#ifdef ALP_AVX2_DISPATCH
    if (common::cpu_has_avx2()) {
        avx2_ns = TimeNsPerValue(
            [&]() {
                avx2_exception_count = 0;
                std::fill(avx2_bitmap.begin(), avx2_bitmap.end(), 0);
                AlpAvx2Kernel<T>::EncodeValues(
                    &values[0], count, factor, exponent, &avx2_encoded[0],
                    &avx2_bitmap[0], &avx2_exceptions[0],
                    &avx2_exception_count);
            },
            count);
        ok = simde_exception_count == avx2_exception_count &&
             simde_bitmap == avx2_bitmap && simde_encoded == avx2_encoded;
        for (uint32_t i = 0; ok && i < simde_exception_count; ++i) {
            ok = Traits::BitwiseEqual(simde_exceptions[i], avx2_exceptions[i]);
        }
    }
#endif

    Consume(simde_encoded);
    Consume(simde_bitmap);
    Consume(simde_exceptions);
    Consume(avx2_encoded);
    Consume(avx2_bitmap);
    Consume(avx2_exceptions);

    std::string pair = std::to_string(static_cast<int>(factor)) + "/" +
                       std::to_string(static_cast<int>(exponent));
    const double simde_mps = simde_ns > 0.0 ? 1.0e3 / simde_ns : 0.0;
    const double avx2_mps = avx2_ns > 0.0 ? 1.0e3 / avx2_ns : 0.0;
    PrintRow(pattern, pair, simde_mps, avx2_mps, ok);
}

template <typename T>
void BenchDecode(const std::string& pattern, const std::vector<T>& values) {
    typedef AlpTypeTraits<T> Traits;
    typedef typename Traits::Encoded Encoded;
    const uint32_t count = static_cast<uint32_t>(values.size());
    uint8_t factor = 0;
    uint8_t exponent = 0;
    if (!AlpChooseFactorExponent(&values[0], count, factor, exponent)) {
        return;
    }

    std::vector<Encoded> encoded(count, 0);
    std::vector<uint8_t> bitmap((count + 7) / 8, 0);
    std::vector<T> exceptions(count, T());
    uint32_t exception_count = 0;
    AlpSimdeKernel<T>::EncodeValues(&values[0], count, factor, exponent,
                                    &encoded[0], &bitmap[0], &exceptions[0],
                                    &exception_count);

    std::vector<T> simde_out(count, T());
    std::vector<T> avx2_out(count, T());

    const double simde_ns = TimeNsPerValue(
        [&]() {
            AlpSimdeKernel<T>::DecodeValues(
                &encoded[0], count, factor, exponent, &bitmap[0],
                exception_count == 0 ? nullptr : &exceptions[0],
                exception_count, &simde_out[0]);
        },
        count);

    double avx2_ns = 0.0;
    bool ok = true;
#ifdef ALP_AVX2_DISPATCH
    if (common::cpu_has_avx2()) {
        avx2_ns = TimeNsPerValue(
            [&]() {
                AlpAvx2Kernel<T>::DecodeValues(
                    &encoded[0], count, factor, exponent, &bitmap[0],
                    exception_count == 0 ? nullptr : &exceptions[0],
                    exception_count, &avx2_out[0]);
            },
            count);
        for (uint32_t i = 0; ok && i < count; ++i) {
            ok = Traits::BitwiseEqual(simde_out[i], avx2_out[i]);
        }
    }
#endif

    Consume(simde_out);
    Consume(avx2_out);

    std::string pair = std::to_string(static_cast<int>(factor)) + "/" +
                       std::to_string(static_cast<int>(exponent));
    const double simde_mps = simde_ns > 0.0 ? 1.0e3 / simde_ns : 0.0;
    const double avx2_mps = avx2_ns > 0.0 ? 1.0e3 / avx2_ns : 0.0;
    PrintRow(pattern, pair, simde_mps, avx2_mps, ok);
}

template <typename Unsigned>
void BenchUnpack(uint8_t bit_width, uint32_t count) {
    const Unsigned mask =
        bit_width >= sizeof(Unsigned) * 8
            ? static_cast<Unsigned>(~static_cast<Unsigned>(0))
            : static_cast<Unsigned>((static_cast<uint64_t>(1) << bit_width) -
                                    1);
    std::vector<Unsigned> values(count, 0);
    uint64_t state = 0x243f6a8885a308d3ULL;
    for (uint32_t i = 0; i < count; ++i) {
        state = state * 6364136223846793005ULL + 1442695040888963407ULL;
        values[i] = static_cast<Unsigned>(state) & mask;
    }
    std::vector<uint8_t> body;
    AlpPackBits(&values[0], count, bit_width, body);

    std::vector<Unsigned> simde_out(count, 0);
    std::vector<Unsigned> avx2_out(count, 0);

    const double simde_ns = TimeNsPerValue(
        [&]() {
            AlpSimdeUnpackKernel<Unsigned>::Unpack(body.data(), count,
                                                   bit_width, &simde_out[0]);
        },
        count);

    double avx2_ns = 0.0;
    bool ok = true;
#ifdef ALP_AVX2_DISPATCH
    if (common::cpu_has_avx2()) {
        avx2_ns = TimeNsPerValue(
            [&]() {
                AlpAvx2UnpackKernel<Unsigned>::Unpack(body.data(), count,
                                                      bit_width, &avx2_out[0]);
            },
            count);
        ok = simde_out == avx2_out;
    }
#endif

    Consume(simde_out);
    Consume(avx2_out);

    std::string pair = "w=" + std::to_string(static_cast<int>(bit_width));
    const double simde_mps = simde_ns > 0.0 ? 1.0e3 / simde_ns : 0.0;
    const double avx2_mps = avx2_ns > 0.0 ? 1.0e3 / avx2_ns : 0.0;
    PrintRow(sizeof(Unsigned) == 4 ? "u32" : "u64", pair, simde_mps, avx2_mps,
             ok);
}

template <typename T>
void RunType(const std::string& type_name) {
    const uint64_t count = storage::alp::ALP_BLOCK_SIZE;
    const std::vector<T> decimal = MakeDecimalData<T>(count, 0.01, -1.0);
    const std::vector<T> random = MakeRandomData<T>(count);
    const std::vector<T> constant = MakeConstantData<T>(count);

    PrintHeader((type_name + " EncodeValues").c_str());
    BenchEncode<T>("decimal", decimal);
    BenchEncode<T>("random", random);
    BenchEncode<T>("constant", constant);

    PrintHeader((type_name + " DecodeValues").c_str());
    BenchDecode<T>("decimal", decimal);
    BenchDecode<T>("random", random);
    BenchDecode<T>("constant", constant);
}

#endif  // ENABLE_SIMD

}  // namespace

int main() {
#ifndef ENABLE_SIMD
    std::cout << "ENABLE_SIMD is OFF; nothing to benchmark." << std::endl;
    return 0;
#else
#ifdef ALP_AVX2_DISPATCH
    if (!common::cpu_has_avx2()) {
        std::cout << "CPU has no AVX2; only the SIMDe column is measured."
                  << std::endl;
    }
#else
    std::cout << "ALP AVX2 dispatch is not compiled on this target; only the "
                 "SIMDe column is measured."
              << std::endl;
#endif

    std::cout << "ALP SIMD kernel micro-benchmark, "
              << storage::alp::ALP_BLOCK_SIZE << " values per call"
              << std::endl;
    RunType<float>("FLOAT");
    RunType<double>("DOUBLE");

    PrintHeader("Unpack (uint32_t)");
    const uint8_t u32_widths[] = {1, 2, 3, 4, 5, 8, 12, 16, 20, 24, 28, 32};
    for (size_t i = 0; i < sizeof(u32_widths); ++i) {
        BenchUnpack<uint32_t>(u32_widths[i], storage::alp::ALP_BLOCK_SIZE);
    }
    PrintHeader("Unpack (uint64_t)");
    const uint8_t u64_widths[] = {1,  2,  4,  8,  12, 16, 20,
                                  24, 32, 40, 48, 56, 57};
    for (size_t i = 0; i < sizeof(u64_widths); ++i) {
        BenchUnpack<uint64_t>(u64_widths[i], storage::alp::ALP_BLOCK_SIZE);
    }
    std::cout << "sink " << g_checksum_sink << std::endl;
    return 0;
#endif
}
