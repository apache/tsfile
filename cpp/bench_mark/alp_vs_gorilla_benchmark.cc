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

#include "common/allocator/byte_stream.h"
#include "common/db_common.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "encoding/ts2diff_encoder.h"
#include "encoding/ts2diff_encoder.h"
#include "utils/errno_define.h"

namespace {

int g_ts2diff_mpn = -1;

using Clock = std::chrono::high_resolution_clock;

template <typename T>
struct BenchResult {
    std::string encoding;
    std::string pattern;
    uint64_t value_count;
    uint64_t raw_bytes;
    uint64_t encoded_bytes;
    double encode_ms;
    double decode_ms;
    double encode_mbps;
    double decode_mbps;
    bool correct;
};

template <typename T>
bool BitEqual(T lhs, T rhs) {
    return std::memcmp(&lhs, &rhs, sizeof(T)) == 0;
}

int CopyEncodedBytes(common::ByteStream& stream, std::vector<uint8_t>& bytes) {
    bytes.clear();
    bytes.reserve(static_cast<size_t>(stream.total_size()));
    common::ByteStream::BufferIterator iter = stream.init_buffer_iterator();
    while (true) {
        common::ByteStream::Buffer buffer = iter.get_next_buf();
        if (buffer.buf_ == NULL || buffer.len_ == 0) {
            break;
        }
        const uint8_t* begin = reinterpret_cast<const uint8_t*>(buffer.buf_);
        bytes.insert(bytes.end(), begin, begin + buffer.len_);
    }
    return bytes.size() == stream.total_size() ? common::E_OK
                                                : common::E_PARTIAL_READ;
}

template <typename T>
int EncodeOnce(common::TSEncoding encoding, common::TSDataType data_type,
               const std::vector<T>& values, std::vector<uint8_t>& bytes,
               double& elapsed_ms) {
    storage::Encoder* encoder =
        storage::EncoderFactory::alloc_value_encoder(encoding, data_type);
    if (encoder == NULL) {
        return common::E_NOT_SUPPORT;
    }
    if (encoding == common::TS_2DIFF && g_ts2diff_mpn >= 0) {
        if (data_type == common::FLOAT) {
            static_cast<storage::FloatTS2DIFFEncoder*>(encoder)
                ->set_max_point_number(g_ts2diff_mpn);
        } else if (data_type == common::DOUBLE) {
            static_cast<storage::DoubleTS2DIFFEncoder*>(encoder)
                ->set_max_point_number(g_ts2diff_mpn);
        }
    }
    common::ByteStream stream(1024, common::MOD_DEFAULT);
    const Clock::time_point start = Clock::now();
    int ret = encoder->encode_batch(&values[0],
                                    static_cast<uint32_t>(values.size()),
                                    stream);
    if (ret == common::E_OK) {
        ret = encoder->flush(stream);
    }
    const Clock::time_point end = Clock::now();
    elapsed_ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
            end - start)
            .count();
    if (ret == common::E_OK) {
        ret = CopyEncodedBytes(stream, bytes);
    }
    storage::EncoderFactory::free(encoder);
    return ret;
}

template <typename T>
int DecodeOnce(common::TSEncoding encoding, common::TSDataType data_type,
               const std::vector<uint8_t>& bytes, uint64_t count,
               std::vector<T>& decoded, double& elapsed_ms) {
    common::ByteStream stream;
    stream.wrap_from(reinterpret_cast<const char*>(&bytes[0]),
                     static_cast<int32_t>(bytes.size()));
    storage::Decoder* decoder =
        storage::DecoderFactory::alloc_value_decoder(encoding, data_type);
    if (decoder == NULL) {
        return common::E_NOT_SUPPORT;
    }
    decoded.resize(static_cast<size_t>(count));
    const Clock::time_point start = Clock::now();
    int ret = common::E_OK;
    if (data_type == common::FLOAT) {
        ret = decoder->read_exact_float(
            reinterpret_cast<float*>(decoded.empty() ? NULL : &decoded[0]),
            static_cast<int>(count), stream);
    } else {
        ret = decoder->read_exact_double(
            reinterpret_cast<double*>(decoded.empty() ? NULL : &decoded[0]),
            static_cast<int>(count), stream);
    }
    const Clock::time_point end = Clock::now();
    elapsed_ms =
        std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(
            end - start)
            .count();
    storage::DecoderFactory::free(decoder);
    return ret;
}

template <typename T>
BenchResult<T> RunCase(const std::string& encoding_name,
                       common::TSEncoding encoding,
                       const std::string& pattern,
                       const std::vector<T>& values,
                       uint32_t repetitions) {
    const common::TSDataType data_type =
        sizeof(T) == sizeof(float) ? common::FLOAT : common::DOUBLE;
    BenchResult<T> result;
    result.encoding = encoding_name;
    result.pattern = pattern;
    result.value_count = values.size();
    result.raw_bytes = values.size() * sizeof(T);
    result.encoded_bytes = 0;
    result.encode_ms = std::numeric_limits<double>::max();
    result.decode_ms = std::numeric_limits<double>::max();
    result.encode_mbps = 0.0;
    result.decode_mbps = 0.0;
    result.correct = true;

    std::vector<uint8_t> best_bytes;
    std::vector<T> decoded;
    for (uint32_t rep = 0; rep < repetitions; ++rep) {
        std::vector<uint8_t> bytes;
        double encode_ms = 0.0;
        const int encode_ret = EncodeOnce(encoding, data_type, values, bytes,
                                          encode_ms);
        if (encode_ret != common::E_OK) {
            result.correct = false;
            return result;
        }
        double decode_ms = 0.0;
        std::vector<T> rep_decoded;
        const int decode_ret = DecodeOnce(encoding, data_type, bytes,
                                          values.size(), rep_decoded,
                                          decode_ms);
        if (decode_ret != common::E_OK) {
            result.correct = false;
            return result;
        }
        if (rep == 0) {
            best_bytes.swap(bytes);
            decoded.swap(rep_decoded);
        }
        if (encode_ms < result.encode_ms) {
            result.encode_ms = encode_ms;
        }
        if (decode_ms < result.decode_ms) {
            result.decode_ms = decode_ms;
        }
    }

    for (size_t i = 0; i < values.size(); ++i) {
        if (!BitEqual(values[i], decoded[i])) {
            result.correct = false;
            break;
        }
    }

    result.encoded_bytes = best_bytes.size();
    const double value_count = static_cast<double>(values.size());
    if (result.encode_ms > 0.0) {
        result.encode_mbps =
            (static_cast<double>(result.raw_bytes) / (1024.0 * 1024.0)) /
            (result.encode_ms / 1000.0);
    }
    if (result.decode_ms > 0.0) {
        result.decode_mbps =
            (static_cast<double>(result.raw_bytes) / (1024.0 * 1024.0)) /
            (result.decode_ms / 1000.0);
    }
    return result;
}

template <typename T>
void PrintResult(const BenchResult<T>& result) {
    const double ratio =
        result.encoded_bytes == 0
            ? 0.0
            : static_cast<double>(result.raw_bytes) /
                  static_cast<double>(result.encoded_bytes);
    std::cout << std::left << std::setw(8) << result.encoding << std::setw(12)
              << result.pattern << std::right << std::setw(10)
              << result.value_count << std::setw(14) << result.raw_bytes
              << std::setw(14) << result.encoded_bytes << std::setw(10)
              << std::fixed << std::setprecision(3) << ratio << std::setw(12)
              << result.encode_ms << std::setw(12) << result.decode_ms
              << std::setw(14) << result.encode_mbps << std::setw(14)
              << result.decode_mbps << std::setw(10)
              << (result.correct ? "yes" : "no") << std::endl;
}

template <typename T>
std::vector<T> MakeDecimalData(uint64_t count, double scale,
                               double offset) {
    std::vector<T> values(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        const double raw = static_cast<double>(i % 100000) * scale + offset;
        values[static_cast<size_t>(i)] = static_cast<T>(raw);
    }
    return values;
}

template <typename T>
std::vector<T> MakeSmoothData(uint64_t count) {
    std::vector<T> values(static_cast<size_t>(count));
    for (uint64_t i = 0; i < count; ++i) {
        const double x = static_cast<double>(i) / 1000.0;
        values[static_cast<size_t>(i)] =
            static_cast<T>(std::sin(x) + 0.25 * std::cos(x * 0.5));
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

template <typename T>
void RunAll(const std::string& type_name, uint32_t repetitions,
            const std::string& codec_filter,
            const std::string& pattern_filter) {
    const uint64_t count = 1024 * 1024;
    const common::TSDataType data_type =
        sizeof(T) == sizeof(float) ? common::FLOAT : common::DOUBLE;

    struct Case {
        std::string name;
        std::vector<T> values;
    };
    std::vector<Case> cases;
    cases.push_back(Case{"decimal", MakeDecimalData<T>(count, 0.01, -1.0)});
    cases.push_back(Case{"smooth", MakeSmoothData<T>(count)});
    cases.push_back(Case{"random", MakeRandomData<T>(count)});
    cases.push_back(Case{"constant", MakeConstantData<T>(count)});

    std::cout << "==== " << type_name << " ====" << std::endl;
    std::cout << std::left << std::setw(8) << "codec" << std::setw(12)
              << "pattern" << std::right << std::setw(10) << "values"
              << std::setw(14) << "raw(B)" << std::setw(14) << "encoded(B)"
              << std::setw(10) << "ratio" << std::setw(12) << "enc(ms)"
              << std::setw(12) << "dec(ms)" << std::setw(14) << "enc(MB/s)"
              << std::setw(14) << "dec(MB/s)" << std::setw(10) << "ok"
              << std::endl;
    for (size_t c = 0; c < cases.size(); ++c) {
        if (!pattern_filter.empty() && cases[c].name != pattern_filter) {
            continue;
        }
        if (codec_filter.empty() || codec_filter == "ALP") {
            PrintResult(RunCase("ALP", common::ALP, cases[c].name,
                                cases[c].values, repetitions));
        }
        if (codec_filter.empty() || codec_filter == "GORILLA") {
            PrintResult(RunCase("GORILLA", common::GORILLA, cases[c].name,
                                cases[c].values, repetitions));
        }
        if (codec_filter.empty() || codec_filter == "TS_2DIFF") {
            PrintResult(RunCase("TS_2DIFF", common::TS_2DIFF, cases[c].name,
                                cases[c].values, repetitions));
        }
        if (codec_filter.empty() || codec_filter == "TS_2DIFF") {
            PrintResult(RunCase("TS_2DIFF", common::TS_2DIFF, cases[c].name,
                                cases[c].values, repetitions));
        }
    }
    (void)data_type;
}

}  // namespace

int main(int argc, char** argv) {
    uint32_t repetitions = 3;
    if (argc > 1) {
        repetitions = static_cast<uint32_t>(std::max(1, atoi(argv[1])));
    }
    const std::string codec_filter = argc > 2 ? argv[2] : "";
    const std::string type_filter = argc > 3 ? argv[3] : "";
    const std::string pattern_filter = argc > 4 ? argv[4] : "";
    g_ts2diff_mpn = argc > 5 ? atoi(argv[5]) : -1;
    std::cout << "ALP vs GORILLA vs TS_2DIFF codec benchmark, best of "
              << repetitions << " runs";
    if (g_ts2diff_mpn >= 0) {
        std::cout << ", TS_2DIFF mpn=" << g_ts2diff_mpn;
    } else {
        std::cout << ", TS_2DIFF mpn=2(float)/3(double)";
    }
    std::cout << std::endl;
    if (type_filter.empty() || type_filter == "FLOAT") {
        if (g_ts2diff_mpn < 0) {
            g_ts2diff_mpn = 2;
        }
        RunAll<float>("FLOAT", repetitions, codec_filter, pattern_filter);
    }
    if (type_filter.empty() || type_filter == "DOUBLE") {
        if (argc <= 5) {
            g_ts2diff_mpn = 3;
        }
        RunAll<double>("DOUBLE", repetitions, codec_filter, pattern_filter);
    }
    return 0;
}
