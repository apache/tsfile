/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <unordered_set>
#include <vector>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#else
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif
#include <fcntl.h>

#include "common/config/config.h"
#include "common/db_common.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "file/local_random_access_read_file.h"
#include "file/tsfile_io_reader.h"
#include "file/write_file.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_table_writer.h"

using namespace common;
using namespace storage;

namespace {

const char* const kGenerateDirEnv = "TSFILE_COMPAT_GENERATE_DIR";
const char* const kValidateDirEnv = "TSFILE_COMPAT_VALIDATE_DIR";
const char* const kManifestFile = "manifest.csv";
const char* const kManifestHeader =
    "file,table,tagColumn,valueColumn,dataType,encoding,compression,rowCount,"
    "nullPattern,booleanEncoding";
const char* const kTableName = "compat_table";
const char* const kTagColumn = "device";
const char* const kValueColumn = "value";
const char* const kTagValue = "compat_device";
const char* const kBooleanColumn = "boolean_value";
constexpr int kRowCounts[] = {32, 4096, 1000000};
constexpr uint32_t kNullPercent = 10;
const char* const kAnchorColumn = "anchor";
const int kNullPageRows = 64;

const int32_t kIntValues[] = {
    0,
    1,
    -1,
    7,
    -8,
    1024,
    -1024,
    123456,
    -654321,
    std::numeric_limits<int32_t>::max() - 1024,
    std::numeric_limits<int32_t>::min() + 1024,
    42,
    42,
    0,
    -8,
    2048,
};

const int64_t kLongValues[] = {
    0LL,
    1LL,
    -1LL,
    7LL,
    -8LL,
    1234567890LL,
    -987654321LL,
    4294967296LL,
    -4294967296LL,
    1234567890123LL,
    -1234567890123LL,
    std::numeric_limits<int64_t>::max() - 4096,
    std::numeric_limits<int64_t>::min() + 4096,
    42LL,
    42LL,
    0LL,
};

const uint32_t kFloatBits[] = {
    0x00000000U, 0x80000000U, 0x3f800000U, 0xbf800000U,
    0x41280000U, 0xc0700000U, 0x44801000U, 0xc5002000U,
    0x40490fdbU, 0xc02df854U, 0x3f800000U, 0x3f800000U,
    0x00000000U, 0x80000000U, 0x42f6e979U, 0xc2f6e979U,
};

const uint64_t kDoubleBits[] = {
    UINT64_C(0x0000000000000000), UINT64_C(0x8000000000000000),
    UINT64_C(0x3ff0000000000000), UINT64_C(0xbff0000000000000),
    UINT64_C(0x4029000000000000), UINT64_C(0xc00c000000000000),
    UINT64_C(0x4090008000000000), UINT64_C(0xc0a0010000000000),
    UINT64_C(0x400921fb54442d18), UINT64_C(0xc005bf0a8b145769),
    UINT64_C(0x3ff0000000000000), UINT64_C(0x3ff0000000000000),
    UINT64_C(0x0000000000000000), UINT64_C(0x8000000000000000),
    UINT64_C(0x405edd2f1a9fbe77), UINT64_C(0xc05edd2f1a9fbe77),
};

// FLOAT/DOUBLE + TS_2DIFF converts values to fixed-point with
// maxPointNumber (10^mpn). The expected value is computed by applying the
// same tri-state conversion the writer performs and then decoding it back,
// so expectations reflect the encoding rules rather than the raw input
// bits. Every chosen value has the same expected value whether the writer
// used maxPointNumber 0 (explicit property; the "*.mpn0" Java fixtures) or
// 2 (the TSFileConfig.floatPrecision default shared with the C++ writer),
// so the validating reader never needs to know which writer produced the
// file. The values cover all page layouts the writers can produce:
//  - plain integers (scaled form; at mpn = 0 every finite in-range value
//    is scaled, so pages written at mpn = 0 contain no bitmap)
//  - 1.5e9f / 1e18: the scaled product overflows while round(v) still
//    fits -> scale-overflow form, single page-wide bitmap (reachable only
//    at mpn > 0, i.e. the default writer configuration)
//  - NaN and +/-Infinity (stored as raw IEEE bits -> two page-wide
//    bitmaps; Java floatToIntBits canonicalizes NaN)
const uint32_t kTs2DiffFloatBits[] = {
    0x00000000U, 0x3f800000U, 0xbf800000U, 0x461c4000U,
    0xc61c4000U, 0x4eb2d05eU, 0x7f800000U, 0xff800000U,
    0x7fc00000U, 0x3f800000U, 0x00000000U, 0x4a742400U,
    0xca742400U, 0x4e6e6b28U, 0x3f800000U, 0x00000000U,
};

const uint64_t kTs2DiffDoubleBits[] = {
    UINT64_C(0x0000000000000000), UINT64_C(0x3ff0000000000000),
    UINT64_C(0xbff0000000000000), UINT64_C(0x40c3880000000000),
    UINT64_C(0xc0c3880000000000), UINT64_C(0x43abc16d674ec800),
    UINT64_C(0x7ff0000000000000), UINT64_C(0xfff0000000000000),
    UINT64_C(0x7ff8000000000000), UINT64_C(0x3ff0000000000000),
    UINT64_C(0x0000000000000000), UINT64_C(0x41f0000000000000),
    UINT64_C(0xc1cd6f3458800000), UINT64_C(0x430c6bf526340000),
    UINT64_C(0x3ff0000000000000), UINT64_C(0x0000000000000000),
};

// maxPointNumber used by the C++ FloatTS2DIFFEncoder/DoubleTS2DIFFEncoder,
// matching the standard Java schema write path (TSEncodingBuilder.Ts2Diff
// initFromProps -> TSFileConfig.floatPrecision, current default 2).
constexpr int kTs2DiffMaxPointNumber = 2;
constexpr double kTs2DiffMaxPointValue = 100.0;

const int32_t kDateValues[] = {
    19700101, 19991231, 20000229, 20240229,
    20380119, 20500615, 19690720, 19800106,
};

struct FixtureCase {
    std::string file_name;
    std::string table_name;
    std::string tag_column;
    std::string value_column;
    TSDataType data_type;
    TSEncoding encoding;
    CompressionType compression;
    int row_count;
    std::string null_pattern = "NONE";
    TSEncoding boolean_encoding = PLAIN;

    FixtureCase()
        : table_name(kTableName),
          tag_column(kTagColumn),
          value_column(kValueColumn),
          data_type(INVALID_DATATYPE),
          encoding(INVALID_ENCODING),
          compression(INVALID_COMPRESSION),
          row_count(0) {}

    FixtureCase(TSDataType data_type, TSEncoding encoding,
                CompressionType compression, int row_count)
        : file_name(FileName(data_type, encoding, compression)),
          table_name(kTableName),
          tag_column(kTagColumn),
          value_column(kValueColumn),
          data_type(data_type),
          encoding(encoding),
          compression(compression),
          row_count(row_count) {}

    static std::string Lower(std::string value) {
        for (char& c : value) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return value;
    }

    static std::string FileName(TSDataType data_type, TSEncoding encoding,
                                CompressionType compression) {
        return Lower(get_encoding_name(encoding)) + "_" +
               Lower(get_data_type_name(data_type)) + "_" +
               Lower(get_compression_name(compression)) + ".tsfile";
    }

    std::string ToManifestLine() const {
        std::ostringstream out;
        out << file_name << "," << table_name << "," << tag_column << ","
            << value_column << "," << get_data_type_name(data_type) << ","
            << get_encoding_name(encoding) << ","
            << get_compression_name(compression) << "," << row_count << ","
            << null_pattern << "," << get_encoding_name(boolean_encoding);
        return out.str();
    }
};

std::vector<std::string> Split(const std::string& line, char delimiter) {
    std::vector<std::string> parts;
    std::stringstream stream(line);
    std::string part;
    while (std::getline(stream, part, delimiter)) {
        parts.push_back(part);
    }
    if (!line.empty() && line.back() == delimiter) {
        parts.emplace_back();
    }
    return parts;
}

bool ParseEncodingName(const std::string& value, TSEncoding& out) {
    for (int encoding = PLAIN; encoding <= CAMEL; ++encoding) {
        TSEncoding candidate = static_cast<TSEncoding>(encoding);
        if (value == get_encoding_name(candidate)) {
            out = candidate;
            return true;
        }
    }
    return false;
}

bool ParseCompressionName(const std::string& value, CompressionType& out) {
    for (int compression = UNCOMPRESSED; compression <= LZMA2; ++compression) {
        CompressionType candidate = static_cast<CompressionType>(compression);
        if (value == get_compression_name(candidate)) {
            out = candidate;
            return true;
        }
    }
    return false;
}

FixtureCase ParseManifestLine(const std::string& line) {
    FixtureCase fixture_case;
    std::vector<std::string> parts = Split(line, ',');
    if (parts.size() != 10) {
        ADD_FAILURE() << "Bad manifest line: " << line;
        return fixture_case;
    }
    fixture_case.file_name = parts[0];
    fixture_case.table_name = parts[1];
    fixture_case.tag_column = parts[2];
    fixture_case.value_column = parts[3];
    if (!parse_data_type_name(parts[4], fixture_case.data_type)) {
        ADD_FAILURE() << "Bad data type in manifest line: " << line;
    }
    if (!ParseEncodingName(parts[5], fixture_case.encoding)) {
        ADD_FAILURE() << "Bad encoding in manifest line: " << line;
    }
    if (!ParseCompressionName(parts[6], fixture_case.compression)) {
        ADD_FAILURE() << "Bad compression in manifest line: " << line;
    }
    fixture_case.row_count = std::atoi(parts[7].c_str());
    fixture_case.null_pattern = parts[8];
    EXPECT_TRUE(ParseEncodingName(parts[9], fixture_case.boolean_encoding));
    EXPECT_TRUE(fixture_case.null_pattern == "NONE" ||
                fixture_case.null_pattern == "RANDOM" ||
                fixture_case.null_pattern == "MIXED" ||
                fixture_case.null_pattern == "ALL");
    return fixture_case;
}

std::string JoinPath(const std::string& directory, const std::string& file) {
    if (directory.empty()) {
        return file;
    }
    char last = directory[directory.size() - 1];
    if (last == '/' || last == '\\') {
        return directory + file;
    }
    return directory + "/" + file;
}

void MakeDirectory(const std::string& directory) {
    if (directory.empty()) {
        return;
    }
#ifdef _WIN32
    int ret = _mkdir(directory.c_str());
#else
    int ret = mkdir(directory.c_str(), 0777);
#endif
    if (ret != 0 && errno != EEXIST) {
        ADD_FAILURE() << "Failed to create directory " << directory << ": "
                      << std::strerror(errno);
    }
}

void EnsureDirectory(const std::string& directory) {
    size_t pos = 0;
    while ((pos = directory.find_first_of("/\\", pos + 1)) !=
           std::string::npos) {
        if (pos > 0) {
            MakeDirectory(directory.substr(0, pos));
        }
    }
    MakeDirectory(directory);
}

int32_t IntValue(int row) {
    return kIntValues[row % (sizeof(kIntValues) / sizeof(kIntValues[0]))];
}

int64_t LongValue(int row) {
    return kLongValues[row % (sizeof(kLongValues) / sizeof(kLongValues[0]))];
}

uint32_t FloatBits(float value) {
    uint32_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

float BitsToFloat(uint32_t bits) {
    float value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

float FloatValue(int row) {
    uint32_t bits =
        kFloatBits[row % (sizeof(kFloatBits) / sizeof(kFloatBits[0]))];
    float value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

float Ts2DiffFloatValue(int row) {
    uint32_t bits = kTs2DiffFloatBits[row % (sizeof(kTs2DiffFloatBits) /
                                             sizeof(kTs2DiffFloatBits[0]))];
    float value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

uint64_t DoubleBits(double value) {
    uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

double BitsToDouble(uint64_t bits) {
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

double DoubleValue(int row) {
    uint64_t bits =
        kDoubleBits[row % (sizeof(kDoubleBits) / sizeof(kDoubleBits[0]))];
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

double Ts2DiffDoubleValue(int row) {
    uint64_t bits = kTs2DiffDoubleBits[row % (sizeof(kTs2DiffDoubleBits) /
                                              sizeof(kTs2DiffDoubleBits[0]))];
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

bool IsTs2DiffFloatCase(const FixtureCase& fixture_case) {
    return fixture_case.encoding == TS_2DIFF &&
           (fixture_case.data_type == FLOAT ||
            fixture_case.data_type == DOUBLE);
}

bool StringEndsWith(const std::string& value, const std::string& suffix) {
    return value.size() >= suffix.size() &&
           value.compare(value.size() - suffix.size(), suffix.size(), suffix) ==
               0;
}

// maxPointValue of the writer configuration for this fixture: the
// "*.mpn0" Java fixtures carry an explicit max_point_number=0 property,
// all others use the shared default (see kTs2DiffMaxPointNumber).
double Ts2DiffMaxPointValue(const FixtureCase& fixture_case) {
    return StringEndsWith(fixture_case.file_name, ".mpn0")
               ? 1.0
               : kTs2DiffMaxPointValue;
}

// Java Math.round(x) == floor(x + 0.5) (ties towards +infinity).
static double JavaRound(double x) { return std::floor(x + 0.5); }

float ExpectedFloatValue(const FixtureCase& fixture_case, int row) {
    if (!IsTs2DiffFloatCase(fixture_case)) {
        return FloatValue(row);
    }
    float value = Ts2DiffFloatValue(row);
    if (std::isnan(value)) {
        // Java FloatEncoder normalizes any NaN to the canonical pattern
        // via floatToIntBits.
        return BitsToFloat(0x7fc00000U);
    }
    // Mirror FloatTS2DIFFEncoder::convert_float_to_int, then
    // FloatDecoder::readFloat: scaled -> stored / mpv, scale-overflow ->
    // stored / 1, raw bits -> intBitsToFloat.
    const double mpv = Ts2DiffMaxPointValue(fixture_case);
    const double scaled = static_cast<double>(value) * mpv;
    const bool scaled_overflow =
        scaled > static_cast<double>(std::numeric_limits<int32_t>::max()) ||
        scaled < static_cast<double>(std::numeric_limits<int32_t>::min());
    if (scaled_overflow) {
        const bool value_overflows =
            value > static_cast<float>(std::numeric_limits<int32_t>::max()) ||
            value < static_cast<float>(std::numeric_limits<int32_t>::min());
        if (value_overflows) {
            // Raw IEEE bits, restored losslessly (infinite values here).
            return value;
        }
        return static_cast<float>(JavaRound(value)) / 1.0f;
    }
    return static_cast<float>(JavaRound(scaled) / mpv);
}

double ExpectedDoubleValue(const FixtureCase& fixture_case, int row) {
    if (!IsTs2DiffFloatCase(fixture_case)) {
        return DoubleValue(row);
    }
    double value = Ts2DiffDoubleValue(row);
    if (std::isnan(value)) {
        return BitsToDouble(UINT64_C(0x7ff8000000000000));
    }
    const double mpv = Ts2DiffMaxPointValue(fixture_case);
    const double scaled = value * mpv;
    const bool scaled_overflow =
        scaled > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
        scaled < static_cast<double>(std::numeric_limits<int64_t>::min());
    if (scaled_overflow) {
        const bool value_overflows =
            value > static_cast<double>(std::numeric_limits<int64_t>::max()) ||
            value < static_cast<double>(std::numeric_limits<int64_t>::min());
        if (value_overflows) {
            return value;
        }
        return JavaRound(value) / 1.0;
    }
    return JavaRound(scaled) / mpv;
}

int32_t DateValue(int row) {
    return kDateValues[row % (sizeof(kDateValues) / sizeof(kDateValues[0]))];
}

std::vector<FixtureCase> BuildMatrix() {
    std::vector<FixtureCase> cases;
    CompressionType compressions[] = {UNCOMPRESSED, SNAPPY, GZIP,
                                      LZ4,          ZSTD,   LZMA2};
    TSDataType data_types[] = {INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE};
    TSDataType all_types[] = {BOOLEAN, INT32,  DATE, INT64,  TIMESTAMP,
                              FLOAT,   DOUBLE, TEXT, STRING, BLOB};
    for (CompressionType compression : compressions) {
        // Fail rather than silently dropping an advertised compressor from CI.
        Compressor* compressor =
            CompressorFactory::alloc_compressor(compression);
        EXPECT_NE(nullptr, compressor) << get_compression_name(compression);
        if (compressor != nullptr) CompressorFactory::free(compressor);
        for (TSDataType data_type : all_types) {
            for (int value = PLAIN; value <= CAMEL; ++value) {
                TSEncoding encoding = static_cast<TSEncoding>(value);
                Encoder* encoder =
                    EncoderFactory::alloc_value_encoder(encoding, data_type);
                Decoder* decoder =
                    DecoderFactory::alloc_value_decoder(encoding, data_type);
                bool supported = encoder != nullptr && decoder != nullptr;
                if (encoder != nullptr) EncoderFactory::free(encoder);
                if (decoder != nullptr) DecoderFactory::free(decoder);
                if (supported)
                    cases.emplace_back(data_type, encoding, compression,
                                       kRowCounts[0]);
            }
        }
    }
    for (CompressionType compression : {UNCOMPRESSED, LZ4, ZSTD, LZMA2}) {
        for (TSDataType data_type : data_types) {
            for (const char* pattern : {"MIXED", "ALL"}) {
                FixtureCase fixture(data_type, PLAIN, compression,
                                    kNullPageRows * 8);
                fixture.null_pattern = pattern;
                fixture.file_name += "." + FixtureCase::Lower(pattern) + ".512";
                cases.push_back(fixture);
            }
            FixtureCase fixture(data_type, PLAIN, compression, kNullPageRows);
            fixture.null_pattern = "ALL";
            fixture.file_name += ".all.64";
            cases.push_back(fixture);
        }
    }
    std::vector<FixtureCase> scaled;
    const char* compression_filter = std::getenv("TSFILE_COMPAT_COMPRESSION");
    const char* rows_filter = std::getenv("TSFILE_COMPAT_ROWS");
    for (const FixtureCase& fixture : cases) {
        if (fixture.null_pattern != "NONE") {
            scaled.push_back(fixture);
        } else {
            for (int rows : kRowCounts) {
                FixtureCase copy = fixture;
                copy.row_count = rows;
                copy.null_pattern = "RANDOM";
                copy.file_name += "." + std::to_string(rows);
                scaled.push_back(copy);
            }
        }
    }
    scaled.erase(
        std::remove_if(scaled.begin(), scaled.end(),
                       [&](const FixtureCase& f) {
                           return (compression_filter != nullptr &&
                                   std::string(get_compression_name(
                                       f.compression)) != compression_filter) ||
                                  (rows_filter != nullptr &&
                                   f.row_count != std::atoi(rows_filter));
                       }),
        scaled.end());
    return scaled;
}

std::set<std::string> CoverageKeys(const std::vector<FixtureCase>& cases) {
    std::set<std::string> keys;
    const char* compression_filter = std::getenv("TSFILE_COMPAT_COMPRESSION");
    const char* rows_filter = std::getenv("TSFILE_COMPAT_ROWS");
    for (const FixtureCase& f : cases) {
        if ((compression_filter != nullptr &&
             std::string(get_compression_name(f.compression)) !=
                 compression_filter) ||
            (rows_filter != nullptr && f.row_count != std::atoi(rows_filter)))
            continue;
        keys.insert(std::string(get_data_type_name(f.data_type)) + "/" +
                    get_encoding_name(f.encoding) + "/" +
                    get_compression_name(f.compression) + "/" +
                    std::to_string(f.row_count) + "/" + f.null_pattern);
    }
    return keys;
}

// Same fixed-seed unsigned 32-bit mixing and per-column salts as Java.
uint32_t RowHash(int row, uint32_t salt) {
    uint32_t value =
        (static_cast<uint32_t>(row) + 1) * UINT32_C(0x9e3779b9) ^ salt;
    value ^= value >> 16;
    value *= UINT32_C(0x85ebca6b);
    value ^= value >> 13;
    return value;
}

bool RandomNull(int row, uint32_t salt) {
    return RowHash(row, salt) % 100 < kNullPercent;
}

bool BooleanNull(int row) { return RandomNull(row, UINT32_C(0x87654321)); }

bool BooleanValue(int row) {
    return row % 192 < 64 ? row % 2 == 0 : row % 192 < 128;
}

// Shared contract with the Java test: empty first/middle/consecutive/trailing
// pages, non-empty pages 1/5, and alternating null values inside page 4.
bool IsNull(const FixtureCase& fixture_case, int row) {
    if (fixture_case.null_pattern == "RANDOM")
        return RandomNull(row, UINT32_C(0x12345678));
    if (fixture_case.null_pattern == "ALL") return true;
    if (fixture_case.null_pattern == "NONE") return false;
    int page = row / kNullPageRows;
    return page == 0 || page == 2 || page == 3 || page >= 6 ||
           (page == 4 && row % 2 == 0);
}

// Restore process-wide settings even when a fatal assertion returns early.
class ScopedNullPageSize {
   public:
    explicit ScopedNullPageSize(const FixtureCase& fixture_case)
        : old_rows_(g_config_value_.page_writer_max_point_num_),
          old_bytes_(g_config_value_.page_writer_max_memory_bytes_) {
        if (fixture_case.null_pattern != "NONE") {
            g_config_value_.page_writer_max_point_num_ =
                fixture_case.null_pattern == "RANDOM" ? 256 : kNullPageRows;
            g_config_value_.page_writer_max_memory_bytes_ = 1024 * 1024;
        }
    }
    ~ScopedNullPageSize() {
        g_config_value_.page_writer_max_point_num_ = old_rows_;
        g_config_value_.page_writer_max_memory_bytes_ = old_bytes_;
    }

   private:
    uint32_t old_rows_;
    uint32_t old_bytes_;
};

TableSchema* CreateTableSchema(const FixtureCase& fixture_case) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.emplace_back(new MeasurementSchema(
        fixture_case.tag_column, STRING, PLAIN, UNCOMPRESSED));
    column_categories.emplace_back(ColumnCategory::TAG);
    measurement_schemas.emplace_back(
        new MeasurementSchema(fixture_case.value_column, fixture_case.data_type,
                              fixture_case.encoding, fixture_case.compression));
    column_categories.emplace_back(ColumnCategory::FIELD);
    measurement_schemas.emplace_back(new MeasurementSchema(
        kBooleanColumn, BOOLEAN, fixture_case.boolean_encoding,
        fixture_case.compression));
    column_categories.emplace_back(ColumnCategory::FIELD);
    if (fixture_case.null_pattern != "NONE") {
        measurement_schemas.emplace_back(new MeasurementSchema(
            kAnchorColumn, INT64, PLAIN, fixture_case.compression));
        column_categories.emplace_back(ColumnCategory::FIELD);
    }
    return new TableSchema(fixture_case.table_name, measurement_schemas,
                           column_categories);
}

std::string BinaryValue(int row) {
    static const std::string values[] = {
        "", "alpha", std::string("\x00\x01\xff\x7f", 4), "alpha", "中文"};
    return values[row % 5];
}

void AddValue(Tablet& tablet, const FixtureCase& fixture_case, int row) {
    switch (fixture_case.data_type) {
        case BOOLEAN:
            ASSERT_EQ(E_OK,
                      tablet.add_value(row, kValueColumn, BooleanValue(row)));
            break;
        case TEXT:
        case STRING:
        case BLOB: {
            std::string bytes = BinaryValue(row);
            common::String value(bytes.data(),
                                 static_cast<uint32_t>(bytes.size()));
            ASSERT_EQ(E_OK, tablet.add_value(row, kValueColumn, value));
            break;
        }
        case INT32:
            ASSERT_EQ(E_OK, tablet.add_value(row, kValueColumn, IntValue(row)));
            break;
        case DATE:
            ASSERT_EQ(E_OK,
                      tablet.add_value(row, kValueColumn, DateValue(row)));
            break;
        case INT64:
        case TIMESTAMP:
            ASSERT_EQ(E_OK,
                      tablet.add_value(row, kValueColumn, LongValue(row)));
            break;
        case FLOAT:
            ASSERT_EQ(E_OK,
                      tablet.add_value(row, kValueColumn,
                                       ExpectedFloatValue(fixture_case, row)));
            break;
        case DOUBLE:
            ASSERT_EQ(E_OK,
                      tablet.add_value(row, kValueColumn,
                                       ExpectedDoubleValue(fixture_case, row)));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

Tablet CreateTablet(TableSchema* table_schema,
                    const FixtureCase& fixture_case) {
    Tablet tablet(
        table_schema->get_table_name(), table_schema->get_measurement_names(),
        table_schema->get_data_types(), table_schema->get_column_categories(),
        fixture_case.row_count);
    for (int row = 0; row < fixture_case.row_count; ++row) {
        EXPECT_EQ(E_OK, tablet.add_timestamp(row, row));
        EXPECT_EQ(E_OK, tablet.add_value(row, kTagColumn, kTagValue));
        // Tablet entries remain null until add_value is called.
        if (!IsNull(fixture_case, row)) {
            AddValue(tablet, fixture_case, row);
        }
        if (!BooleanNull(row)) {
            EXPECT_EQ(E_OK,
                      tablet.add_value(row, kBooleanColumn, BooleanValue(row)));
        }
        if (fixture_case.null_pattern != "NONE") {
            EXPECT_EQ(E_OK, tablet.add_value(row, kAnchorColumn,
                                             static_cast<int64_t>(row)));
        }
    }
    return tablet;
}

void WriteFixture(const std::string& directory,
                  const FixtureCase& fixture_case) {
    ScopedNullPageSize page_size(fixture_case);
    std::string file_name = JoinPath(directory, fixture_case.file_name);
    remove(file_name.c_str());
    WriteFile write_file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(E_OK, write_file.create(file_name, flags, 0666));
    TableSchema* table_schema = CreateTableSchema(fixture_case);
    TsFileTableWriter writer(&write_file, table_schema);
    Tablet tablet = CreateTablet(table_schema, fixture_case);
    ASSERT_EQ(E_OK, writer.write_table(tablet));
    ASSERT_EQ(E_OK, writer.flush());
    ASSERT_EQ(E_OK, writer.close());
    delete table_schema;
}

void WriteManifest(const std::string& directory,
                   const std::vector<FixtureCase>& cases) {
    std::ofstream out(JoinPath(directory, kManifestFile).c_str(),
                      std::ios::out | std::ios::trunc);
    ASSERT_TRUE(out.good());
    out << kManifestHeader << "\n";
    for (const FixtureCase& fixture_case : cases) {
        out << fixture_case.ToManifestLine() << "\n";
    }
}

std::vector<FixtureCase> ReadManifest(const std::string& directory) {
    std::ifstream in(JoinPath(directory, kManifestFile).c_str());
    EXPECT_TRUE(in.good());
    std::vector<FixtureCase> cases;
    std::string line;
    if (!std::getline(in, line)) {
        ADD_FAILURE() << "Empty manifest";
        return cases;
    }
    EXPECT_EQ(kManifestHeader, line);
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (!line.empty()) {
            cases.push_back(ParseManifestLine(line));
        }
    }
    return cases;
}

void AssertValue(const FixtureCase& fixture_case, int row,
                 TableResultSet* result_set) {
    switch (fixture_case.data_type) {
        case TEXT:
        case STRING:
        case BLOB:
            ASSERT_EQ(
                BinaryValue(row),
                result_set->get_value<common::String*>(3)->to_std_string());
            break;
        case BOOLEAN:
            ASSERT_EQ(row % 192 < 64 ? row % 2 == 0 : row % 192 < 128,
                      result_set->get_value<bool>(3));
            break;
        case INT32:
            ASSERT_EQ(IntValue(row), result_set->get_value<int32_t>(3));
            break;
        case DATE:
            ASSERT_EQ(DateValue(row), result_set->get_value<int32_t>(3));
            break;
        case INT64:
        case TIMESTAMP:
            ASSERT_EQ(LongValue(row), result_set->get_value<int64_t>(3));
            break;
        case FLOAT:
            ASSERT_EQ(FloatBits(ExpectedFloatValue(fixture_case, row)),
                      FloatBits(result_set->get_value<float>(3)));
            break;
        case DOUBLE:
            ASSERT_EQ(DoubleBits(ExpectedDoubleValue(fixture_case, row)),
                      DoubleBits(result_set->get_value<double>(3)));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

void AssertColumnCodec(const std::string& directory,
                       const FixtureCase& fixture_case,
                       const std::string& measurement, TSEncoding encoding) {
    TsFileIOReader io_reader;
    ASSERT_EQ(E_OK,
              io_reader.init(JoinPath(directory, fixture_case.file_name)));

    MetaIndexNode* table_root = nullptr;
    ASSERT_EQ(E_OK, io_reader.get_tsfile_meta()->get_table_metaindex_node(
                        fixture_case.table_name, table_root));
    ASSERT_NE(nullptr, table_root);
    ASSERT_EQ(1U, table_root->children_.size());
    std::shared_ptr<IDeviceID> device_id =
        table_root->children_.front()->get_device_id();
    ASSERT_NE(nullptr, device_id);

    common::PageArena arena;
    arena.init(512, common::MOD_TSFILE_READER);
    std::vector<ITimeseriesIndex*> indexes(1, nullptr);
    std::unordered_set<std::string> measurements = {measurement};
    ASSERT_EQ(E_OK, io_reader.get_timeseries_indexes(device_id, measurements,
                                                     indexes, arena));
    ASSERT_NE(nullptr, indexes.front());
    auto* aligned_index =
        dynamic_cast<AlignedTimeseriesIndex*>(indexes.front());
    ASSERT_NE(nullptr, aligned_index);

    auto* value_chunks = aligned_index->get_value_chunk_meta_list();
    ASSERT_NE(nullptr, value_chunks);
    ASSERT_GT(value_chunks->size(), 0U);

    LocalRandomAccessReadFile read_file;
    ASSERT_EQ(E_OK,
              read_file.open(JoinPath(directory, fixture_case.file_name)));
    for (auto cursor = value_chunks->begin(); cursor != value_chunks->end();
         cursor++) {
        ChunkMeta* chunk = cursor.get();
        ASSERT_NE(nullptr, chunk);
        char header_buf[1024];
        int32_t read_len = 0;
        ASSERT_EQ(E_OK,
                  read_file.read(chunk->offset_of_chunk_header_, header_buf,
                                 sizeof(header_buf), read_len));
        ASSERT_GE(read_len, 7);
        common::ByteStream stream;
        stream.wrap_from(header_buf, read_len);
        ChunkHeader chunk_header;
        ASSERT_EQ(E_OK, chunk_header.deserialize_from(stream));
        ASSERT_EQ(encoding, chunk_header.encoding_type_);
        ASSERT_EQ(fixture_case.compression, chunk_header.compression_type_);
    }
}

void AssertOnWireCodec(const std::string& directory,
                       const FixtureCase& fixture_case) {
    ASSERT_NO_FATAL_FAILURE(AssertColumnCodec(directory, fixture_case,
                                              fixture_case.value_column,
                                              fixture_case.encoding));
    ASSERT_NO_FATAL_FAILURE(AssertColumnCodec(directory, fixture_case,
                                              kBooleanColumn,
                                              fixture_case.boolean_encoding));
}

void ValidateQuery(const std::string& directory,
                   const FixtureCase& fixture_case, bool with_anchor, int start,
                   int end) {
    SCOPED_TRACE(::testing::Message()
                 << fixture_case.ToManifestLine() << " anchor=" << with_anchor
                 << " range=" << start << ":" << end);
    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(JoinPath(directory, fixture_case.file_name)));
    std::vector<std::string> columns = {
        fixture_case.tag_column, fixture_case.value_column, kBooleanColumn};
    if (with_anchor) columns.push_back(kAnchorColumn);
    ResultSet* raw_result_set = nullptr;
    ASSERT_EQ(E_OK, reader.query(fixture_case.table_name, columns, start, end,
                                 raw_result_set));
    ASSERT_NE(nullptr, raw_result_set);
    auto* result_set = static_cast<TableResultSet*>(raw_result_set);
    // Assertion failures must release the query before its reader is destroyed.
    auto cleanup = [&reader](TableResultSet* result) {
        reader.destroy_query_data_set(result);
    };
    std::unique_ptr<TableResultSet, decltype(cleanup)> owned_result(result_set,
                                                                    cleanup);
    std::shared_ptr<ResultSetMetadata> metadata = result_set->get_metadata();
    ASSERT_EQ(with_anchor ? 5U : 4U, metadata->get_column_count());
    ASSERT_EQ("time", metadata->get_column_name(1));
    ASSERT_EQ(INT64, metadata->get_column_type(1));
    ASSERT_EQ(fixture_case.tag_column, metadata->get_column_name(2));
    ASSERT_EQ(STRING, metadata->get_column_type(2));
    ASSERT_EQ(fixture_case.value_column, metadata->get_column_name(3));
    ASSERT_EQ(fixture_case.data_type, metadata->get_column_type(3));

    ASSERT_EQ(kBooleanColumn, metadata->get_column_name(4));
    ASSERT_EQ(BOOLEAN, metadata->get_column_type(4));
    bool has_next = false;
    int row = start;
    while (true) {
        ASSERT_EQ(E_OK, result_set->next(has_next));
        if (!has_next) break;
        SCOPED_TRACE(::testing::Message() << "row=" << row);
        ASSERT_LE(row, end);
        ASSERT_EQ(row, result_set->get_value<int64_t>(1));
        ASSERT_FALSE(result_set->is_null(2));
        ASSERT_EQ(kTagValue,
                  result_set->get_value<common::String*>(2)->to_std_string());
        ASSERT_EQ(IsNull(fixture_case, row), result_set->is_null(3));
        if (!IsNull(fixture_case, row)) {
            ASSERT_NO_FATAL_FAILURE(AssertValue(fixture_case, row, result_set));
        }
        ASSERT_EQ(BooleanNull(row), result_set->is_null(4));
        if (!BooleanNull(row)) {
            ASSERT_EQ(BooleanValue(row), result_set->get_value<bool>(4));
        }
        if (with_anchor) {
            ASSERT_FALSE(result_set->is_null(5));
            ASSERT_EQ(row, result_set->get_value<int64_t>(5));
        }
        ++row;
    }
    ASSERT_EQ(end + 1, row);
    owned_result.reset();
    ASSERT_EQ(E_OK, reader.close());
}

void ValidateFixture(const std::string& directory,
                     const FixtureCase& fixture_case) {
    SCOPED_TRACE(fixture_case.ToManifestLine());
    ASSERT_NO_FATAL_FAILURE(AssertOnWireCodec(directory, fixture_case));
    if (fixture_case.null_pattern == "RANDOM") {
        ASSERT_NO_FATAL_FAILURE(ValidateQuery(directory, fixture_case, true, 0,
                                              fixture_case.row_count - 1));
        ASSERT_NO_FATAL_FAILURE(
            ValidateQuery(directory, fixture_case, false,
                          std::max(0, fixture_case.row_count - 3),
                          fixture_case.row_count - 1));
        return;
    }
    ASSERT_NO_FATAL_FAILURE(ValidateQuery(directory, fixture_case, false, 0,
                                          fixture_case.row_count - 1));
    if (fixture_case.null_pattern != "NONE") {
        ASSERT_NO_FATAL_FAILURE(ValidateQuery(directory, fixture_case, true, 0,
                                              fixture_case.row_count - 1));
        if (fixture_case.row_count > 383) {
            ASSERT_NO_FATAL_FAILURE(
                ValidateQuery(directory, fixture_case, false, 63, 321));
            ASSERT_NO_FATAL_FAILURE(
                ValidateQuery(directory, fixture_case, true, 128, 255));
            ASSERT_NO_FATAL_FAILURE(
                ValidateQuery(directory, fixture_case, true, 320, 383));
        }
    }
}

}  // namespace

class TableModelEncodingCompressionCompatibilityTest : public ::testing::Test {
   protected:
    void SetUp() override { ASSERT_EQ(E_OK, libtsfile_init()); }
    void TearDown() override { libtsfile_destroy(); }
};

TEST_F(TableModelEncodingCompressionCompatibilityTest, GenerateFixtures) {
    const char* output_dir = std::getenv(kGenerateDirEnv);
    if (output_dir == nullptr || std::strlen(output_dir) == 0) {
        GTEST_SKIP() << kGenerateDirEnv << " is not set";
    }
    EnsureDirectory(output_dir);
    std::vector<FixtureCase> cases = BuildMatrix();
    ASSERT_FALSE(cases.empty());
    for (const FixtureCase& fixture_case : cases) {
        WriteFixture(output_dir, fixture_case);
        AssertOnWireCodec(output_dir, fixture_case);
    }
    WriteManifest(output_dir, cases);
}

TEST_F(TableModelEncodingCompressionCompatibilityTest, ValidateFixtures) {
    const char* input_dir = std::getenv(kValidateDirEnv);
    if (input_dir == nullptr || std::strlen(input_dir) == 0) {
        GTEST_SKIP() << kValidateDirEnv << " is not set";
    }
    std::vector<FixtureCase> cases = ReadManifest(input_dir);
    auto expected = CoverageKeys(BuildMatrix());
    ASSERT_FALSE(expected.empty());
    ASSERT_EQ(expected, CoverageKeys(cases)) << "writer/reader matrix coverage";
    for (const FixtureCase& fixture_case : cases) {
        const char* compression_filter =
            std::getenv("TSFILE_COMPAT_COMPRESSION");
        const char* rows_filter = std::getenv("TSFILE_COMPAT_ROWS");
        if ((compression_filter != nullptr &&
             std::string(get_compression_name(fixture_case.compression)) !=
                 compression_filter) ||
            (rows_filter != nullptr &&
             fixture_case.row_count != std::atoi(rows_filter)))
            continue;
        EXPECT_NO_FATAL_FAILURE(ValidateFixture(input_dir, fixture_case));
    }
}
