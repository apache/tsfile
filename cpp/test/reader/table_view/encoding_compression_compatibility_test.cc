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

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
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
#include "common/record.h"
#include "common/row_record.h"
#include "common/schema.h"
#include "common/tablet.h"
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"
#include "encoding/encoder_factory.h"
#include "file/local_random_access_read_file.h"
#include "file/write_file.h"
#include "reader/result_set.h"
#include "reader/table_result_set.h"
#include "reader/tsfile_reader.h"
#include "reader/tsfile_tree_reader.h"
#include "writer/tsfile_table_writer.h"
#include "writer/tsfile_tree_writer.h"

using namespace common;
using namespace storage;

namespace {

const char* const kSpecEnv = "TSFILE_COMPAT_SPEC";
const char* const kGenerateDirEnv = "TSFILE_COMPAT_GENERATE_DIR";
const char* const kValidateDirEnv = "TSFILE_COMPAT_VALIDATE_DIR";
const char* const kShardIndexEnv = "TSFILE_COMPAT_SHARD_INDEX";
const char* const kShardCountEnv = "TSFILE_COMPAT_SHARD_COUNT";

int GetShardIndex() {
    const char* value = std::getenv(kShardIndexEnv);
    return value == nullptr ? 0 : std::atoi(value);
}

int GetShardCount() {
    const char* value = std::getenv(kShardCountEnv);
    return value == nullptr ? 1 : std::atoi(value);
}
const char* const kDefaultSpec = "compat/compatibility-matrix.csv";
const char* const kLanguage = "cpp";

const char* const kManifestFile = "manifest.csv";
const char* const kReportFile = "report.csv";
const char* const kSummaryFile = "summary.md";

const char* const kSpecHeader =
    "id,model,dataType,encoding,compressions,rowCount,pageRows,chunks,devices,"
    "valueProfile,nullPatterns,javaWrite,cppWrite,javaRead,cppRead,gapReason";
const char* const kManifestHeader =
    "producer,id,model,dataType,encoding,compression,rowCount,pageRows,chunks,"
    "devices,valueProfile,nullPattern";

const char* const kTableName = "compat_table";
const char* const kTagColumn = "device";
const char* const kValueColumn = "value";
const char* const kAnchorColumn = "anchor";
const char* const kDevicePrefix = "compat_table_device_";

const char* const kAllCompressions[] = {"UNCOMPRESSED", "SNAPPY", "GZIP",
                                        "LZ4",          "ZSTD",   "LZMA2"};

// Concrete storage types covered by the matrix; see the Java counterpart.
const TSDataType kMatrixDataTypes[] = {BOOLEAN, INT32,  INT64, TIMESTAMP, DATE,
                                       FLOAT,   DOUBLE, TEXT,  STRING,    BLOB};

enum class Model { TABLE, TREE };
enum class NullPattern { NONE, MIXED, ALL };
enum class ValueProfile {
    DEFAULT,
    TS2DIFF,
    TS2DIFF_MPN0,
    SPRINTZ,
    RUNS,
    STRINGS,
    DICT_HIGH,
    BLOB
};

const char* ModelName(Model model) {
    return model == Model::TABLE ? "TABLE" : "TREE";
}

const char* NullPatternName(NullPattern pattern) {
    switch (pattern) {
        case NullPattern::NONE:
            return "NONE";
        case NullPattern::MIXED:
            return "MIXED";
        case NullPattern::ALL:
            return "ALL";
    }
    return "NONE";
}

const char* ProfileName(ValueProfile profile) {
    switch (profile) {
        case ValueProfile::DEFAULT:
            return "DEFAULT";
        case ValueProfile::TS2DIFF:
            return "TS2DIFF";
        case ValueProfile::TS2DIFF_MPN0:
            return "TS2DIFF_MPN0";
        case ValueProfile::SPRINTZ:
            return "SPRINTZ";
        case ValueProfile::RUNS:
            return "RUNS";
        case ValueProfile::STRINGS:
            return "STRINGS";
        case ValueProfile::DICT_HIGH:
            return "DICT_HIGH";
        case ValueProfile::BLOB:
            return "BLOB";
    }
    return "DEFAULT";
}

std::string Upper(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return value;
}

std::string Lower(std::string value) {
    for (char& c : value) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return value;
}

struct FixtureCase {
    std::string spec_id;
    std::string id;
    Model model = Model::TABLE;
    TSDataType data_type = INVALID_DATATYPE;
    TSEncoding encoding = INVALID_ENCODING;
    CompressionType compression = INVALID_COMPRESSION;
    int row_count = 0;
    int page_rows = 0;
    int chunks = 1;
    int devices = 1;
    ValueProfile profile = ValueProfile::DEFAULT;
    NullPattern null_pattern = NullPattern::NONE;
    bool java_write = false;
    bool cpp_write = false;
    bool java_read = false;
    bool cpp_read = false;
    std::string gap_reason;

    bool Writes(const std::string& language) const {
        return language == "java" ? java_write : cpp_write;
    }
    bool Reads(const std::string& language) const {
        return language == "java" ? java_read : cpp_read;
    }
    std::string FileName() const { return id + ".tsfile"; }
    std::string Describe() const {
        return id + " (" + ModelName(model) + "/" +
               get_data_type_name(data_type) + "/" +
               get_encoding_name(encoding) + "/" +
               get_compression_name(compression) + ")";
    }
};

// ---------------------------------------------------------------------------
// Deterministic value profiles, shared with the Java test
// ---------------------------------------------------------------------------

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

// SPRINTZ predicts from first-order differences of the stored integers; a
// block spanning INT_MIN and INT_MAX overflows the predictor, so this profile
// stays inside a safe range. Streaming numeric extremes are covered under the
// other encodings.
const int32_t kSprintzIntValues[] = {
    0,       1,         -1,         7,  -8, 1024, -1024, 123456,
    -654321, 100000000, -100000000, 42, 42, 0,    -8,    2048,
};

const int64_t kSprintzLongValues[] = {
    0LL,
    1LL,
    -1LL,
    7LL,
    -8LL,
    1234567890LL,
    -987654321LL,
    4294967296LL,
    -4294967296LL,
    1000000000000LL,
    -1000000000000LL,
    42LL,
    42LL,
    0LL,
    -8LL,
    2048LL,
};

const uint32_t kSprintzFloatBits[] = {
    0x3f800000U, 0x3fa00000U, 0x3fc00000U, 0x3fe00000U,
    0x40000000U, 0x3fc00000U, 0x3fa00000U, 0x3f800000U,
    0x3fc00000U, 0x3fe00000U, 0x40000000U, 0x3fe00000U,
    0x3fc00000U, 0x3f800000U, 0x3fa00000U, 0x3fc00000U,
};

const uint64_t kSprintzDoubleBits[] = {
    UINT64_C(0x3ff0000000000000), UINT64_C(0x3ff4000000000000),
    UINT64_C(0x3ff8000000000000), UINT64_C(0x3ffc000000000000),
    UINT64_C(0x4000000000000000), UINT64_C(0x3ff8000000000000),
    UINT64_C(0x3ff4000000000000), UINT64_C(0x3ff0000000000000),
    UINT64_C(0x3ff8000000000000), UINT64_C(0x3ffc000000000000),
    UINT64_C(0x4000000000000000), UINT64_C(0x3ffc000000000000),
    UINT64_C(0x3ff8000000000000), UINT64_C(0x3ff0000000000000),
    UINT64_C(0x3ff4000000000000), UINT64_C(0x3ff8000000000000),
};

const int32_t kDateValues[] = {
    19700101, 19991231, 20000229, 20240229,
    20380119, 20500615, 19690720, 19800106,
};

std::string Repeat(char c, size_t count) { return std::string(count, c); }

std::vector<std::string> MakeStringValues() {
    std::vector<std::string> values;
    values.emplace_back("");
    values.emplace_back("hello");
    values.emplace_back(
        "\xE4\xBD\xA0\xE5\xA5\xBD\xEF\xBC\x8C\xE4\xB8\x96\xE7\x95\x8C");
    values.emplace_back(std::string("nul\0inside", 10));
    values.emplace_back(
        "\xC3\x84\xC3\x96\xC3\x9C \xC3\x9F "
        "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E");
    values.emplace_back("value");
    values.emplace_back("value");
    values.emplace_back(Repeat('a', 1024));
    values.emplace_back("\xF0\x9F\x98\x80 emoji \xF0\x9F\x8C\x8D");
    return values;
}

const std::vector<std::string>& StringValues() {
    static const std::vector<std::string> values = MakeStringValues();
    return values;
}

std::vector<std::string> MakeBlobValues() {
    std::vector<std::string> values;
    values.emplace_back("");
    values.emplace_back(1, '\0');
    values.emplace_back(4, '\0');
    values.emplace_back("\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0a", 10);
    values.emplace_back("blob-as-text");
    values.emplace_back("\xff\xfe\x80\x00\x7f", 5);
    values.emplace_back("");
    values.emplace_back("blob-as-text");
    return values;
}

const std::vector<std::string>& BlobValues() {
    static const std::vector<std::string> values = MakeBlobValues();
    return values;
}

template <size_t N>
int32_t IntAt(const int32_t (&values)[N], int row) {
    return values[static_cast<size_t>(row) % N];
}

// Mirrors Java Math.round(x) == floor(x + 0.5).
double JavaRound(double x) {
    return static_cast<double>(java_round_to_long(x));
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

template <size_t N>
uint32_t FloatBitsAt(const uint32_t (&values)[N], int row) {
    return values[static_cast<size_t>(row) % N];
}

template <size_t N>
uint64_t DoubleBitsAt(const uint64_t (&values)[N], int row) {
    return values[static_cast<size_t>(row) % N];
}

bool IsFloatEncoderCase(const FixtureCase& fixture_case) {
    return (fixture_case.encoding == TS_2DIFF ||
            fixture_case.encoding == RLE) &&
           (fixture_case.data_type == FLOAT ||
            fixture_case.data_type == DOUBLE);
}

double MaxPointValue(const FixtureCase& fixture_case) {
    return fixture_case.profile == ValueProfile::TS2DIFF_MPN0 ? 1.0 : 100.0;
}

bool BooleanValue(int row) {
    return row % 192 < 64 ? row % 2 == 0 : row % 192 < 128;
}

int32_t IntValue(const FixtureCase& fixture_case, int row) {
    if (fixture_case.profile == ValueProfile::RUNS) {
        return (row / 256) % 4 - 1;
    }
    if (fixture_case.profile == ValueProfile::SPRINTZ) {
        return IntAt(kSprintzIntValues, row);
    }
    return IntAt(kIntValues, row);
}

int64_t LongValue(const FixtureCase& fixture_case, int row) {
    if (fixture_case.profile == ValueProfile::RUNS) {
        return static_cast<int64_t>((row / 256) % 4 - 1) * 1000000000LL;
    }
    if (fixture_case.profile == ValueProfile::SPRINTZ) {
        const size_t count =
            sizeof(kSprintzLongValues) / sizeof(kSprintzLongValues[0]);
        return kSprintzLongValues[static_cast<size_t>(row) % count];
    }
    const size_t count = sizeof(kLongValues) / sizeof(kLongValues[0]);
    return kLongValues[static_cast<size_t>(row) % count];
}

int32_t DateValue(const FixtureCase& fixture_case, int row) {
    const size_t count = sizeof(kDateValues) / sizeof(kDateValues[0]);
    if (fixture_case.profile == ValueProfile::RUNS) {
        return kDateValues[static_cast<size_t>(row / 256) % count];
    }
    return kDateValues[static_cast<size_t>(row) % count];
}

float FloatValue(const FixtureCase& fixture_case, int row) {
    switch (fixture_case.profile) {
        case ValueProfile::TS2DIFF:
        case ValueProfile::TS2DIFF_MPN0:
            return BitsToFloat(FloatBitsAt(kTs2DiffFloatBits, row));
        case ValueProfile::RUNS:
            return static_cast<float>((row / 256) % 4 - 1);
        case ValueProfile::SPRINTZ:
            return BitsToFloat(FloatBitsAt(kSprintzFloatBits, row));
        default:
            return BitsToFloat(FloatBitsAt(kFloatBits, row));
    }
}

double DoubleValue(const FixtureCase& fixture_case, int row) {
    switch (fixture_case.profile) {
        case ValueProfile::TS2DIFF:
        case ValueProfile::TS2DIFF_MPN0:
            return BitsToDouble(DoubleBitsAt(kTs2DiffDoubleBits, row));
        case ValueProfile::RUNS:
            return static_cast<double>((row / 256) % 4 - 1);
        case ValueProfile::SPRINTZ:
            return BitsToDouble(DoubleBitsAt(kSprintzDoubleBits, row));
        default:
            return BitsToDouble(DoubleBitsAt(kDoubleBits, row));
    }
}

std::string StringValue(const FixtureCase& fixture_case, int row) {
    if (fixture_case.profile == ValueProfile::DICT_HIGH) {
        return "key_" + std::to_string(row);
    }
    const std::vector<std::string>& values = StringValues();
    return values[static_cast<size_t>(row) % values.size()];
}

const std::string& BlobValue(const FixtureCase& fixture_case, int row) {
    (void)fixture_case;
    const std::vector<std::string>& values = BlobValues();
    return values[static_cast<size_t>(row) % values.size()];
}

float ExpectedFloatValue(const FixtureCase& fixture_case, int row) {
    if (!IsFloatEncoderCase(fixture_case)) {
        return FloatValue(fixture_case, row);
    }
    float value = FloatValue(fixture_case, row);
    if (std::isnan(value)) {
        return BitsToFloat(0x7fc00000U);
    }
    const double mpv = MaxPointValue(fixture_case);
    const double scaled = static_cast<double>(value) * mpv;
    const bool scaled_overflow =
        scaled > static_cast<double>(std::numeric_limits<int32_t>::max()) ||
        scaled < static_cast<double>(std::numeric_limits<int32_t>::min());
    if (scaled_overflow) {
        const bool value_overflows =
            value > static_cast<float>(std::numeric_limits<int32_t>::max()) ||
            value < static_cast<float>(std::numeric_limits<int32_t>::min());
        if (value_overflows) {
            return value;
        }
        return static_cast<float>(JavaRound(value)) / 1.0f;
    }
    return static_cast<float>(JavaRound(scaled) / mpv);
}

double ExpectedDoubleValue(const FixtureCase& fixture_case, int row) {
    if (!IsFloatEncoderCase(fixture_case)) {
        return DoubleValue(fixture_case, row);
    }
    double value = DoubleValue(fixture_case, row);
    if (std::isnan(value)) {
        return BitsToDouble(UINT64_C(0x7ff8000000000000));
    }
    const double mpv = MaxPointValue(fixture_case);
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

// ---------------------------------------------------------------------------
// Spec parsing
// ---------------------------------------------------------------------------

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

bool ParseModel(const std::string& value, Model& out) {
    std::string upper = Upper(value);
    if (upper == "TABLE") {
        out = Model::TABLE;
        return true;
    }
    if (upper == "TREE") {
        out = Model::TREE;
        return true;
    }
    return false;
}

bool ParseNullPattern(const std::string& value, NullPattern& out) {
    std::string upper = Upper(value);
    if (upper == "NONE") {
        out = NullPattern::NONE;
        return true;
    }
    if (upper == "MIXED") {
        out = NullPattern::MIXED;
        return true;
    }
    if (upper == "ALL") {
        out = NullPattern::ALL;
        return true;
    }
    return false;
}

bool ParseProfile(const std::string& value, ValueProfile& out) {
    std::string upper = Upper(value);
    static const ValueProfile kProfiles[] = {
        ValueProfile::DEFAULT,      ValueProfile::TS2DIFF,
        ValueProfile::TS2DIFF_MPN0, ValueProfile::SPRINTZ,
        ValueProfile::RUNS,         ValueProfile::STRINGS,
        ValueProfile::DICT_HIGH,    ValueProfile::BLOB,
    };
    for (ValueProfile profile : kProfiles) {
        if (upper == ProfileName(profile)) {
            out = profile;
            return true;
        }
    }
    return false;
}

bool ParseYesNo(const std::string& value, bool& out) {
    if (value == "yes") {
        out = true;
        return true;
    }
    if (value == "no") {
        out = false;
        return true;
    }
    return false;
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

std::string ResolveSpecPath() {
    const char* configured = std::getenv(kSpecEnv);
    if (configured != nullptr && std::strlen(configured) > 0) {
        return configured;
    }
    std::string directory = ".";
    for (int depth = 0; depth < 8; depth++) {
        std::string candidate = JoinPath(directory, kDefaultSpec);
        std::ifstream input(candidate.c_str());
        if (input.good()) {
            return candidate;
        }
        directory = JoinPath(directory, "..");
    }
    ADD_FAILURE() << "Cannot locate " << kDefaultSpec;
    return kDefaultSpec;
}

int ColumnIndex(const std::map<std::string, int>& columns,
                const std::string& name) {
    std::map<std::string, int>::const_iterator it = columns.find(name);
    if (it == columns.end()) {
        ADD_FAILURE() << "Missing column " << name;
        return -1;
    }
    return it->second;
}

std::map<std::string, int> IndexColumns(const std::string& header,
                                        const std::string& expected,
                                        const std::string& path) {
    EXPECT_EQ(expected, header) << "Unexpected header in " << path;
    std::map<std::string, int> columns;
    std::vector<std::string> names = Split(header, ',');
    for (size_t i = 0; i < names.size(); i++) {
        columns[names[i]] = static_cast<int>(i);
    }
    return columns;
}

std::vector<FixtureCase> ReadSpec(const std::string& path) {
    std::vector<FixtureCase> cases;
    std::ifstream input(path.c_str());
    if (!input.good()) {
        ADD_FAILURE() << "Cannot read compatibility spec " << path;
        return cases;
    }
    std::map<std::string, int> columns;
    bool header_seen = false;
    std::string line;
    std::unordered_set<std::string> ids;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty() || line[0] == '#') {
            continue;
        }
        std::vector<std::string> parts = Split(line, ',');
        if (!header_seen) {
            columns = IndexColumns(line, kSpecHeader, path);
            header_seen = true;
            continue;
        }
        if (columns.empty() || parts.size() != columns.size()) {
            ADD_FAILURE() << "Bad spec line: " << line;
            continue;
        }
        const std::string spec_id = parts[ColumnIndex(columns, "id")];
        std::string model_name = parts[ColumnIndex(columns, "model")];
        std::string data_type_name = parts[ColumnIndex(columns, "dataType")];
        std::string encoding_name = parts[ColumnIndex(columns, "encoding")];
        std::string compressions = parts[ColumnIndex(columns, "compressions")];
        int row_count =
            std::atoi(parts[ColumnIndex(columns, "rowCount")].c_str());
        int page_rows =
            std::atoi(parts[ColumnIndex(columns, "pageRows")].c_str());
        int chunks = std::atoi(parts[ColumnIndex(columns, "chunks")].c_str());
        int devices = std::atoi(parts[ColumnIndex(columns, "devices")].c_str());
        std::string profile_name = parts[ColumnIndex(columns, "valueProfile")];
        std::string patterns = parts[ColumnIndex(columns, "nullPatterns")];
        bool java_write = false;
        bool cpp_write = false;
        bool java_read = false;
        bool cpp_read = false;
        if (!ParseYesNo(parts[ColumnIndex(columns, "javaWrite")], java_write) ||
            !ParseYesNo(parts[ColumnIndex(columns, "cppWrite")], cpp_write) ||
            !ParseYesNo(parts[ColumnIndex(columns, "javaRead")], java_read) ||
            !ParseYesNo(parts[ColumnIndex(columns, "cppRead")], cpp_read)) {
            ADD_FAILURE() << "Bad yes/no in spec line: " << line;
            continue;
        }
        std::string gap_reason = parts[ColumnIndex(columns, "gapReason")];

        Model model;
        TSDataType data_type;
        TSEncoding encoding;
        ValueProfile profile;
        if (!ParseModel(model_name, model) ||
            !parse_data_type_name(data_type_name, data_type) ||
            !ParseEncodingName(encoding_name, encoding) ||
            !ParseProfile(profile_name, profile)) {
            ADD_FAILURE() << "Bad enum value in spec line: " << line;
            continue;
        }

        std::vector<std::string> compression_list;
        if (compressions == "ALL") {
            compression_list.assign(std::begin(kAllCompressions),
                                    std::end(kAllCompressions));
        } else {
            compression_list = Split(compressions, '+');
        }
        std::vector<std::string> pattern_list = Split(patterns, '|');
        for (const std::string& compression_name : compression_list) {
            CompressionType compression;
            if (!ParseCompressionName(compression_name, compression)) {
                ADD_FAILURE() << "Bad compression in spec line: " << line;
                continue;
            }
            for (const std::string& pattern_name : pattern_list) {
                NullPattern pattern;
                if (!ParseNullPattern(pattern_name, pattern)) {
                    ADD_FAILURE() << "Bad null pattern in spec line: " << line;
                    continue;
                }
                FixtureCase fixture_case;
                fixture_case.spec_id = spec_id;
                fixture_case.id = spec_id + "." + Lower(compression_name) +
                                  "." + Lower(pattern_name);
                if (!ids.insert(fixture_case.id).second) {
                    ADD_FAILURE()
                        << "Duplicate expanded case id " << fixture_case.id;
                }
                fixture_case.model = model;
                fixture_case.data_type = data_type;
                fixture_case.encoding = encoding;
                fixture_case.compression = compression;
                fixture_case.row_count = row_count;
                fixture_case.page_rows = page_rows;
                fixture_case.chunks = chunks;
                fixture_case.devices = devices;
                fixture_case.profile = profile;
                fixture_case.null_pattern = pattern;
                fixture_case.java_write = java_write;
                fixture_case.cpp_write = cpp_write;
                fixture_case.java_read = java_read;
                fixture_case.cpp_read = cpp_read;
                fixture_case.gap_reason = gap_reason;
                cases.push_back(fixture_case);
            }
        }
    }
    if (!header_seen) {
        ADD_FAILURE() << "Empty compatibility spec " << path;
    }
    return cases;
}

// ---------------------------------------------------------------------------
// Manifest and reports
// ---------------------------------------------------------------------------

struct ManifestEntry {
    std::string producer;
    FixtureCase fixture_case;
};

void ExpectSameCase(const FixtureCase& expected, const FixtureCase& produced) {
    SCOPED_TRACE("manifest case " + produced.id);
    EXPECT_EQ(expected.id, produced.id);
    EXPECT_EQ(static_cast<int>(expected.model),
              static_cast<int>(produced.model));
    EXPECT_EQ(expected.data_type, produced.data_type);
    EXPECT_EQ(expected.encoding, produced.encoding);
    EXPECT_EQ(expected.compression, produced.compression);
    EXPECT_EQ(expected.row_count, produced.row_count);
    EXPECT_EQ(expected.page_rows, produced.page_rows);
    EXPECT_EQ(expected.chunks, produced.chunks);
    EXPECT_EQ(expected.devices, produced.devices);
    EXPECT_EQ(static_cast<int>(expected.profile),
              static_cast<int>(produced.profile));
    EXPECT_EQ(static_cast<int>(expected.null_pattern),
              static_cast<int>(produced.null_pattern));
}

void WriteManifest(const std::string& directory,
                   const std::vector<FixtureCase>& cases) {
    std::ofstream out(JoinPath(directory, kManifestFile).c_str(),
                      std::ios::out | std::ios::trunc);
    ASSERT_TRUE(out.good());
    out << kManifestHeader << "\n";
    for (const FixtureCase& fixture_case : cases) {
        out << kLanguage << "," << fixture_case.id << ","
            << ModelName(fixture_case.model) << ","
            << get_data_type_name(fixture_case.data_type) << ","
            << get_encoding_name(fixture_case.encoding) << ","
            << get_compression_name(fixture_case.compression) << ","
            << fixture_case.row_count << "," << fixture_case.page_rows << ","
            << fixture_case.chunks << "," << fixture_case.devices << ","
            << ProfileName(fixture_case.profile) << ","
            << NullPatternName(fixture_case.null_pattern) << "\n";
    }
}

std::vector<ManifestEntry> ReadManifest(const std::string& directory) {
    std::vector<ManifestEntry> entries;
    std::ifstream input(JoinPath(directory, kManifestFile).c_str());
    if (!input.good()) {
        ADD_FAILURE() << "Missing manifest in " << directory;
        return entries;
    }
    std::map<std::string, int> columns;
    bool header_seen = false;
    std::string line;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        if (!header_seen) {
            columns = IndexColumns(line, kManifestHeader,
                                   JoinPath(directory, kManifestFile));
            header_seen = true;
            continue;
        }
        std::vector<std::string> parts = Split(line, ',');
        if (columns.empty() || parts.size() != columns.size()) {
            ADD_FAILURE() << "Bad manifest line: " << line;
            continue;
        }
        ManifestEntry entry;
        entry.producer = parts[ColumnIndex(columns, "producer")];
        FixtureCase& fixture_case = entry.fixture_case;
        fixture_case.id = parts[ColumnIndex(columns, "id")];
        if (!ParseModel(parts[ColumnIndex(columns, "model")],
                        fixture_case.model)) {
            ADD_FAILURE() << "Bad model in manifest line: " << line;
        }
        if (!parse_data_type_name(parts[ColumnIndex(columns, "dataType")],
                                  fixture_case.data_type)) {
            ADD_FAILURE() << "Bad data type in manifest line: " << line;
        }
        if (!ParseEncodingName(parts[ColumnIndex(columns, "encoding")],
                               fixture_case.encoding)) {
            ADD_FAILURE() << "Bad encoding in manifest line: " << line;
        }
        if (!ParseCompressionName(parts[ColumnIndex(columns, "compression")],
                                  fixture_case.compression)) {
            ADD_FAILURE() << "Bad compression in manifest line: " << line;
        }
        fixture_case.row_count =
            std::atoi(parts[ColumnIndex(columns, "rowCount")].c_str());
        fixture_case.page_rows =
            std::atoi(parts[ColumnIndex(columns, "pageRows")].c_str());
        fixture_case.chunks =
            std::atoi(parts[ColumnIndex(columns, "chunks")].c_str());
        fixture_case.devices =
            std::atoi(parts[ColumnIndex(columns, "devices")].c_str());
        if (!ParseProfile(parts[ColumnIndex(columns, "valueProfile")],
                          fixture_case.profile)) {
            ADD_FAILURE() << "Bad value profile in manifest line: " << line;
        }
        if (!ParseNullPattern(parts[ColumnIndex(columns, "nullPattern")],
                              fixture_case.null_pattern)) {
            ADD_FAILURE() << "Bad null pattern in manifest line: " << line;
        }
        entries.push_back(entry);
    }
    return entries;
}

struct ReportRow {
    std::string id;
    std::string status;
    std::string detail;
};

void WriteReport(const std::string& directory, const std::string& name,
                 const std::vector<ReportRow>& report) {
    std::ofstream out(JoinPath(directory, name).c_str(),
                      std::ios::out | std::ios::trunc);
    ASSERT_TRUE(out.good());
    out << "id,status,detail\n";
    for (const ReportRow& row : report) {
        out << row.id << "," << row.status << "," << row.detail << "\n";
    }
}

std::string FormatCounts(const std::map<std::string, int>& counts) {
    std::ostringstream out;
    bool first = true;
    for (std::map<std::string, int>::const_iterator it = counts.begin();
         it != counts.end(); ++it) {
        if (!first) {
            out << ", ";
        }
        first = false;
        out << it->first << "=" << it->second;
    }
    return out.str();
}

void WriteSummary(const std::string& directory, const std::string& name,
                  const std::string& title,
                  const std::vector<FixtureCase>& cases,
                  const std::vector<ReportRow>& report) {
    int generated = 0;
    int validated = 0;
    int skipped = 0;
    std::map<std::string, int> by_status;
    std::map<std::string, int> by_model;
    std::map<std::string, int> by_type;
    std::vector<std::string> gaps;
    for (const ReportRow& row : report) {
        by_status[row.status]++;
        if (row.status.find("generated") == 0 || row.status == "written") {
            generated++;
        }
        if (row.status.find("validated") != std::string::npos) {
            validated++;
        }
        if (row.status.find("skipped") == 0) {
            skipped++;
            gaps.push_back(row.id + ": " + row.detail);
        } else if (row.status == "not-mine") {
            // A writer-side gap this language does not produce; list its
            // reason without counting it as a skipped reader validation.
            gaps.push_back(row.id + ": " + row.detail);
        }
    }
    for (const FixtureCase& fixture_case : cases) {
        by_model[ModelName(fixture_case.model)]++;
        by_type[get_data_type_name(fixture_case.data_type)]++;
    }

    std::ostringstream out;
    out << "### " << title << "\n\n";
    out << "- Spec cases: " << cases.size() << " (" << FormatCounts(by_model)
        << ")\n";
    out << "- Status: " << FormatCounts(by_status) << "\n";
    out << "- Generated: " << generated << ", validated: " << validated
        << ", skipped: " << skipped << "\n";
    out << "- Types: " << FormatCounts(by_type) << "\n";
    if (!gaps.empty()) {
        out << "\nDeclared/observed gaps:\n\n";
        for (const std::string& gap : gaps) {
            out << "- " << gap << "\n";
        }
    }
    std::ofstream file(JoinPath(directory, name).c_str(),
                       std::ios::out | std::ios::trunc);
    if (file.good()) {
        file << out.str();
    }
    std::cout << out.str() << std::flush;
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

// ---------------------------------------------------------------------------
// Fixture writing
// ---------------------------------------------------------------------------

bool IsNull(const FixtureCase& fixture_case, int row) {
    if (fixture_case.null_pattern == NullPattern::ALL) {
        return true;
    }
    if (fixture_case.null_pattern == NullPattern::NONE) {
        return false;
    }
    int page_rows = fixture_case.page_rows > 0 ? fixture_case.page_rows : 64;
    int page = row / page_rows;
    return page == 0 || page == 2 || page == 3 || page >= 6 ||
           (page == 4 && row % 2 == 0);
}

// Restore process-wide settings even when a fatal assertion returns early.
class ScopedPageConfig {
   public:
    explicit ScopedPageConfig(const FixtureCase& fixture_case)
        : old_rows_(g_config_value_.page_writer_max_point_num_),
          old_bytes_(g_config_value_.page_writer_max_memory_bytes_) {
        if (fixture_case.page_rows > 0) {
            g_config_value_.page_writer_max_point_num_ =
                static_cast<uint32_t>(fixture_case.page_rows);
            g_config_value_.page_writer_max_memory_bytes_ = 16 * 1024 * 1024;
        }
    }
    ~ScopedPageConfig() {
        g_config_value_.page_writer_max_point_num_ = old_rows_;
        g_config_value_.page_writer_max_memory_bytes_ = old_bytes_;
    }

   private:
    uint32_t old_rows_;
    uint32_t old_bytes_;
};

std::vector<std::pair<int, int>> Segments(const FixtureCase& fixture_case) {
    int chunks = std::max(1, fixture_case.chunks);
    std::vector<std::pair<int, int>> segments;
    int base = fixture_case.row_count / chunks;
    int extra = fixture_case.row_count % chunks;
    int start = 0;
    for (int i = 0; i < chunks; i++) {
        int length = base + (i < extra ? 1 : 0);
        segments.emplace_back(start, start + length);
        start += length;
    }
    return segments;
}

std::string DeviceId(int device) {
    return std::string(kDevicePrefix) + std::to_string(device);
}

TableSchema* CreateTableSchema(const FixtureCase& fixture_case) {
    std::vector<MeasurementSchema*> measurement_schemas;
    std::vector<ColumnCategory> column_categories;
    measurement_schemas.emplace_back(
        new MeasurementSchema(kTagColumn, STRING, PLAIN, UNCOMPRESSED));
    column_categories.emplace_back(ColumnCategory::TAG);
    measurement_schemas.emplace_back(
        new MeasurementSchema(kValueColumn, fixture_case.data_type,
                              fixture_case.encoding, fixture_case.compression));
    column_categories.emplace_back(ColumnCategory::FIELD);
    if (fixture_case.null_pattern != NullPattern::NONE) {
        measurement_schemas.emplace_back(new MeasurementSchema(
            kAnchorColumn, INT64, PLAIN, fixture_case.compression));
        column_categories.emplace_back(ColumnCategory::FIELD);
    }
    return new TableSchema(kTableName, measurement_schemas, column_categories);
}

void AddTabletValue(Tablet& tablet, const FixtureCase& fixture_case,
                    uint32_t index, int row) {
    switch (fixture_case.data_type) {
        case BOOLEAN:
            EXPECT_EQ(E_OK,
                      tablet.add_value(index, kValueColumn, BooleanValue(row)));
            break;
        case INT32:
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             IntValue(fixture_case, row)));
            break;
        case DATE:
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             DateValue(fixture_case, row)));
            break;
        case INT64:
        case TIMESTAMP:
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             LongValue(fixture_case, row)));
            break;
        case FLOAT:
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             FloatValue(fixture_case, row)));
            break;
        case DOUBLE:
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             DoubleValue(fixture_case, row)));
            break;
        case TEXT:
        case STRING: {
            std::string value = StringValue(fixture_case, row);
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             common::String(value)));
            break;
        }
        case BLOB: {
            const std::string& value = BlobValue(fixture_case, row);
            EXPECT_EQ(E_OK, tablet.add_value(index, kValueColumn,
                                             common::String(value)));
            break;
        }
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

Tablet CreateTablet(TableSchema* table_schema, const FixtureCase& fixture_case,
                    int device, int start, int end) {
    Tablet tablet(
        table_schema->get_table_name(), table_schema->get_measurement_names(),
        table_schema->get_data_types(), table_schema->get_column_categories(),
        static_cast<uint32_t>(std::max(1, end - start)));
    uint32_t index = 0;
    for (int row = start; row < end; row++, index++) {
        EXPECT_EQ(E_OK, tablet.add_timestamp(index, row));
        EXPECT_EQ(E_OK, tablet.add_value(index, kTagColumn,
                                         common::String(DeviceId(device))));
        if (!IsNull(fixture_case, row)) {
            AddTabletValue(tablet, fixture_case, index, row);
        }
        if (fixture_case.null_pattern != NullPattern::NONE) {
            EXPECT_EQ(E_OK, tablet.add_value(index, kAnchorColumn,
                                             static_cast<int64_t>(row)));
        }
    }
    return tablet;
}

void CreateWriteFile(const std::string& path, WriteFile& write_file) {
    remove(path.c_str());
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    ASSERT_EQ(E_OK, write_file.create(path, flags, 0666));
}

// The tree write path stores a borrowed pointer to the value bytes, so the
// caller keeps the string alive through writer.write().
void AddRecordPoint(TsRecord& record, std::string& keep_alive,
                    const FixtureCase& fixture_case, int row) {
    switch (fixture_case.data_type) {
        case BOOLEAN:
            EXPECT_EQ(E_OK, record.add_point(kValueColumn, BooleanValue(row)));
            break;
        case INT32:
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             IntValue(fixture_case, row)));
            break;
        case DATE:
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             DateValue(fixture_case, row)));
            break;
        case INT64:
        case TIMESTAMP:
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             LongValue(fixture_case, row)));
            break;
        case FLOAT:
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             FloatValue(fixture_case, row)));
            break;
        case DOUBLE:
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             DoubleValue(fixture_case, row)));
            break;
        case TEXT:
        case STRING:
            keep_alive = StringValue(fixture_case, row);
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             common::String(keep_alive)));
            break;
        case BLOB:
            keep_alive = BlobValue(fixture_case, row);
            EXPECT_EQ(E_OK, record.add_point(kValueColumn,
                                             common::String(keep_alive)));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

void WriteTableFixture(const std::string& directory,
                       const FixtureCase& fixture_case) {
    ScopedPageConfig page_config(fixture_case);
    std::string path = JoinPath(directory, fixture_case.FileName());
    WriteFile write_file;
    CreateWriteFile(path, write_file);
    TableSchema* table_schema = CreateTableSchema(fixture_case);
    TsFileTableWriter writer(&write_file, table_schema);
    for (int device = 0; device < fixture_case.devices; device++) {
        for (const std::pair<int, int>& segment : Segments(fixture_case)) {
            Tablet tablet = CreateTablet(table_schema, fixture_case, device,
                                         segment.first, segment.second);
            ASSERT_EQ(E_OK, writer.write_table(tablet));
            ASSERT_EQ(E_OK, writer.flush());
        }
    }
    ASSERT_EQ(E_OK, writer.close());
    delete table_schema;
}

void WriteTreeFixture(const std::string& directory,
                      const FixtureCase& fixture_case) {
    ScopedPageConfig page_config(fixture_case);
    std::string path = JoinPath(directory, fixture_case.FileName());
    WriteFile write_file;
    CreateWriteFile(path, write_file);
    TsFileTreeWriter writer(&write_file);
    for (int device = 0; device < fixture_case.devices; device++) {
        std::string device_id = DeviceId(device);
        MeasurementSchema* value_schema = new MeasurementSchema(
            kValueColumn, fixture_case.data_type, fixture_case.encoding,
            fixture_case.compression);
        ASSERT_EQ(E_OK, writer.register_timeseries(device_id, value_schema));
        delete value_schema;
        if (fixture_case.null_pattern != NullPattern::NONE) {
            MeasurementSchema* anchor_schema = new MeasurementSchema(
                kAnchorColumn, INT64, PLAIN, fixture_case.compression);
            ASSERT_EQ(E_OK,
                      writer.register_timeseries(device_id, anchor_schema));
            delete anchor_schema;
        }
    }
    for (int device = 0; device < fixture_case.devices; device++) {
        std::string device_id = DeviceId(device);
        for (const std::pair<int, int>& segment : Segments(fixture_case)) {
            for (int row = segment.first; row < segment.second; row++) {
                TsRecord record(device_id, row);
                std::string keep_alive;
                int points = 0;
                if (!IsNull(fixture_case, row)) {
                    AddRecordPoint(record, keep_alive, fixture_case, row);
                    points++;
                }
                if (fixture_case.null_pattern != NullPattern::NONE) {
                    EXPECT_EQ(E_OK,
                              record.add_point(kAnchorColumn,
                                               static_cast<int64_t>(row)));
                    points++;
                }
                if (points > 0) {
                    ASSERT_EQ(E_OK, writer.write(record));
                }
            }
            ASSERT_EQ(E_OK, writer.flush());
        }
    }
    ASSERT_EQ(E_OK, writer.close());
}

void AssertOnWireCodec(const std::string& directory,
                       const FixtureCase& fixture_case) {
    // Prove that the declared encoding and compression are what the file
    // actually carries. Reading values back cannot tell a PLAIN column from an
    // RLE column that happens to decode identically, nor whether a codec
    // request was silently replaced by a default.
    std::string path = JoinPath(directory, fixture_case.FileName());
    TsFileIOReader io_reader;
    ASSERT_EQ(E_OK, io_reader.init(path));
    TsFileMeta* tsfile_meta = io_reader.get_tsfile_meta();
    ASSERT_NE(nullptr, tsfile_meta);

    std::vector<std::shared_ptr<IDeviceID>> devices;
    if (fixture_case.model == Model::TABLE) {
        MetaIndexNode* table_root = nullptr;
        ASSERT_EQ(E_OK, tsfile_meta->get_table_metaindex_node(kTableName,
                                                              table_root));
        ASSERT_NE(nullptr, table_root);
        for (const std::shared_ptr<IMetaIndexEntry>& entry :
             table_root->children_) {
            std::shared_ptr<IDeviceID> device_id = entry->get_device_id();
            if (device_id != nullptr) {
                devices.push_back(device_id);
            }
        }
    } else {
        for (int device = 0; device < fixture_case.devices; device++) {
            devices.push_back(
                std::make_shared<StringArrayDeviceID>(DeviceId(device)));
        }
    }
    ASSERT_EQ(static_cast<size_t>(fixture_case.devices), devices.size());

    LocalRandomAccessReadFile read_file;
    ASSERT_EQ(E_OK, read_file.open(path));
    common::PageArena arena;
    arena.init(512, common::MOD_TSFILE_READER);
    for (const std::shared_ptr<IDeviceID>& device_id : devices) {
        std::vector<ITimeseriesIndex*> indexes(1, nullptr);
        std::unordered_set<std::string> measurements = {kValueColumn};
        ASSERT_EQ(E_OK, io_reader.get_timeseries_indexes(
                            device_id, measurements, indexes, arena));
        ASSERT_NE(nullptr, indexes.front());
        common::SimpleList<ChunkMeta*>* chunks =
            indexes.front()->get_value_chunk_meta_list();
        if (chunks == nullptr) {
            chunks = indexes.front()->get_chunk_meta_list();
        }
        ASSERT_NE(nullptr, chunks);
        ASSERT_EQ(static_cast<size_t>(fixture_case.chunks), chunks->size());
        for (auto cursor = chunks->begin(); cursor != chunks->end(); cursor++) {
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
            EXPECT_EQ(fixture_case.encoding, chunk_header.encoding_type_)
                << fixture_case.Describe() << " encoding on wire";
            EXPECT_EQ(fixture_case.compression, chunk_header.compression_type_)
                << fixture_case.Describe() << " compression on wire";
        }
    }
}

void WriteFixture(const std::string& directory,
                  const FixtureCase& fixture_case) {
    if (fixture_case.model == Model::TABLE) {
        WriteTableFixture(directory, fixture_case);
    } else {
        WriteTreeFixture(directory, fixture_case);
    }
    std::string path = JoinPath(directory, fixture_case.FileName());
    std::ifstream input(path.c_str(), std::ios::binary | std::ios::ate);
    ASSERT_TRUE(input.good()) << "fixture was not created: " << path;
    ASSERT_GT(input.tellg(), 0) << "fixture is empty: " << path;
    AssertOnWireCodec(directory, fixture_case);
}

// ---------------------------------------------------------------------------
// Fixture validation
// ---------------------------------------------------------------------------

std::string StringOf(const common::String* value) {
    if (value == nullptr || value->buf_ == nullptr || value->len_ == 0) {
        return std::string();
    }
    return std::string(value->buf_, value->len_);
}

// Verifies one value cell of a TableResultSet against the fixture's expected
// value.  Primitive types use the direct buffer accessors (get_*_at), which
// read straight from the column vector without materializing a RowRecord/Field
// or doing a column-name lookup per row; only the variable-length string/binary
// columns fall back to get_value().
void ExpectTableValue(const FixtureCase& fixture_case, int row,
                      ResultSet* result_set) {
    SCOPED_TRACE(::testing::Message() << fixture_case.id << " row=" << row);
    switch (fixture_case.data_type) {
        case BOOLEAN:
            EXPECT_EQ(BooleanValue(row), result_set->get_bool_at(3));
            break;
        case INT32:
            EXPECT_EQ(IntValue(fixture_case, row), result_set->get_int32_at(3));
            break;
        case DATE:
            EXPECT_EQ(DateValue(fixture_case, row),
                      result_set->get_int32_at(3));
            break;
        case INT64:
        case TIMESTAMP:
            EXPECT_EQ(LongValue(fixture_case, row),
                      result_set->get_int64_at(3));
            break;
        case FLOAT:
            EXPECT_EQ(FloatBits(ExpectedFloatValue(fixture_case, row)),
                      FloatBits(result_set->get_float_at(3)));
            break;
        case DOUBLE:
            EXPECT_EQ(DoubleBits(ExpectedDoubleValue(fixture_case, row)),
                      DoubleBits(result_set->get_double_at(3)));
            break;
        case TEXT:
        case STRING:
            EXPECT_EQ(StringValue(fixture_case, row),
                      StringOf(result_set->get_value<common::String*>(3)));
            break;
        case BLOB:
            EXPECT_EQ(BlobValue(fixture_case, row),
                      StringOf(result_set->get_value<common::String*>(3)));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

void ExpectFieldValue(const FixtureCase& fixture_case, int row, Field* field) {
    SCOPED_TRACE(::testing::Message() << fixture_case.id << " row=" << row);
    EXPECT_EQ(fixture_case.data_type, field->type_);
    switch (fixture_case.data_type) {
        case BOOLEAN:
            EXPECT_EQ(BooleanValue(row), field->get_value<bool>());
            break;
        case INT32:
            EXPECT_EQ(IntValue(fixture_case, row), field->get_value<int32_t>());
            break;
        case DATE:
            EXPECT_EQ(DateValue(fixture_case, row),
                      field->get_value<int32_t>());
            break;
        case INT64:
        case TIMESTAMP:
            EXPECT_EQ(LongValue(fixture_case, row),
                      field->get_value<int64_t>());
            break;
        case FLOAT:
            EXPECT_EQ(FloatBits(ExpectedFloatValue(fixture_case, row)),
                      FloatBits(field->get_value<float>()));
            break;
        case DOUBLE:
            EXPECT_EQ(DoubleBits(ExpectedDoubleValue(fixture_case, row)),
                      DoubleBits(field->get_value<double>()));
            break;
        case TEXT:
        case STRING:
            EXPECT_EQ(StringValue(fixture_case, row),
                      StringOf(field->get_string_value()));
            break;
        case BLOB:
            EXPECT_EQ(BlobValue(fixture_case, row),
                      StringOf(field->get_string_value()));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

struct Range {
    int start;
    int end;
};

std::vector<Range> QueryRanges(const FixtureCase& fixture_case) {
    std::vector<Range> ranges;
    ranges.push_back({0, fixture_case.row_count - 1});
    int page_rows = fixture_case.page_rows;
    if (fixture_case.null_pattern != NullPattern::NONE && page_rows > 0 &&
        fixture_case.row_count > 2 * page_rows) {
        ranges.push_back({page_rows - 1, std::min(fixture_case.row_count - 1,
                                                  2 * page_rows + 1)});
        ranges.push_back({page_rows, std::min(fixture_case.row_count - 1,
                                              2 * page_rows - 1)});
    }
    return ranges;
}

void ValidateTableRange(const std::string& directory,
                        const FixtureCase& fixture_case, const Range& range,
                        bool with_anchor) {
    SCOPED_TRACE(::testing::Message()
                 << fixture_case.Describe() << " table range=" << range.start
                 << ":" << range.end << " anchor=" << with_anchor);
    std::string path = JoinPath(directory, fixture_case.FileName());
    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(path));
    std::vector<std::string> columns = {kTagColumn, kValueColumn};
    if (with_anchor) {
        columns.push_back(kAnchorColumn);
    }
    ResultSet* raw_result_set = nullptr;
    ASSERT_EQ(E_OK, reader.query(kTableName, columns, range.start, range.end,
                                 raw_result_set));
    ASSERT_NE(nullptr, raw_result_set);
    auto cleanup = [&reader](ResultSet* result) {
        reader.destroy_query_data_set(result);
    };
    std::unique_ptr<ResultSet, decltype(cleanup)> owned_result(raw_result_set,
                                                               cleanup);
    std::shared_ptr<ResultSetMetadata> metadata = owned_result->get_metadata();
    ASSERT_EQ(with_anchor ? 4U : 3U, metadata->get_column_count());
    ASSERT_EQ("time", metadata->get_column_name(1));
    ASSERT_EQ(INT64, metadata->get_column_type(1));
    ASSERT_EQ(kTagColumn, metadata->get_column_name(2));
    ASSERT_EQ(STRING, metadata->get_column_type(2));
    ASSERT_EQ(kValueColumn, metadata->get_column_name(3));
    ASSERT_EQ(fixture_case.data_type, metadata->get_column_type(3));

    // Rows arrive grouped by device, each device's rows time-ordered, so the
    // tag only changes at device boundaries.  Verify it once per device rather
    // than materializing a string Field for every row; primitive columns use
    // the direct buffer accessors.  This keeps a 1M-row fixture's validation
    // from doing a million string copies and map inserts.
    const int range_len = range.end - range.start + 1;
    int device = 0;
    int expected_time = range.start;
    int rows_in_device = 0;
    int total_rows = 0;
    bool tag_verified = false;
    bool has_next = false;
    while (true) {
        ASSERT_EQ(E_OK, owned_result->next(has_next));
        if (!has_next) {
            break;
        }
        total_rows++;
        SCOPED_TRACE(::testing::Message()
                     << "device=" << device << " t=" << expected_time);

        if (!tag_verified) {
            common::String* tag = owned_result->get_value<common::String*>(2);
            ASSERT_NE(nullptr, tag);
            EXPECT_EQ(DeviceId(device), StringOf(tag));
            tag_verified = true;
        }

        EXPECT_EQ(static_cast<int64_t>(expected_time),
                  owned_result->get_int64_at(1));

        const bool expected_null = IsNull(fixture_case, expected_time);
        EXPECT_EQ(expected_null, owned_result->is_null(3));
        if (!expected_null) {
            ASSERT_NO_FATAL_FAILURE(ExpectTableValue(
                fixture_case, expected_time, owned_result.get()));
        }

        if (with_anchor) {
            EXPECT_EQ(static_cast<int64_t>(expected_time),
                      owned_result->get_int64_at(4));
        }

        expected_time++;
        if (++rows_in_device == range_len) {
            device++;
            expected_time = range.start;
            rows_in_device = 0;
            tag_verified = false;
        }
    }
    EXPECT_EQ(fixture_case.devices, device);
    EXPECT_EQ(fixture_case.devices * range_len, total_rows);
    owned_result.reset();
    ASSERT_EQ(E_OK, reader.close());
}

void ValidateTreeRange(const std::string& directory,
                       const FixtureCase& fixture_case, int device,
                       const Range& range, bool with_anchor) {
    SCOPED_TRACE(::testing::Message()
                 << fixture_case.Describe() << " tree device=" << device
                 << " range=" << range.start << ":" << range.end
                 << " anchor=" << with_anchor);
    std::string path = JoinPath(directory, fixture_case.FileName());
    TsFileTreeReader reader;
    ASSERT_EQ(E_OK, reader.open(path));
    std::vector<std::string> device_ids = {DeviceId(device)};
    std::vector<std::string> measurements = {kValueColumn};
    if (with_anchor) {
        measurements.push_back(kAnchorColumn);
    }
    ResultSet* result = nullptr;
    ASSERT_EQ(E_OK, reader.query(device_ids, measurements, range.start,
                                 range.end, result));
    ASSERT_NE(nullptr, result);
    auto cleanup = [&reader](ResultSet* owned) {
        reader.destroy_query_data_set(owned);
    };
    std::unique_ptr<ResultSet, decltype(cleanup)> owned_result(result, cleanup);
    ResultSetIterator iter = owned_result->iterator();
    int expected_time = range.start;
    while (iter.hasNext()) {
        RowRecord* record = iter.next();
        ASSERT_NE(nullptr, record);
        if (!with_anchor) {
            while (expected_time <= range.end &&
                   IsNull(fixture_case, expected_time)) {
                expected_time++;
            }
        }
        ASSERT_LE(expected_time, range.end);
        EXPECT_EQ(static_cast<int64_t>(expected_time), record->get_timestamp());
        Field* value_field = record->get_field(1);
        ASSERT_NE(nullptr, value_field);
        bool is_null = value_field->type_ == NULL_TYPE;
        EXPECT_EQ(IsNull(fixture_case, expected_time), is_null);
        if (!is_null) {
            ExpectFieldValue(fixture_case, expected_time, value_field);
        }
        if (with_anchor) {
            Field* anchor_field = record->get_field(2);
            ASSERT_NE(nullptr, anchor_field);
            EXPECT_EQ(static_cast<int64_t>(expected_time),
                      anchor_field->get_value<int64_t>());
        }
        expected_time++;
    }
    if (!with_anchor) {
        while (expected_time <= range.end &&
               IsNull(fixture_case, expected_time)) {
            expected_time++;
        }
    }
    EXPECT_EQ(range.end + 1, expected_time);
    owned_result.reset();
    ASSERT_EQ(E_OK, reader.close());
}

void ValidateFixture(const std::string& directory,
                     const FixtureCase& fixture_case) {
    SCOPED_TRACE(fixture_case.Describe());
    for (const Range& range : QueryRanges(fixture_case)) {
        if (fixture_case.model == Model::TABLE) {
            ValidateTableRange(directory, fixture_case, range, false);
            if (fixture_case.null_pattern != NullPattern::NONE) {
                ValidateTableRange(directory, fixture_case, range, true);
            }
        } else {
            for (int device = 0; device < fixture_case.devices; device++) {
                ValidateTreeRange(directory, fixture_case, device, range,
                                  false);
                if (fixture_case.null_pattern != NullPattern::NONE) {
                    ValidateTreeRange(directory, fixture_case, device, range,
                                      true);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Capability probes for the spec integrity test
// ---------------------------------------------------------------------------

bool EncoderSupported(TSEncoding encoding, TSDataType data_type) {
    Encoder* encoder = EncoderFactory::alloc_value_encoder(encoding, data_type);
    if (encoder == nullptr) {
        return false;
    }
    EncoderFactory::free(encoder);
    return true;
}

bool DecoderSupported(TSEncoding encoding, TSDataType data_type) {
    Decoder* decoder = DecoderFactory::alloc_value_decoder(encoding, data_type);
    if (decoder == nullptr) {
        return false;
    }
    DecoderFactory::free(decoder);
    return true;
}

bool CompressionSupported(CompressionType compression) {
    Compressor* compressor = CompressorFactory::alloc_compressor(compression);
    if (compressor == nullptr) {
        return false;
    }
    CompressorFactory::free(compressor);
    return true;
}

CompressionType ParseCompressionOrFail(const std::string& name) {
    CompressionType compression = INVALID_COMPRESSION;
    if (!ParseCompressionName(name, compression)) {
        ADD_FAILURE() << "Bad compression name " << name;
    }
    return compression;
}

std::string CapabilityKey(Model model, TSDataType data_type,
                          TSEncoding encoding, CompressionType compression) {
    return std::string(ModelName(model)) + "/" + get_data_type_name(data_type) +
           "/" + get_encoding_name(encoding) + "/" +
           get_compression_name(compression);
}

}  // namespace

class EncodingCompressionCompatibilityTest : public ::testing::Test {
   protected:
    void SetUp() override { ASSERT_EQ(E_OK, libtsfile_init()); }
    void TearDown() override { libtsfile_destroy(); }
};

// Guards the spec against drift on the C++ side: every combination the C++
// encoder/decoder factory supports must be declared, every declared C++ case
// must be constructible, and every declared gap must carry a reason. Runs
// without any environment configuration.
TEST_F(EncodingCompressionCompatibilityTest, SpecMatchesCppCapabilities) {
    std::vector<FixtureCase> cases = ReadSpec(ResolveSpecPath());
    std::unordered_set<std::string> write_coverage;
    std::unordered_set<std::string> read_coverage;
    for (const FixtureCase& fixture_case : cases) {
        if (fixture_case.gap_reason.empty()) {
            EXPECT_TRUE(fixture_case.java_write && fixture_case.cpp_write &&
                        fixture_case.java_read && fixture_case.cpp_read)
                << "gapReason is required when a case declares a gap: "
                << fixture_case.Describe();
        }
        if (fixture_case.cpp_write) {
            EXPECT_TRUE(fixture_case.cpp_read)
                << "a C++-written case must also be C++-readable: "
                << fixture_case.Describe();
            EXPECT_TRUE(
                EncoderSupported(fixture_case.encoding, fixture_case.data_type))
                << "C++ cannot build an encoder for "
                << fixture_case.Describe();
            write_coverage.insert(
                CapabilityKey(fixture_case.model, fixture_case.data_type,
                              fixture_case.encoding, fixture_case.compression));
        }
        if (fixture_case.cpp_read) {
            EXPECT_TRUE(
                DecoderSupported(fixture_case.encoding, fixture_case.data_type))
                << "C++ cannot build a decoder for " << fixture_case.Describe();
            read_coverage.insert(
                CapabilityKey(fixture_case.model, fixture_case.data_type,
                              fixture_case.encoding, fixture_case.compression));
        }
    }

    std::vector<std::string> available_compressions;
    for (const char* name : kAllCompressions) {
        CompressionType compression = ParseCompressionOrFail(name);
        if (CompressionSupported(compression)) {
            available_compressions.push_back(name);
        }
    }
    ASSERT_FALSE(available_compressions.empty());

    int missing = 0;
    for (Model model : {Model::TABLE, Model::TREE}) {
        for (TSDataType data_type : kMatrixDataTypes) {
            for (int e = PLAIN; e <= CAMEL; e++) {
                TSEncoding encoding = static_cast<TSEncoding>(e);
                const bool can_write = EncoderSupported(encoding, data_type);
                const bool can_read = DecoderSupported(encoding, data_type);
                if (!can_write && !can_read) {
                    continue;
                }
                for (const std::string& compression_name :
                     available_compressions) {
                    CompressionType compression =
                        ParseCompressionOrFail(compression_name);
                    std::string key =
                        CapabilityKey(model, data_type, encoding, compression);
                    if (can_write && write_coverage.count(key) == 0) {
                        ADD_FAILURE()
                            << "compatibility-matrix.csv does not declare a "
                               "C++-writable case for "
                            << key;
                        missing++;
                    }
                    if (can_read && read_coverage.count(key) == 0) {
                        ADD_FAILURE()
                            << "compatibility-matrix.csv does not declare a "
                               "C++-readable case for "
                            << key;
                        missing++;
                    }
                }
            }
        }
    }
    EXPECT_EQ(0, missing);
}

TEST_F(EncodingCompressionCompatibilityTest, GenerateFixtures) {
    const char* output_dir = std::getenv(kGenerateDirEnv);
    if (output_dir == nullptr || std::strlen(output_dir) == 0) {
        GTEST_SKIP() << kGenerateDirEnv << " is not set";
    }
    EnsureDirectory(output_dir);
    std::vector<FixtureCase> cases = ReadSpec(ResolveSpecPath());
    std::vector<FixtureCase> written;
    std::vector<ReportRow> report;
    const int shard_index = GetShardIndex();
    const int shard_count = GetShardCount();
    int case_index = 0;
    for (const FixtureCase& fixture_case : cases) {
        const int assigned_shard = case_index % shard_count;
        ++case_index;
        if (assigned_shard != shard_index) {
            continue;
        }
        if (!fixture_case.cpp_write) {
            report.push_back(
                {fixture_case.id, "not-mine", fixture_case.gap_reason});
            continue;
        }
        ASSERT_NO_FATAL_FAILURE(WriteFixture(output_dir, fixture_case));
        ASSERT_NO_FATAL_FAILURE(ValidateFixture(output_dir, fixture_case));
        written.push_back(fixture_case);
        report.push_back({fixture_case.id, "generated-and-validated", ""});
    }
    WriteManifest(output_dir, written);
    WriteReport(output_dir, kReportFile, report);
    WriteSummary(output_dir, kSummaryFile, "C++ fixture generation", cases,
                 report);
}

TEST_F(EncodingCompressionCompatibilityTest, ValidateFixtures) {
    const char* input_dir = std::getenv(kValidateDirEnv);
    if (input_dir == nullptr || std::strlen(input_dir) == 0) {
        GTEST_SKIP() << kValidateDirEnv << " is not set";
    }
    std::vector<ManifestEntry> manifest = ReadManifest(input_dir);
    ASSERT_FALSE(manifest.empty());
    const std::string producer = manifest.front().producer;
    ASSERT_TRUE(producer == "java" || producer == "cpp")
        << "unexpected producer " << producer;

    std::vector<FixtureCase> spec_cases = ReadSpec(ResolveSpecPath());
    std::map<std::string, FixtureCase> spec;
    for (const FixtureCase& fixture_case : spec_cases) {
        spec[fixture_case.id] = fixture_case;
    }

    // The generator partitions the spec by shardIndex % shardCount, so the
    // validator applies the same partition to compute the expected subset for
    // this shard (rather than expecting the whole spec in one manifest).
    std::unordered_set<std::string> expected_ids;
    const int shard_index = GetShardIndex();
    const int shard_count = GetShardCount();
    int case_index = 0;
    for (const FixtureCase& fixture_case : spec_cases) {
        const int assigned_shard = case_index % shard_count;
        ++case_index;
        if (assigned_shard == shard_index && fixture_case.Writes(producer)) {
            expected_ids.insert(fixture_case.id);
        }
    }
    std::unordered_set<std::string> manifest_ids;
    for (const ManifestEntry& entry : manifest) {
        manifest_ids.insert(entry.fixture_case.id);
    }
    for (const std::string& id : expected_ids) {
        if (manifest_ids.count(id) == 0) {
            ADD_FAILURE() << "manifest for " << producer
                          << " is missing declared case " << id;
        }
    }
    for (const std::string& id : manifest_ids) {
        if (expected_ids.count(id) == 0) {
            ADD_FAILURE() << "manifest for " << producer
                          << " has undeclared case " << id;
        }
    }

    std::vector<ReportRow> report;
    for (const ManifestEntry& entry : manifest) {
        const FixtureCase& produced = entry.fixture_case;
        std::map<std::string, FixtureCase>::const_iterator it =
            spec.find(produced.id);
        if (it == spec.end()) {
            ADD_FAILURE() << "manifest case absent from the spec: "
                          << produced.id;
            continue;
        }
        const FixtureCase& fixture_case = it->second;
        ASSERT_NO_FATAL_FAILURE(ExpectSameCase(fixture_case, produced));
        if (!fixture_case.Reads(kLanguage)) {
            report.push_back(
                {produced.id, "skipped-declared-gap", fixture_case.gap_reason});
            continue;
        }
        std::string path = JoinPath(input_dir, produced.FileName());
        std::ifstream input(path.c_str(), std::ios::binary);
        ASSERT_TRUE(input.good())
            << "missing fixture declared as readable: " << path;
        ASSERT_NO_FATAL_FAILURE(ValidateFixture(input_dir, fixture_case));
        report.push_back({produced.id, "validated", ""});
    }
    WriteReport(input_dir, std::string(kReportFile) + ".validate", report);
    std::vector<FixtureCase> all_cases;
    for (const auto& entry : spec) {
        all_cases.push_back(entry.second);
    }
    WriteSummary(input_dir, std::string(kSummaryFile) + ".validate",
                 "C++ validation of " + producer + " fixtures", all_cases,
                 report);
}
