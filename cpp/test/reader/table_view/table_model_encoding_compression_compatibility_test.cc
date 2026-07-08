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

#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
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

#include "common/db_common.h"
#include "common/schema.h"
#include "common/tablet.h"
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
    "file,table,tagColumn,valueColumn,dataType,encoding,compression,rowCount";
const char* const kTableName = "compat_table";
const char* const kTagColumn = "device";
const char* const kValueColumn = "value";
const char* const kTagValue = "compat_device";
constexpr int kRowCount = 32;

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
            << get_compression_name(compression) << "," << row_count;
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
    if (parts.size() != 8) {
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

float FloatValue(int row) {
    uint32_t bits =
        kFloatBits[row % (sizeof(kFloatBits) / sizeof(kFloatBits[0]))];
    float value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

uint64_t DoubleBits(double value) {
    uint64_t bits;
    std::memcpy(&bits, &value, sizeof(bits));
    return bits;
}

double DoubleValue(int row) {
    uint64_t bits =
        kDoubleBits[row % (sizeof(kDoubleBits) / sizeof(kDoubleBits[0]))];
    double value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

int32_t DateValue(int row) {
    return kDateValues[row % (sizeof(kDateValues) / sizeof(kDateValues[0]))];
}

std::vector<FixtureCase> BuildMatrix() {
    std::vector<FixtureCase> cases;
    CompressionType compressions[] = {UNCOMPRESSED, ZSTD, LZMA2};
    TSDataType data_types[] = {INT32, DATE, INT64, TIMESTAMP, FLOAT, DOUBLE};
    for (CompressionType compression : compressions) {
        for (TSDataType data_type : data_types) {
            cases.emplace_back(data_type, CHIMP, compression, kRowCount);
            cases.emplace_back(data_type, RLBE, compression, kRowCount);
        }
        cases.emplace_back(DOUBLE, CAMEL, compression, kRowCount);
    }
    return cases;
}

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
    return new TableSchema(fixture_case.table_name, measurement_schemas,
                           column_categories);
}

void AddValue(Tablet& tablet, TSDataType data_type, int row) {
    switch (data_type) {
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
                      tablet.add_value(row, kValueColumn, FloatValue(row)));
            break;
        case DOUBLE:
            ASSERT_EQ(E_OK,
                      tablet.add_value(row, kValueColumn, DoubleValue(row)));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(data_type);
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
        AddValue(tablet, fixture_case.data_type, row);
    }
    return tablet;
}

void WriteFixture(const std::string& directory,
                  const FixtureCase& fixture_case) {
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
            ASSERT_EQ(FloatBits(FloatValue(row)),
                      FloatBits(result_set->get_value<float>(3)));
            break;
        case DOUBLE:
            ASSERT_EQ(DoubleBits(DoubleValue(row)),
                      DoubleBits(result_set->get_value<double>(3)));
            break;
        default:
            FAIL() << "Unsupported data type: "
                   << get_data_type_name(fixture_case.data_type);
    }
}

void ValidateFixture(const std::string& directory,
                     const FixtureCase& fixture_case) {
    SCOPED_TRACE(fixture_case.ToManifestLine());
    TsFileReader reader;
    ASSERT_EQ(E_OK, reader.open(JoinPath(directory, fixture_case.file_name)));
    ResultSet* raw_result_set = nullptr;
    ASSERT_EQ(E_OK,
              reader.query(fixture_case.table_name,
                           {fixture_case.tag_column, fixture_case.value_column},
                           INT64_MIN, INT64_MAX, raw_result_set));
    ASSERT_NE(nullptr, raw_result_set);
    auto* result_set = static_cast<TableResultSet*>(raw_result_set);
    std::shared_ptr<ResultSetMetadata> metadata = result_set->get_metadata();
    ASSERT_EQ(3U, metadata->get_column_count());
    ASSERT_EQ("time", metadata->get_column_name(1));
    ASSERT_EQ(INT64, metadata->get_column_type(1));
    ASSERT_EQ(fixture_case.tag_column, metadata->get_column_name(2));
    ASSERT_EQ(STRING, metadata->get_column_type(2));
    ASSERT_EQ(fixture_case.value_column, metadata->get_column_name(3));
    ASSERT_EQ(fixture_case.data_type, metadata->get_column_type(3));

    bool has_next = false;
    int row = 0;
    while (IS_SUCC(result_set->next(has_next)) && has_next) {
        SCOPED_TRACE(::testing::Message() << "row=" << row);
        ASSERT_EQ(row, result_set->get_value<int64_t>(1));
        ASSERT_FALSE(result_set->is_null(2));
        ASSERT_EQ(kTagValue,
                  result_set->get_value<common::String*>(2)->to_std_string());
        ASSERT_FALSE(result_set->is_null(3));
        AssertValue(fixture_case, row, result_set);
        ++row;
    }
    ASSERT_EQ(fixture_case.row_count, row);
    result_set->close();
    reader.destroy_query_data_set(result_set);
    ASSERT_EQ(E_OK, reader.close());
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
    for (const FixtureCase& fixture_case : cases) {
        WriteFixture(output_dir, fixture_case);
    }
    WriteManifest(output_dir, cases);
}

TEST_F(TableModelEncodingCompressionCompatibilityTest, ValidateFixtures) {
    const char* input_dir = std::getenv(kValidateDirEnv);
    if (input_dir == nullptr || std::strlen(input_dir) == 0) {
        GTEST_SKIP() << kValidateDirEnv << " is not set";
    }
    std::vector<FixtureCase> cases = ReadManifest(input_dir);
    for (const FixtureCase& fixture_case : cases) {
        ValidateFixture(input_dir, fixture_case);
    }
}
