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

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <vector>

#include "common/schema.h"
#include "common/tablet.h"
#include "encoding/alp/alp_scalar.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"

namespace {
constexpr uint32_t kPageRows = 1103;
constexpr uint32_t kRows = 3 * kPageRows + 32;

void check(int code) {
    if (code != common::E_OK) {
        std::cerr << "TsFile error: " << code << '\n';
        std::exit(EXIT_FAILURE);
    }
}

template <typename T, typename U>
T from_bits(U bits) {
    static_assert(sizeof(T) == sizeof(U), "Mismatched bit width");
    T value;
    std::memcpy(&value, &bits, sizeof(value));
    return value;
}

float float_value(uint32_t row) {
    if (row >= 3 * kPageRows) {
        row = (row % 4 == 0) ? 7 : (row % 4 == 1) ? 8 : 1023;
    } else if (row >= 2 * kPageRows) {
        return -7.25f;
    }
    switch (row) {
        case 7:
            return from_bits<float>(uint32_t{0x7fc12345});
        case 8:
            return std::numeric_limits<float>::infinity();
        case 1023:
            return -std::numeric_limits<float>::infinity();
        case 1024:
            return -0.0f;
        case 1099:
            return std::numeric_limits<float>::denorm_min();
        case 600:
            return 0.1234567f;
        default:
            return static_cast<float>(static_cast<int>(row % 61) - 30) *
                   0.01f;
    }
}

double double_value(uint32_t row) {
    if (row >= 3 * kPageRows) {
        row = (row % 4 == 0) ? 7 : (row % 4 == 1) ? 8 : 1023;
    } else if (row >= 2 * kPageRows) {
        return 42.125;
    }
    switch (row) {
        case 7:
            return from_bits<double>(uint64_t{0xfff8123456789abc});
        case 8:
            return std::numeric_limits<double>::infinity();
        case 1023:
            return -std::numeric_limits<double>::infinity();
        case 1024:
            return -0.0;
        case 1099:
            return std::numeric_limits<double>::denorm_min();
        case 600:
            return 0.123456789012345;
        default:
            return static_cast<double>(static_cast<int>(row % 127) - 63) *
                   0.0001;
    }
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::cerr << "Usage: generate <output.tsfile> <empty-page.bin>\n";
        return EXIT_FAILURE;
    }

    check(storage::libtsfile_init());
    std::vector<uint8_t> empty_float_page;
    if (storage::alp::AlpEncodePage<float>(NULL, 0, empty_float_page) !=
        storage::alp::ALP_OK) {
        return EXIT_FAILURE;
    }
    std::ofstream empty_page_file(argv[2], std::ios::binary);
    empty_page_file.write(reinterpret_cast<const char*>(empty_float_page.data()),
                          empty_float_page.size());
    if (!empty_page_file) {
        return EXIT_FAILURE;
    }
    empty_page_file.close();
    check(storage::set_page_max_point_count(kPageRows));
    common::g_config_value_.time_compress_type_ = common::UNCOMPRESSED;
    {
        storage::WriteFile file;
        check(file.create(argv[1], O_WRONLY | O_CREAT | O_TRUNC, 0644));
        std::vector<storage::MeasurementSchema*> columns = {
            new storage::MeasurementSchema("tag", common::STRING, common::PLAIN,
                                           common::UNCOMPRESSED),
            new storage::MeasurementSchema("keep", common::INT32,
                                           common::TS_2DIFF,
                                           common::UNCOMPRESSED),
            new storage::MeasurementSchema("f", common::FLOAT, common::ALP,
                                           common::UNCOMPRESSED),
            new storage::MeasurementSchema("d", common::DOUBLE, common::ALP,
                                           common::UNCOMPRESSED)};
        std::vector<common::ColumnCategory> categories = {
            common::ColumnCategory::TAG, common::ColumnCategory::FIELD,
            common::ColumnCategory::FIELD, common::ColumnCategory::FIELD};
        storage::TableSchema schema("alp_table", columns, categories);
        storage::TsFileTableWriter writer(&file, &schema);
        check(writer.get_error());

        storage::Tablet tablet(
            schema.get_table_name(), schema.get_measurement_names(),
            schema.get_data_types(), schema.get_column_categories(), kRows);
        for (uint32_t row = 0; row < kRows; ++row) {
            check(tablet.add_timestamp(row, row));
            check(tablet.add_value(row, "tag", common::String("dev")));
            // Keep rows visible when the entire FLOAT/DOUBLE page is null.
            check(tablet.add_value(row, "keep", static_cast<int32_t>(row)));
            if (row < kPageRows || row >= 2 * kPageRows) {
                check(tablet.add_value(row, "f", float_value(row)));
                check(tablet.add_value(row, "d", double_value(row)));
            }
        }
        check(writer.write_table(tablet));
        check(writer.close());
    }
    storage::libtsfile_destroy();
}
