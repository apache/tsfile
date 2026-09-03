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

#include <fcntl.h>
#include <file/write_file.h>
#include <reader/tsfile_reader.h>
#include <writer/tsfile_table_writer.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

namespace {

int check(int code, const char* operation) {
    if (code != common::E_OK) {
        std::cerr << operation << " failed with error " << code << std::endl;
    }
    return code;
}

}  // namespace

int main() {
    storage::libtsfile_init();

    const std::string file_name = "example_cpp.tsfile";
    const std::string table_name = "sensors";
    std::remove(file_name.c_str());

    {
        storage::WriteFile file;
        int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        if (check(file.create(file_name, flags, 0666), "open writer") !=
            common::E_OK) {
            return 1;
        }

        storage::TableSchema schema(
            table_name,
            {common::ColumnSchema("device", common::STRING,
                                  common::UNCOMPRESSED, common::PLAIN,
                                  common::ColumnCategory::TAG),
             common::ColumnSchema("temperature", common::DOUBLE,
                                  common::UNCOMPRESSED, common::PLAIN,
                                  common::ColumnCategory::FIELD)});
        storage::TsFileTableWriter writer(&file, &schema);
        storage::Tablet tablet(
            table_name, {"device", "temperature"},
            {common::STRING, common::DOUBLE},
            {common::ColumnCategory::TAG, common::ColumnCategory::FIELD}, 5);

        for (uint32_t row = 0; row < 5; ++row) {
            tablet.add_timestamp(row, row);
            tablet.add_value(row, "device", "sensor_01");
            tablet.add_value(row, "temperature", 20.0 + row * 0.5);
        }

        if (check(writer.write_table(tablet), "write table") != common::E_OK ||
            check(writer.flush(), "flush writer") != common::E_OK ||
            check(writer.close(), "close writer") != common::E_OK) {
            return 1;
        }
    }

    {
        storage::TsFileReader reader;
        if (check(reader.open(file_name), "open reader") != common::E_OK) {
            return 1;
        }

        storage::ResultSet* result = nullptr;
        std::vector<std::string> columns{"device", "temperature"};
        if (check(reader.query(table_name, columns, 0, 4, result), "query") !=
            common::E_OK) {
            return 1;
        }

        bool has_next = false;
        int code = common::E_OK;
        while ((code = result->next(has_next)) == common::E_OK && has_next) {
            std::cout << result->get_value<double>("temperature") << std::endl;
        }
        result->close();
        reader.close();
        if (check(code, "read row") != common::E_OK) {
            return 1;
        }
    }

    storage::libtsfile_destroy();
    return 0;
}
