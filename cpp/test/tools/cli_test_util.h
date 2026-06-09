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

#ifndef TSFILE_CLI_TEST_UTIL_H
#define TSFILE_CLI_TEST_UTIL_H

#include <fcntl.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include <sstream>
#include <string>

#include "common/schema.h"
#include "common/tablet.h"
#include "file/write_file.h"
#include "writer/tsfile_table_writer.h"

namespace tsfile_cli_test {

// Unique per-process path so tests stay isolated when ctest runs the
// gtest-discovered cases in parallel processes.
inline std::string unique_temp_path(const std::string& stem,
                                    const std::string& ext) {
    static unsigned counter = 0;
#ifdef _WIN32
    long pid = static_cast<long>(_getpid());
#else
    long pid = static_cast<long>(getpid());
#endif
    std::ostringstream ss;
    ss << stem << "_" << pid << "_" << counter++ << ext;
    return ss.str();
}

inline std::string write_table_fixture() {
    storage::libtsfile_init();
    std::string out_path = unique_temp_path("tsfile_cli_fixture", ".tsfile");
    std::string table_name = "table1";

    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    file.create(out_path, flags, 0666);

    auto* schema = new storage::TableSchema(
        table_name,
        {
            common::ColumnSchema("id1", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("id2", common::STRING, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::TAG),
            common::ColumnSchema("s1", common::INT64, common::UNCOMPRESSED,
                                 common::PLAIN, common::ColumnCategory::FIELD),
        });

    auto* writer = new storage::TsFileTableWriter(&file, schema);
    storage::Tablet tablet(
        table_name, {"id1", "id2", "s1"},
        {common::STRING, common::STRING, common::INT64},
        {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
         common::ColumnCategory::FIELD},
        10);

    for (int row = 0; row < 5; ++row) {
        tablet.add_timestamp(row, static_cast<int64_t>(row));
        tablet.add_value(row, "id1", "id1_field_1");
        tablet.add_value(row, "id2", "id2_field_2");
        tablet.add_value(row, "s1", static_cast<int64_t>(row * 10));
    }

    writer->write_table(tablet);
    writer->flush();
    writer->close();

    delete writer;
    delete schema;
    return out_path;
}

}  // namespace tsfile_cli_test

#endif  // TSFILE_CLI_TEST_UTIL_H
