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

#include <cwrapper/tsfile_cwrapper.h>
#include <time.h>
#include <writer/tsfile_table_writer.h>

#include <iostream>
#include <random>
#include <string>

#include "cpp_examples.h"

using namespace storage;

int demo_write() {
    int code = 0;
    libtsfile_init();

    std::string table_name = "table1";

    // Create a file with specify path to write tsfile.
    storage::WriteFile file;
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    file.create("test_cpp.tsfile", flags, mode);

    // Create table schema to describe a table in a tsfile.
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



    // Create a file with specify path to write tsfile.
    auto* writer = new TsFileTableWriter(&file, schema);

    // Create tablet to insert data.
    storage::Tablet tablet = storage::Tablet(
        table_name, {"id1", "id2", "s1"},
        {common::STRING, common::STRING, common::INT64},
        {common::ColumnCategory::TAG, common::ColumnCategory::TAG,
         common::ColumnCategory::FIELD},
        10);


    for (int row = 0; row < 5; row++) {
        long timestamp = row;
        tablet.add_timestamp(row, timestamp);
        tablet.add_value(row, "id1", "id1_filed_1");
        tablet.add_value(row, "id2", "id1_filed_2");
        tablet.add_value(row, "s1", static_cast<int64_t>(row));
    }

    // Write tablet data.
    HANDLE_ERROR(writer->write_table(tablet));

    // Flush data
    HANDLE_ERROR(writer->flush());

    // Close writer.
    HANDLE_ERROR(writer->close());

    delete writer;

    return 0;
}
