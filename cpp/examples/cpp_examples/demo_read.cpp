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
#include <iostream>
#include <string>
#include <vector>

#include "../c_examples/c_examples.h"
#include "cpp_examples.h"

using namespace storage;

int demo_read() {

    int code = 0;
    libtsfile_init();
    std::string table_name = "table1";

    // Create tsfile reader and open tsfile with specify path.
    storage::TsFileReader reader;
    reader.open("test_cpp.tsfile");

    // Query data with tsfile reader.
    storage::ResultSet* temp_ret = nullptr;
    std::vector<std::string> columns;
    columns.emplace_back("id1");
    columns.emplace_back("id2");
    columns.emplace_back("s1");

    // Column vector contains the columns you want to select.
    HANDLE_ERROR(reader.query(table_name, columns, 0, 100, temp_ret));

    // Get query handler.
    auto ret = dynamic_cast<storage::TableResultSet*>(temp_ret);

    // Metadata in query handler.
    auto metadata = ret->get_metadata();
    int column_num = metadata->get_column_count();
    for (int i = 1; i <= column_num; i++) {
        std::cout << "column name: " << metadata->get_column_name(i)
                  << std::endl;
        std::cout << "column type: " << std::to_string(metadata->get_column_type(i))
                  << std::endl;
    }

    // Check and get next data.
    bool has_next = false;
    while ((code = ret->next(has_next)) == common::E_OK && has_next) {
        // Timestamp at column 1 and column index begin from 1.
        Timestamp timestamp = ret->get_value<Timestamp>(1);
        for (int i = 1; i <= column_num; i++) {
            if (ret->is_null(i)) {
                std::cout << "null" << std::endl;
            } else {
                switch (metadata->get_column_type(i)) {
                    case common::BOOLEAN:
                        std::cout << ret->get_value<bool>(i) << std::endl;
                        break;
                    case common::INT32:
                        std::cout << ret->get_value<int32_t>(i) << std::endl;
                        break;
                    case common::INT64:
                        std::cout << ret->get_value<int64_t>(i) << std::endl;
                        break;
                    case common::FLOAT:
                        std::cout << ret->get_value<float>(i) << std::endl;
                        break;
                    case common::DOUBLE:
                        std::cout << ret->get_value<double>(i) << std::endl;
                        break;
                    case common::STRING:
                        std::cout << *(ret->get_value<common::String*>(i))
                                  << std::endl;
                        break;
                    default:;
                }
            }
        }
    }

    // Close query result set.
    ret->close();

    // Close reader.
    reader.close();
    return 0;
}
