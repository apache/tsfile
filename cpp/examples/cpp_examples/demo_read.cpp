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

#include "cpp_examples.h"

std::string field_to_string(storage::Field *value) {
    if (value->type_ == common::TEXT) {
        return std::string(value->value_.sval_);
    } else {
        std::stringstream ss;
        switch (value->type_) {
            case common::BOOLEAN:
                ss << (value->value_.bval_ ? "true" : "false");
                break;
            case common::INT32:
                ss << value->value_.ival_;
                break;
            case common::INT64:
                ss << value->value_.lval_;
                break;
            case common::FLOAT:
                ss << value->value_.fval_;
                break;
            case common::DOUBLE:
                ss << value->value_.dval_;
                break;
            case common::NULL_TYPE:
                ss << "NULL";
                break;
            default:
                ASSERT(false);
                break;
        }
        return ss.str();
    }
}

int demo_read() {
    std::cout << "begin to read tsfile from demo_ts.tsfile" << std::endl;
    std::string device_name = "root.db001.dev001";
    std::string measurement_name = "m001";
    storage::Path p1(device_name, measurement_name);
    std::vector<storage::Path> select_list;
    select_list.push_back(p1);
    storage::QueryExpression *query_expr =
        storage::QueryExpression::create(select_list, nullptr);

    common::init_config_value();
    storage::TsFileReader reader;
    int ret = reader.open("cpp_rw.tsfile");

    std::cout << "begin to query expr" << std::endl;
    ASSERT(ret == 0);
    storage::QueryDataSet *qds = nullptr;
    ret = reader.query(query_expr, qds);

    storage::RowRecord *record;
    std::cout << "begin to dump data from tsfile ---" << std::endl;
    int row_cout = 0;
    do {
        record = qds->get_next();
        if (record) {
            std::cout << "dump QDS :  " << record->get_timestamp() << ",";
            if (record) {
                int size = record->get_fields()->size();
                for (int i = 0; i < size; ++i) {
                    std::cout << field_to_string(record->get_field(i)) << ",";
                }
                std::cout << std::endl;
                row_cout++;
            }
        } else {
            break;
        }
    } while (true);

    return (0);
}
