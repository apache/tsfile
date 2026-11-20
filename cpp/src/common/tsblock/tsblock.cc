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
#include "tsblock.h"

#include <string>

namespace common {

int TsBlock::init() {
    int ret = 0;
    uint32_t row_size = tuple_desc_->get_single_row_len(&ret);
    if (ret != 0) {
        return ret;
    }

    // No maximum number of rows given, calculated with capacity_
    if (LIKELY(max_row_count_ == 0)) {
        max_row_count_ = capacity_ / row_size;
    } else {
        // max_row_count_ given, calculated with max_row_count_
        capacity_ = row_size * max_row_count_;
    }
    int colnum = tuple_desc_->get_column_count();
    for (int i = 0; i < colnum; ++i) {
        ret = build_vector(tuple_desc_->get_column_type(i), max_row_count_);
        if (ret != 0) {
            return ret;
        }
    }
    return ret;
}

int TsBlock::build_vector(common::TSDataType type, uint32_t row_count) {
    Vector* vec;
    int ret = 0;
    if (LIKELY(type != common::TEXT && type != common::STRING &&
               type != common::BLOB)) {
        vec = new FixedLengthVector(type, row_count, get_len(type), this);
    } else if (type == common::TEXT || type == common::STRING ||
               type == common::BLOB) {
        vec = new VariableLengthVector(
            type, row_count, DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN, this);
    } else {
        // log_err("TsBlock::BuildVector failed because of unknown type %d",
        // static_cast<int>(type));
        ret = E_TYPE_NOT_SUPPORTED;
    }
    vectors_.push_back(vec);
    return ret;
}

#ifdef DEBUG
std::string TsBlock::debug_string() {
    std::stringstream out;
    out << "print TsBlock: " << this << std::endl
        << "capacity_: " << capacity_ << std::endl
        << "row_count_: " << row_count_ << std::endl
        << "max_row_count_: " << max_row_count_ << std::endl;

    out << "------ tuple desc ------" << std::endl << std::endl;
    out << tuple_desc_->debug_string() << std::endl;

    out << "------ real data area ------" << std::endl;

    RowIterator iter(this);
    while (!iter.end()) {
        out << iter.debug_string() << std::endl;
        iter.next();
    }
    return out.str();
}

std::string RowIterator::debug_string() {
    std::stringstream out;
    out.precision(20);  // Default precision is 6, set precision, otherwise
                        // double precision will be lost

    for (uint32_t i = 0; i < column_count_; ++i) {
        bool is_null = false;
        uint32_t len = 0;
        void* value = read(i, &len, &is_null);
        if (is_null) {
            out << "NULL";
        } else {
            ColumnSchema& col_schema =
                tsblock_->tuple_desc_->get_column_schema(i);
            switch (col_schema.data_type_) {
                case common::BOOLEAN: {
                    out << *static_cast<bool*>(value);
                    break;
                }
                case common::INT32: {
                    out << *static_cast<int32_t*>(value);
                    break;
                }
                case common::INT64: {
                    out << *static_cast<int64_t*>(value);
                    break;
                }
                case common::FLOAT: {
                    out << *static_cast<float*>(value);
                    break;
                }
                case common::DOUBLE: {
                    out << *static_cast<double*>(value);
                    break;
                }
                case common::TEXT: {
                    out << std::string(static_cast<char*>(value), len);
                    break;
                }
                case common::STRING: {
                    out << *static_cast<String*>(value);
                    break;
                }
                default: {
                    out << "ERR";
                }
            }
        }
        if (i + 1 < tsblock_->tuple_desc_->get_column_count()) {
            out << ", ";
        }
    }
    return out.str();
}
#endif
}  // namespace common
