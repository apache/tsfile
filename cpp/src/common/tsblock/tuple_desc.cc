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
#include "tuple_desc.h"

namespace common {
uint32_t TupleDesc::get_single_row_len(int *erro_code) {
    int size = get_column_count();
    int totol_len = 0;
    for (int i = 0; i < size; ++i) {
        switch (column_list_[i].type_) {
            case common::BOOLEAN: {
                totol_len += sizeof(bool);
                break;
            }
            case common::INT32: {
                totol_len += sizeof(int32_t);
                break;
            }
            case common::INT64: {
                totol_len += sizeof(int64_t);
                break;
            }
            case common::FLOAT: {
                totol_len += sizeof(float);
                break;
            }
            case common::DOUBLE: {
                totol_len += sizeof(double);
                break;
            }
            case common::TEXT: {
                totol_len += DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN;
                break;
            }
            default: {
                // log_err("TsBlock::BuildVector unknown type %d",
                // static_cast<int>(column_list_[i].type_));
                *erro_code = E_TYPE_NOT_SUPPORTED;
                break;
            }
        }
    }
    return totol_len;
}

uint32_t get_len(TSDataType type) {
    switch (type) {
        case common::BOOLEAN: {
            return sizeof(bool);
        }
        case common::INT32: {
            return sizeof(int32_t);
        }
        case common::INT64: {
            return sizeof(int64_t);
        }
        case common::FLOAT: {
            return sizeof(float);
        }
        case common::DOUBLE: {
            return sizeof(double);
        }
        case common::TEXT: {
            return DEFAULT_RESERVED_SIZE_OF_TEXT + TEXT_LEN;
        }
        default: {
            // todo: log error
        }
    }
    return 0;
}
}  // namespace common
