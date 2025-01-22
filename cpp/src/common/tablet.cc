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

#include "tablet.h"

#include <cstdlib>

#include "utils/errno_define.h"

using namespace common;

namespace storage {

int Tablet::init() {
    ASSERT(timestamps_ == nullptr);
    timestamps_ = (int64_t *)malloc(sizeof(int64_t) * max_row_num_);

    size_t schema_count = schema_vec_->size();
    std::pair<std::map<std::string, int>::iterator, bool> ins_res;
    for (size_t c = 0; c < schema_count; c++) {
        ins_res = schema_map_.insert(
            std::make_pair(schema_vec_->at(c).measurement_name_, c));
        if (!ins_res.second) {
            ASSERT(false);
            // maybe dup measurement_name
            return E_INVALID_ARG;
        }
    }
    ASSERT(schema_map_.size() == schema_count);
    value_matrix_ = (ValueMatrixEntry *)malloc(sizeof(ValueMatrixEntry) * schema_count);
    for (size_t c = 0; c < schema_count; ++c) {
        const MeasurementSchema &schema = schema_vec_->at(c);

        switch (schema.data_type_) {
            case BOOLEAN:
                value_matrix_[c].bool_data = (bool *)malloc(get_data_type_size(schema.data_type_) * max_row_num_);
                break;
            case INT32:
                value_matrix_[c].int32_data = (int32_t *)malloc(get_data_type_size(schema.data_type_) * max_row_num_);
                break;
            case INT64:
                value_matrix_[c].int64_data = (int64_t *)malloc(get_data_type_size(schema.data_type_) * max_row_num_);
                break;
            case FLOAT:
                value_matrix_[c].float_data = (float *)malloc(get_data_type_size(schema.data_type_) * max_row_num_);
                break;
            case DOUBLE:
                value_matrix_[c].double_data = (double *)malloc(get_data_type_size(schema.data_type_) * max_row_num_);
                break;
            case STRING: {
                value_matrix_[c].string_data = (common::String *)malloc(sizeof(String) * max_row_num_);
                break;
            }
            default:
                ASSERT(false);
            return E_INVALID_ARG;
        }
    }

    bitmaps_ = new BitMap[schema_count];
    for (size_t c = 0; c < schema_count; c++) {
        bitmaps_[c].init(max_row_num_, false);
    }
    return E_OK;
}

void Tablet::destroy() {
    if (timestamps_ != nullptr) {
        free(timestamps_);
        timestamps_ = nullptr;
    }

    if (value_matrix_ != nullptr) {
        for (size_t c = 0; c < schema_vec_->size(); c++) {
            const MeasurementSchema &schema = schema_vec_->at(c);
            switch (schema.data_type_) {
                case INT32:
                    free(value_matrix_[c].int32_data);
                break;
                case INT64:
                    free(value_matrix_[c].int64_data);
                break;
                case FLOAT:
                    free(value_matrix_[c].float_data);
                break;
                case DOUBLE:
                    free(value_matrix_[c].double_data);
                break;
                case BOOLEAN:
                    free(value_matrix_[c].bool_data);
                break;
                case STRING:
                    free(value_matrix_[c].string_data);
                break;
                default:
                        break;
            }
        }
        free(value_matrix_);
        value_matrix_ = nullptr;
    }

    if (bitmaps_ != nullptr) {
        delete[] bitmaps_;
        bitmaps_ = nullptr;
    }
}

int Tablet::add_timestamp(uint32_t row_index, int64_t timestamp) {
    ASSERT(timestamps_ != NULL);
    if (UNLIKELY(row_index >= static_cast<uint32_t>(max_row_num_))) {
        ASSERT(false);
        return E_OUT_OF_RANGE;
    }
    timestamps_[row_index] = timestamp;
    cur_row_size_ = std::max(row_index, cur_row_size_);
    return E_OK;
}

void* Tablet::get_value(int row_index, uint32_t schema_index, common::TSDataType& data_type) const {
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        return nullptr;
    }
    const MeasurementSchema& schema = schema_vec_->at(schema_index);

    ValueMatrixEntry column_values = value_matrix_[schema_index];
    data_type = schema.data_type_;
    if (bitmaps_[schema_index].test(row_index)) {
        return nullptr;
    }
    switch (schema.data_type_) {
        case BOOLEAN: {
            bool* bool_values = column_values.bool_data;
            return &bool_values[row_index];
        }
        case INT32: {
            int32_t* int32_values = column_values.int32_data;
            return &int32_values[row_index];
        }
        case INT64: {
            int64_t* int64_values = column_values.int64_data;
            return &int64_values[row_index];
        }
        case FLOAT: {
            float* float_values = column_values.float_data;
            return &float_values[row_index];
        }
        case DOUBLE: {
            double* double_values = column_values.double_data;
            return &double_values[row_index];
        }
        case STRING: {
            auto string_values = column_values.string_data;
            return &string_values[row_index];
        }
        default:
            return nullptr;
    }
}

template <>
void Tablet::process_val(uint32_t row_index, uint32_t schema_index, common::String val) {
    value_matrix_[schema_index].string_data[row_index].dup_from(val, page_arena_);
    bitmaps_[schema_index].clear(row_index); /* mark as non-null */
}

template <typename T>
void Tablet::process_val(uint32_t row_index, uint32_t schema_index, T val) {
    switch (schema_vec_->at(schema_index).data_type_) {
        case common::BOOLEAN:
            (value_matrix_[schema_index].bool_data)[row_index] = static_cast<bool>(val);
        break;
        case common::INT32:
            value_matrix_[schema_index].int32_data[row_index] = static_cast<int32_t>(val);
        break;
        case common::INT64:
            value_matrix_[schema_index].int64_data[row_index] = static_cast<int64_t>(val);
        break;
        case common::FLOAT:
            value_matrix_[schema_index].float_data[row_index] = static_cast<float>(val);
        break;
        case common::DOUBLE:
            value_matrix_[schema_index].double_data[row_index] = static_cast<double>(val);
        break;
        default:
            ASSERT(false);
    }
    bitmaps_[schema_index].clear(row_index); /* mark as non-null */
}

template <typename T>
int Tablet::add_value(uint32_t row_index, uint32_t schema_index, T val) {
    int ret = common::E_OK;
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        ASSERT(false);
        ret = common::E_OUT_OF_RANGE;
    } else {
        const MeasurementSchema &schema = schema_vec_->at(schema_index);
        if (UNLIKELY(GetDataTypeFromTemplateType<T>() != schema.data_type_)) {
            if (GetDataTypeFromTemplateType<T>() == common::INT32 &&
                schema.data_type_ == common::INT64) {
                process_val(row_index, schema_index, static_cast<int64_t>(val));
                } else if (GetDataTypeFromTemplateType<T>() == common::FLOAT &&
                           schema.data_type_ == common::DOUBLE) {
                    process_val(row_index, schema_index, static_cast<double>(val));
                           } else {
                               ASSERT(false);
                               return E_TYPE_NOT_MATCH;
                           }
        } else {
            process_val(row_index, schema_index, val);
        }
    }
    return ret;
}

template <>
int Tablet::add_value(uint32_t row_index, uint32_t schema_index, common::String val) {
    int ret = common::E_OK;
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        ASSERT(false);
        ret = common::E_OUT_OF_RANGE;
    }
    process_val(row_index, schema_index, val);
    return ret;
}

template <typename T>
int Tablet::add_value(uint32_t row_index, const std::string &measurement_name,
                      T val) {
    int ret = common::E_OK;
    SchemaMapIterator find_iter = schema_map_.find(measurement_name);
    if (LIKELY(find_iter == schema_map_.end())) {
        ASSERT(false);
        ret = E_INVALID_ARG;
    } else {
        ret = add_value(row_index, find_iter->second, val);
    }
    return ret;
}

template int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                               bool val);
template int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                               int32_t val);
template int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                               int64_t val);
template int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                               float val);
template int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                               double val);

template int Tablet::add_value(uint32_t row_index,
                               const std::string &measurement_name, bool val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string &measurement_name,
                               int32_t val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string &measurement_name,
                               int64_t val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string &measurement_name, float val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string &measurement_name, double val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string &measurement_name,
                               String val);

void Tablet::set_column_categories(const std::vector<ColumnCategory>& column_categories) {
    column_categories_ = column_categories;
    id_column_indexes_.clear();
    for (size_t i = 0; i < column_categories_.size(); i++) {
        ColumnCategory columnCategory = column_categories_[i];
        if (columnCategory == ColumnCategory::TAG) {
            id_column_indexes_.push_back(i);
        }
    }
}

std::shared_ptr<IDeviceID> Tablet::get_device_id(int i) const {
    std::vector<std::string> id_array;
    id_array.push_back(insert_target_name_);
    for (auto id_column_idx : id_column_indexes_) {
        common::TSDataType data_type = INVALID_DATATYPE;
        void* value_ptr = get_value(i, id_column_idx, data_type);
        common::String str;
        switch (data_type) {
            case STRING:
                str = *static_cast<common::String*>(value_ptr);
                id_array.push_back(str.to_std_string());
                break;
            default:
                break ;
        }
    }
    return std::make_shared<StringArrayDeviceID>(id_array);
}

}  // end namespace storage