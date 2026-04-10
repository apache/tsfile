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

#include "allocator/alloc_base.h"
#include "datatype/date_converter.h"
#include "utils/errno_define.h"

using namespace common;

namespace storage {

int Tablet::init() {
    ASSERT(timestamps_ == nullptr);
    timestamps_ = static_cast<int64_t*>(
        common::mem_alloc(sizeof(int64_t) * max_row_num_, common::MOD_TABLET));
    if (timestamps_ == nullptr) return E_OOM;
    cur_row_size_ = 0;

    size_t schema_count = schema_vec_->size();
    std::pair<std::map<std::string, int>::iterator, bool> ins_res;
    for (size_t c = 0; c < schema_count; c++) {
        ins_res = schema_map_.insert(
            std::make_pair(to_lower(schema_vec_->at(c).measurement_name_), c));
        if (!ins_res.second) {
            return E_INVALID_ARG;
        }
    }
    ASSERT(schema_map_.size() == schema_count);
    value_matrix_ = static_cast<ValueMatrixEntry*>(common::mem_alloc(
        sizeof(ValueMatrixEntry) * schema_count, common::MOD_TABLET));
    if (value_matrix_ == nullptr) return E_OOM;
    for (size_t c = 0; c < schema_count; ++c) {
        const MeasurementSchema& schema = schema_vec_->at(c);

        switch (schema.data_type_) {
            case BOOLEAN: {
                size_t sz = sizeof(bool) * max_row_num_;
                value_matrix_[c].bool_data = static_cast<bool*>(
                    common::mem_alloc(sz, common::MOD_TABLET));
                if (value_matrix_[c].bool_data == nullptr) return E_OOM;
                memset(value_matrix_[c].bool_data, 0, sz);
                break;
            }
            case DATE:
            case INT32: {
                size_t sz = sizeof(int32_t) * max_row_num_;
                value_matrix_[c].int32_data = static_cast<int32_t*>(
                    common::mem_alloc(sz, common::MOD_TABLET));
                if (value_matrix_[c].int32_data == nullptr) return E_OOM;
                memset(value_matrix_[c].int32_data, 0, sz);
                break;
            }
            case TIMESTAMP:
            case INT64: {
                size_t sz = sizeof(int64_t) * max_row_num_;
                value_matrix_[c].int64_data = static_cast<int64_t*>(
                    common::mem_alloc(sz, common::MOD_TABLET));
                if (value_matrix_[c].int64_data == nullptr) return E_OOM;
                memset(value_matrix_[c].int64_data, 0, sz);
                break;
            }
            case FLOAT: {
                size_t sz = sizeof(float) * max_row_num_;
                value_matrix_[c].float_data = static_cast<float*>(
                    common::mem_alloc(sz, common::MOD_TABLET));
                if (value_matrix_[c].float_data == nullptr) return E_OOM;
                memset(value_matrix_[c].float_data, 0, sz);
                break;
            }
            case DOUBLE: {
                size_t sz = sizeof(double) * max_row_num_;
                value_matrix_[c].double_data = static_cast<double*>(
                    common::mem_alloc(sz, common::MOD_TABLET));
                if (value_matrix_[c].double_data == nullptr) return E_OOM;
                memset(value_matrix_[c].double_data, 0, sz);
                break;
            }
            case BLOB:
            case TEXT:
            case STRING: {
                auto* sc = static_cast<StringColumn*>(common::mem_alloc(
                    sizeof(StringColumn), common::MOD_TABLET));
                if (sc == nullptr) return E_OOM;
                new (sc) StringColumn();
                // 8 bytes/row is a conservative initial estimate for short
                // string columns (e.g. device IDs, tags). The buffer grows
                // automatically on demand via mem_realloc.
                sc->init(max_row_num_, max_row_num_ * 8);
                value_matrix_[c].string_col = sc;
                break;
            }
            default:
                ASSERT(false);
                return E_INVALID_ARG;
        }
    }

    bitmaps_ = static_cast<BitMap*>(
        common::mem_alloc(sizeof(BitMap) * schema_count, common::MOD_TABLET));
    if (bitmaps_ == nullptr) return E_OOM;
    for (size_t c = 0; c < schema_count; c++) {
        new (&bitmaps_[c]) BitMap();
        bitmaps_[c].init(max_row_num_, false);
    }
    return E_OK;
}

void Tablet::destroy() {
    if (timestamps_ != nullptr) {
        common::mem_free(timestamps_);
        timestamps_ = nullptr;
    }

    if (value_matrix_ != nullptr) {
        for (size_t c = 0; c < schema_vec_->size(); c++) {
            const MeasurementSchema& schema = schema_vec_->at(c);
            switch (schema.data_type_) {
                case DATE:
                case INT32:
                    common::mem_free(value_matrix_[c].int32_data);
                    break;
                case TIMESTAMP:
                case INT64:
                    common::mem_free(value_matrix_[c].int64_data);
                    break;
                case FLOAT:
                    common::mem_free(value_matrix_[c].float_data);
                    break;
                case DOUBLE:
                    common::mem_free(value_matrix_[c].double_data);
                    break;
                case BOOLEAN:
                    common::mem_free(value_matrix_[c].bool_data);
                    break;
                case BLOB:
                case TEXT:
                case STRING:
                    value_matrix_[c].string_col->destroy();
                    common::mem_free(value_matrix_[c].string_col);
                    break;
                default:
                    break;
            }
        }
        common::mem_free(value_matrix_);
        value_matrix_ = nullptr;
    }

    if (bitmaps_ != nullptr) {
        size_t schema_count = schema_vec_->size();
        for (size_t c = 0; c < schema_count; c++) {
            bitmaps_[c].~BitMap();
        }
        common::mem_free(bitmaps_);
        bitmaps_ = nullptr;
    }
}

int Tablet::add_timestamp(uint32_t row_index, int64_t timestamp) {
    if (err_code_ != E_OK) {
        return err_code_;
    }
    ASSERT(timestamps_ != NULL);
    if (UNLIKELY(row_index >= static_cast<uint32_t>(max_row_num_))) {
        ASSERT(false);
        return E_OUT_OF_RANGE;
    }
    timestamps_[row_index] = timestamp;
    cur_row_size_ = std::max(row_index + 1, cur_row_size_);

    return E_OK;
}

int Tablet::set_timestamps(const int64_t* timestamps, uint32_t count) {
    if (err_code_ != E_OK) {
        return err_code_;
    }
    ASSERT(timestamps_ != NULL);
    if (UNLIKELY(count > static_cast<uint32_t>(max_row_num_))) {
        return E_OUT_OF_RANGE;
    }
    std::memcpy(timestamps_, timestamps, count * sizeof(int64_t));
    cur_row_size_ = std::max(count, cur_row_size_);
    return E_OK;
}

int Tablet::set_column_values(uint32_t schema_index, const void* data,
                              const uint8_t* bitmap, uint32_t count) {
    if (err_code_ != E_OK) {
        return err_code_;
    }
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        return E_OUT_OF_RANGE;
    }
    if (UNLIKELY(count > static_cast<uint32_t>(max_row_num_))) {
        return E_OUT_OF_RANGE;
    }

    const MeasurementSchema& schema = schema_vec_->at(schema_index);
    size_t elem_size = 0;
    void* dst = nullptr;
    switch (schema.data_type_) {
        case BOOLEAN:
            elem_size = sizeof(bool);
            dst = value_matrix_[schema_index].bool_data;
            break;
        case DATE:
        case INT32:
            elem_size = sizeof(int32_t);
            dst = value_matrix_[schema_index].int32_data;
            break;
        case TIMESTAMP:
        case INT64:
            elem_size = sizeof(int64_t);
            dst = value_matrix_[schema_index].int64_data;
            break;
        case FLOAT:
            elem_size = sizeof(float);
            dst = value_matrix_[schema_index].float_data;
            break;
        case DOUBLE:
            elem_size = sizeof(double);
            dst = value_matrix_[schema_index].double_data;
            break;
        default:
            return E_TYPE_NOT_SUPPORTED;
    }

    std::memcpy(dst, data, count * elem_size);
    if (bitmap == nullptr) {
        bitmaps_[schema_index].clear_all();
    } else {
        char* tsfile_bm = bitmaps_[schema_index].get_bitmap();
        uint32_t bm_bytes = (count + 7) / 8;
        std::memcpy(tsfile_bm, bitmap, bm_bytes);
    }
    cur_row_size_ = std::max(count, cur_row_size_);
    return E_OK;
}

int Tablet::set_column_string_values(uint32_t schema_index,
                                     const int32_t* offsets, const char* data,
                                     const uint8_t* bitmap, uint32_t count) {
    if (err_code_ != E_OK) {
        return err_code_;
    }
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        return E_OUT_OF_RANGE;
    }
    if (UNLIKELY(count > static_cast<uint32_t>(max_row_num_))) {
        return E_OUT_OF_RANGE;
    }

    StringColumn* sc = value_matrix_[schema_index].string_col;
    if (sc == nullptr) {
        return E_INVALID_ARG;
    }

    uint32_t total_bytes = static_cast<uint32_t>(offsets[count]);
    if (total_bytes > sc->buf_capacity) {
        sc->buf_capacity = total_bytes;
        sc->buffer = (char*)mem_realloc(sc->buffer, sc->buf_capacity);
    }

    if (total_bytes > 0) {
        std::memcpy(sc->buffer, data, total_bytes);
    }
    std::memcpy(sc->offsets, offsets, (count + 1) * sizeof(int32_t));
    sc->buf_used = total_bytes;

    if (bitmap == nullptr) {
        bitmaps_[schema_index].clear_all();
    } else {
        char* tsfile_bm = bitmaps_[schema_index].get_bitmap();
        uint32_t bm_bytes = (count + 7) / 8;
        std::memcpy(tsfile_bm, bitmap, bm_bytes);
    }
    cur_row_size_ = std::max(count, cur_row_size_);
    return E_OK;
}

void* Tablet::get_value(int row_index, uint32_t schema_index,
                        common::TSDataType& data_type) const {
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
        case TEXT:
        case BLOB:
        case STRING: {
            return &column_values.string_col->get_string_view(row_index);
        }
        default:
            return nullptr;
    }
}

template <>
void Tablet::process_val(uint32_t row_index, uint32_t schema_index,
                         common::String str) {
    value_matrix_[schema_index].string_col->append(row_index, str.buf_,
                                                   str.len_);
    bitmaps_[schema_index].clear(row_index); /* mark as non-null */
}

template <typename T>
void Tablet::process_val(uint32_t row_index, uint32_t schema_index, T val) {
    switch (schema_vec_->at(schema_index).data_type_) {
        case common::BOOLEAN:
            (value_matrix_[schema_index].bool_data)[row_index] =
                static_cast<bool>(val);
            break;
        case common::DATE:
        case common::INT32:
            value_matrix_[schema_index].int32_data[row_index] =
                static_cast<int32_t>(val);
            break;
        case common::TIMESTAMP:
        case common::INT64:
            value_matrix_[schema_index].int64_data[row_index] =
                static_cast<int64_t>(val);
            break;
        case common::FLOAT:
            value_matrix_[schema_index].float_data[row_index] =
                static_cast<float>(val);
            break;
        case common::DOUBLE:
            value_matrix_[schema_index].double_data[row_index] =
                static_cast<double>(val);
            break;
        default:
            ASSERT(false);
    }
    bitmaps_[schema_index].clear(row_index); /* mark as non-null */
}

template <typename T>
int Tablet::add_value(uint32_t row_index, uint32_t schema_index, T val) {
    if (err_code_ != E_OK) {
        return err_code_;
    }
    int ret = common::E_OK;
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        ASSERT(false);
        ret = common::E_OUT_OF_RANGE;
    } else {
        const MeasurementSchema& schema = schema_vec_->at(schema_index);
        if (UNLIKELY(!TypeMatch<T>(schema.data_type_))) {
            return E_TYPE_NOT_MATCH;
        }
        process_val(row_index, schema_index, val);
    }
    return ret;
}

template <>
int Tablet::add_value(uint32_t row_index, uint32_t schema_index, std::tm val) {
    if (err_code_ != E_OK) {
        return err_code_;
    }
    int ret = common::E_OK;
    if (UNLIKELY(schema_index >= schema_vec_->size())) {
        ASSERT(false);
        ret = common::E_OUT_OF_RANGE;
    }
    int32_t date_int;
    if (RET_SUCC(common::DateConverter::date_to_int(val, date_int))) {
        process_val(row_index, schema_index, date_int);
    }
    return ret;
}

template <>
int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                      const char* val) {
    return add_value(row_index, schema_index, String(val));
}

template <typename T>
int Tablet::add_value(uint32_t row_index, const std::string& measurement_name,
                      T val) {
    int ret = common::E_OK;
    if (err_code_ != E_OK) {
        return err_code_;
    }
    SchemaMapIterator find_iter = schema_map_.find(to_lower(measurement_name));
    if (LIKELY(find_iter == schema_map_.end())) {
        ret = E_INVALID_ARG;
    } else {
        ret = add_value(row_index, find_iter->second, val);
    }
    return ret;
}

template <>
int Tablet::add_value(uint32_t row_index, const std::string& measurement_name,
                      const char* val) {
    return add_value(row_index, measurement_name, String(val));
}

template <>
int Tablet::add_value(uint32_t row_index, const std::string& measurement_name,
                      std::string val) {
    return add_value(row_index, measurement_name, String(val));
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
template int Tablet::add_value(uint32_t row_index, uint32_t schema_index,
                               String val);

template int Tablet::add_value(uint32_t row_index,
                               const std::string& measurement_name, bool val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string& measurement_name,
                               int32_t val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string& measurement_name,
                               int64_t val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string& measurement_name, float val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string& measurement_name, double val);
template int Tablet::add_value(uint32_t row_index,
                               const std::string& measurement_name, String val);

void Tablet::set_column_categories(
    const std::vector<ColumnCategory>& column_categories) {
    column_categories_ = column_categories;
    id_column_indexes_.clear();
    for (size_t i = 0; i < column_categories_.size(); i++) {
        ColumnCategory columnCategory = column_categories_[i];
        if (columnCategory == ColumnCategory::TAG) {
            id_column_indexes_.push_back(i);
        }
    }
}

void Tablet::reset_string_columns() {
    size_t schema_count = schema_vec_->size();
    for (size_t c = 0; c < schema_count; c++) {
        const MeasurementSchema& schema = schema_vec_->at(c);
        if (schema.data_type_ == STRING || schema.data_type_ == TEXT ||
            schema.data_type_ == BLOB) {
            value_matrix_[c].string_col->reset();
        }
    }
}

// Find all row indices where the device ID changes.  A device ID is the
// composite key formed by all id columns (e.g. region + sensor_id).  Row i
// is a boundary when at least one id column differs between row i-1 and row i.
//
// Example (2 id columns: region, sensor_id):
//   row 0: "A", "s1"
//   row 1: "A", "s2"  <- boundary: sensor_id changed
//   row 2: "B", "s1"  <- boundary: region changed
//   row 3: "B", "s1"
//   row 4: "B", "s2"  <- boundary: sensor_id changed
//   result: [1, 2, 4]
//
// Boundaries are computed in one shot at flush time rather than maintained
// incrementally during add_value / set_column_*. The total work is similar
// either way, but batch computation here is far more CPU-friendly: the inner
// loop is a tight memcmp scan over contiguous buffers with good cache
// locality, and the CPU can pipeline comparisons without the branch overhead
// and cache thrashing of per-row bookkeeping spread across the write path.
std::vector<uint32_t> Tablet::find_all_device_boundaries() const {
    const uint32_t row_count = get_cur_row_size();
    if (row_count <= 1) return {};

    const uint32_t nwords = (row_count + 63) / 64;
    std::vector<uint64_t> boundary(nwords, 0);

    uint32_t boundary_count = 0;
    const uint32_t max_boundaries = row_count - 1;
    for (auto it = id_column_indexes_.rbegin(); it != id_column_indexes_.rend();
         ++it) {
        const StringColumn& sc = *value_matrix_[*it].string_col;
        const int32_t* off = sc.offsets;
        const char* buf = sc.buffer;
        for (uint32_t i = 1; i < row_count; i++) {
            if (boundary[i >> 6] & (1ULL << (i & 63))) continue;
            int32_t len_a = off[i] - off[i - 1];
            int32_t len_b = off[i + 1] - off[i];
            if (len_a != len_b ||
                (len_a > 0 && memcmp(buf + off[i - 1], buf + off[i],
                                     static_cast<uint32_t>(len_a)) != 0)) {
                boundary[i >> 6] |= (1ULL << (i & 63));
                if (++boundary_count >= max_boundaries) break;
            }
        }
        if (boundary_count >= max_boundaries) break;
    }

    // Sweep the bitmap word by word, extracting set bit positions in order.
    // Each word covers 64 consecutive rows: word w covers rows [w*64, w*64+63].
    //
    // For each word we use two standard bit tricks:
    //   __builtin_ctzll(bits)  — count trailing zeros = index of lowest set bit
    //   bits &= bits - 1       — clear the lowest set bit
    //
    // Example: w=1, bits=0b...00010100 (bits 2 and 4 set)
    //   iter 1: ctzll=2 → idx=1*64+2=66, bits becomes 0b...00010000
    //   iter 2: ctzll=4 → idx=1*64+4=68, bits becomes 0b...00000000 → exit
    //
    // Guards: idx>0 because row 0 can never be a boundary (no predecessor);
    // idx<row_count trims padding bits in the last word when row_count%64 != 0.
    std::vector<uint32_t> result;
    for (uint32_t w = 0; w < nwords; w++) {
        uint64_t bits = boundary[w];
        while (bits) {
            uint32_t bit = __builtin_ctzll(bits);
            uint32_t idx = w * 64 + bit;
            if (idx > 0 && idx < row_count) {
                result.push_back(idx);
            }
            bits &= bits - 1;
        }
    }
    return result;
}

std::shared_ptr<IDeviceID> Tablet::get_device_id(int i) const {
    std::vector<std::string*> id_array;
    id_array.push_back(new std::string(insert_target_name_));
    for (auto id_column_idx : id_column_indexes_) {
        common::TSDataType data_type = INVALID_DATATYPE;
        void* value_ptr = get_value(i, id_column_idx, data_type);
        if (value_ptr == nullptr) {
            id_array.push_back(nullptr);
            continue;
        }
        common::String str;
        switch (data_type) {
            case STRING:
                str = *static_cast<common::String*>(value_ptr);
                if (str.buf_ == nullptr || str.len_ == 0) {
                    id_array.push_back(new std::string());
                } else {
                    id_array.push_back(new std::string(str.buf_, str.len_));
                }
                break;
            default:
                break;
        }
    }
    auto res = std::make_shared<StringArrayDeviceID>(id_array);
    for (auto& id : id_array) {
        if (id != nullptr) {
            delete id;
        }
    }
    return res;
}

}  // end namespace storage