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
#include "common/arrow_batch.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <new>

#include "common/container/bit_map.h"
#include "common/schema.h"
#include "utils/date_utils.h"

namespace common {
ArrowColumn::ArrowColumn(TSDataType type, uint32_t capacity)
    : type_(type),
      capacity_(capacity),
      validity_((uint64_t(capacity) + 7) / 8, 0) {
    if (variable()) {
        offsets_.resize(uint64_t(capacity) + 1, 0);
    } else {
        uint64_t size =
            type == BOOLEAN
                ? (uint64_t(capacity) + 7) / 8
                : uint64_t(capacity) *
                      (type == INVALID_DATATYPE ? 0 : get_data_type_size(type));
        values_.resize((size + 7) / 8, 0);
    }
}
bool ArrowColumn::variable() const {
    return type_ == STRING || type_ == TEXT || type_ == BLOB;
}
bool ArrowColumn::is_null(uint32_t row) const {
    return (validity_[row / 8] & (1U << (row % 8))) == 0;
}
void ArrowColumn::set_valid(uint32_t row) {
    validity_[row / 8] |= uint8_t(1U << (row % 8));
}
void ArrowColumn::set_valid_range(uint32_t begin, uint32_t count) {
    while (count && begin % 8) {
        set_valid(begin++);
        --count;
    }
    if (count >= 8) {
        std::memset(validity_.data() + begin / 8, 0xff, count / 8);
        begin += (count / 8) * 8;
        count %= 8;
    }
    while (count--) set_valid(begin++);
}
const char* ArrowColumn::native_value(uint32_t row, char* scratch,
                                      uint32_t& size) const {
    size = 0;
    if (is_null(row)) return nullptr;
    if (variable()) {
        size = offsets_[row + 1] - offsets_[row];
        return size ? bytes_.data() + offsets_[row] : "";
    }
    size = get_data_type_size(type_);
    if (type_ == BOOLEAN) {
        bool value =
            (reinterpret_cast<const uint8_t*>(values_.data())[row / 8] &
             (1U << (row % 8))) != 0;
        std::memcpy(scratch, &value, sizeof(value));
        return scratch;
    }
    const char* data = values() + uint64_t(row) * size;
    if (type_ == DATE) {
        int32_t days;
        std::memcpy(&days, data, sizeof(days));
        int32_t value = DaysSinceEpochToYYYYMMDD(days);
        std::memcpy(scratch, &value, sizeof(value));
        return scratch;
    }
    return data;
}
int ArrowColumn::set_native(uint32_t row, TSDataType source_type,
                            const char* data, uint32_t size) {
    if (row >= capacity_) return E_INVALID_ARG;
    if (variable()) {
        if (row < string_rows_) return E_INVALID_ARG;
        while (string_rows_ <= row) offsets_[string_rows_++] = bytes_.size();
        if (data != nullptr) {
            if (uint64_t(bytes_.size()) + size > INT32_MAX) return E_OVERFLOW;
            bytes_.insert(bytes_.end(), data, data + size);
            set_valid(row);
        }
        offsets_[row + 1] = bytes_.size();
        return E_OK;
    }
    if (data == nullptr) return E_OK;
    char* dest =
        type_ == BOOLEAN
            ? nullptr
            : mutable_values() + uint64_t(row) * get_data_type_size(type_);
    if (type_ == BOOLEAN) {
        if (size != sizeof(bool)) return E_INVALID_ARG;
        bool v;
        std::memcpy(&v, data, sizeof(v));
        if (v)
            reinterpret_cast<uint8_t*>(values_.data())[row / 8] |= 1U
                                                                   << (row % 8);
    } else if (type_ == DATE) {
        if (size != sizeof(int32_t)) return E_INVALID_ARG;
        int32_t v;
        std::memcpy(&v, data, sizeof(v));
        v = YYYYMMDDToDaysSinceEpoch(v);
        std::memcpy(dest, &v, sizeof(v));
    } else if (type_ == INT64 && source_type == INT32) {
        int32_t v;
        std::memcpy(&v, data, sizeof(v));
        int64_t promoted = v;
        std::memcpy(dest, &promoted, sizeof(promoted));
    } else if (type_ == DOUBLE && source_type == FLOAT) {
        float v;
        std::memcpy(&v, data, sizeof(v));
        double promoted = v;
        std::memcpy(dest, &promoted, sizeof(promoted));
    } else {
        if (size != get_data_type_size(type_)) return E_TYPE_NOT_SUPPORTED;
        std::memcpy(dest, data, size);
    }
    set_valid(row);
    return E_OK;
}
int ArrowColumn::copy_selected(const ArrowColumn& source,
                               const std::vector<uint32_t>& rows,
                               uint32_t begin, uint32_t count,
                               uint32_t destination) {
    if (uint64_t(begin) + count > rows.size() ||
        uint64_t(destination) + count > capacity_)
        return E_INVALID_ARG;
    uint32_t n = 0;
    while (n < count) {
        uint32_t row = rows[begin + n];
        if (row >= source.capacity()) return E_INVALID_ARG;
        uint32_t run = 1;
        if (!variable() && type_ != BOOLEAN && type_ == source.type_ &&
            !source.is_null(row)) {
            while (n + run < count && rows[begin + n + run] == row + run &&
                   row + run < source.capacity() && !source.is_null(row + run))
                ++run;
            uint32_t width = get_data_type_size(type_);
            std::memcpy(mutable_values() + uint64_t(destination + n) * width,
                        source.values() + uint64_t(row) * width,
                        uint64_t(run) * width);
            set_valid_range(destination + n, run);
        } else {
            char scratch[8];
            uint32_t size;
            const char* data = source.native_value(row, scratch, size);
            int ret = set_native(destination + n, source.type_, data, size);
            if (ret != E_OK) return ret;
        }
        n += run;
    }
    return E_OK;
}
void ArrowColumn::finish(uint32_t rows) {
    if (variable())
        while (string_rows_ <= rows) offsets_[string_rows_++] = bytes_.size();
}
ArrowBatch::ArrowBatch(const std::vector<std::string>& column_names,
                       const std::vector<TSDataType>& types, uint32_t capacity)
    : names(column_names) {
    for (auto type : types)
        columns.emplace_back(new ArrowColumn(type, capacity));
}
void ArrowBatch::finish() {
    for (auto& col : columns) col->finish(row_count);
}
namespace {
const char* format(TSDataType type) {
    switch (type) {
        case BOOLEAN:
            return "b";
        case INT32:
            return "i";
        case INT64:
            return "l";
        case FLOAT:
            return "f";
        case DOUBLE:
            return "g";
        case DATE:
            return "tdD";
        case TIMESTAMP:
            return "tsn:";
        case TEXT:
        case STRING:
            return "u";
        case BLOB:
            return "z";
        default:
            return "n";
    }
}
struct ArrayOwner {
    std::shared_ptr<ArrowBatch> batch;
    const void* buffers[3] = {};
    std::vector<ArrowArray*> children;
    ~ArrayOwner() {
        for (auto* child : children) {
            if (child->release) child->release(child);
            delete child;
        }
    }
};
struct SchemaOwner {
    std::string name;
    std::vector<ArrowSchema*> children;
    ~SchemaOwner() {
        for (auto* child : children) {
            if (child->release) child->release(child);
            delete child;
        }
    }
};
void release_array(ArrowArray* array) {
    array->release = nullptr;
    delete static_cast<ArrayOwner*>(array->private_data);
    array->private_data = nullptr;
}
void release_schema(ArrowSchema* schema) {
    schema->release = nullptr;
    delete static_cast<SchemaOwner*>(schema->private_data);
    schema->private_data = nullptr;
}
void init_array(ArrowArray& array, const std::shared_ptr<ArrowBatch>& batch) {
    auto* owner = new ArrayOwner;
    owner->batch = batch;
    array = {};
    array.private_data = owner;
    array.release = release_array;
    array.length = batch->row_count;
}
void init_schema(ArrowSchema& schema, const std::string& name,
                 const char* fmt) {
    std::unique_ptr<SchemaOwner> owner(new SchemaOwner);
    owner->name = name;
    schema = {};
    schema.name = owner->name.c_str();
    schema.format = fmt;
    schema.flags = 2;
    schema.release = release_schema;
    schema.private_data = owner.release();
}
}  // namespace
int ArrowBatch::export_to(ArrowArray* array, ArrowSchema* schema) {
    if (!array) return E_INVALID_ARG;
    *array = {};
    if (schema) *schema = {};
    try {
        finish();
        init_array(*array, shared_from_this());
        if (schema) init_schema(*schema, "", "+s");
        auto* a = static_cast<ArrayOwner*>(array->private_data);
        auto* s =
            schema ? static_cast<SchemaOwner*>(schema->private_data) : nullptr;
        a->children.reserve(columns.size());
        if (s) s->children.reserve(columns.size());
        for (size_t i = 0; i < columns.size(); ++i) {
            std::unique_ptr<ArrowArray> child(new ArrowArray{});
            a->children.push_back(child.get());
            child.release();
            if (s) {
                std::unique_ptr<ArrowSchema> cs(new ArrowSchema{});
                s->children.push_back(cs.get());
                cs.release();
            }
            init_array(*a->children.back(), shared_from_this());
            if (s)
                init_schema(*s->children.back(), names[i],
                            format(columns[i]->type()));
            auto& col = *columns[i];
            ArrowArray& out = *a->children.back();
            auto* ca = static_cast<ArrayOwner*>(out.private_data);
            if (col.type() != INVALID_DATATYPE)
                ca->buffers[out.n_buffers++] = col.validity();
            if (col.variable()) {
                ca->buffers[out.n_buffers++] = col.offsets();
                ca->buffers[out.n_buffers++] = col.bytes();
            } else if (col.type() != INVALID_DATATYPE)
                ca->buffers[out.n_buffers++] = col.values();
            out.null_count =
                row_count - bitops::count_valid_bits(col.validity(), row_count);
            out.buffers = ca->buffers;
        }
        array->n_buffers = 1;
        array->buffers = a->buffers;
        array->n_children = a->children.size();
        array->children = a->children.data();
        if (schema) {
            schema->n_children = s->children.size();
            schema->children = s->children.data();
        }
        return E_OK;
    } catch (const std::bad_alloc&) {
        if (array->release) array->release(array);
        if (schema && schema->release) schema->release(schema);
        return E_OOM;
    }
}
int ArrowBatch::to_tsblock(TupleDesc& desc, TsBlock*& block,
                           storage::TableSchema* table_schema) const {
    delete block;
    block = nullptr;
    desc.reset();
    for (size_t c = 0; c < columns.size(); ++c) {
        if (table_schema) {
            desc.push_back(c == 0 ? g_time_column_schema
                                  : table_schema->get_column_schema(names[c]));
        } else {
            desc.push_back(ColumnSchema(
                names[c], columns[c]->type(),
                c == 0 ? ColumnCategory::TIME : ColumnCategory::FIELD));
        }
    }
    int ret = TsBlock::create_tsblock(&desc, block, row_count);
    if (ret != E_OK) return ret;
    RowAppender appender(block);
    for (uint32_t r = 0; r < row_count; ++r) {
        appender.add_row();
        for (size_t c = 0; c < columns.size(); ++c) {
            char scratch[8];
            uint32_t size;
            const char* data = columns[c]->native_value(r, scratch, size);
            if (data)
                appender.append(c, data, size);
            else
                appender.append_null(c);
        }
    }
    for (uint32_t c = 0; c < columns.size(); ++c)
        block->get_vector(c)->add_row_nums(row_count);
    block->fill_trailling_nulls();
    return E_OK;
}
}  // namespace common
