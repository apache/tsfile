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

#include <cstring>
#include <ctime>
#include <type_traits>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/tsblock/tsblock.h"
#include "common/tsblock/tuple_desc.h"
#include "common/tsblock/vector/vector.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "utils/errno_define.h"

namespace arrow {

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowArrayData {
    void** buffers;
    size_t n_buffers;
};

struct ArrowSchemaData {
    std::vector<std::string>* format_strings;
    std::vector<std::string>* name_strings;
    ArrowSchema** children;
    size_t n_children;
};

struct StructArrayData {
    ArrowArray** children;
    size_t n_children;
};

static const char* GetArrowFormatString(common::TSDataType datatype) {
    switch (datatype) {
        case common::BOOLEAN:
            return "b";
        case common::INT32:
            return "i";
        case common::INT64:
            return "l";
        case common::TIMESTAMP:  // nanosecond, no timezone
            return "tsn:";
        case common::FLOAT:
            return "f";
        case common::DOUBLE:
            return "g";
        case common::TEXT:
        case common::STRING:
            return "u";
        case common::DATE:
            return "tdD";  // date32: days since Unix epoch, stored as int32
        default:
            return nullptr;
    }
}

static inline size_t GetNullBitmapSize(int64_t length) {
    return (length + 7) / 8;
}

static void ReleaseArrowArray(ArrowArray* array) {
    if (array == nullptr || array->private_data == nullptr) {
        return;
    }
    ArrowArrayData* data = static_cast<ArrowArrayData*>(array->private_data);
    if (data->buffers != nullptr) {
        for (size_t i = 0; i < data->n_buffers; ++i) {
            if (data->buffers[i] != nullptr) {
                common::mem_free(data->buffers[i]);
            }
        }
        common::mem_free(data->buffers);
    }
    common::mem_free(data);

    array->length = 0;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 0;
    array->n_children = 0;
    array->buffers = nullptr;
    array->children = nullptr;
    array->dictionary = nullptr;
    array->release = nullptr;
    array->private_data = nullptr;
}

static void ReleaseStructArrowArray(ArrowArray* array) {
    if (array == nullptr || array->private_data == nullptr) {
        return;
    }
    StructArrayData* data = static_cast<StructArrayData*>(array->private_data);
    if (data->children != nullptr) {
        for (size_t i = 0; i < data->n_children; ++i) {
            if (data->children[i] != nullptr) {
                if (data->children[i]->release != nullptr) {
                    data->children[i]->release(data->children[i]);
                }
                common::mem_free(data->children[i]);
            }
        }
        common::mem_free(data->children);
    }
    delete data;

    array->length = 0;
    array->null_count = 0;
    array->offset = 0;
    array->n_buffers = 0;
    array->n_children = 0;
    array->buffers = nullptr;
    array->children = nullptr;
    array->dictionary = nullptr;
    array->release = nullptr;
    array->private_data = nullptr;
}

static void ReleaseArrowSchema(ArrowSchema* schema) {
    if (schema == nullptr || schema->private_data == nullptr) {
        return;
    }
    ArrowSchemaData* data = static_cast<ArrowSchemaData*>(schema->private_data);

    // Release children schemas first
    if (data->children != nullptr) {
        for (size_t i = 0; i < data->n_children; ++i) {
            if (data->children[i] != nullptr) {
                if (data->children[i]->release != nullptr) {
                    data->children[i]->release(data->children[i]);
                }
                common::mem_free(data->children[i]);
            }
        }
        common::mem_free(data->children);
    }

    // Release string storage
    if (data->format_strings != nullptr) {
        delete data->format_strings;
    }
    if (data->name_strings != nullptr) {
        delete data->name_strings;
    }

    delete data;

    schema->format = nullptr;
    schema->name = nullptr;
    schema->metadata = nullptr;
    schema->flags = 0;
    schema->n_children = 0;
    schema->children = nullptr;
    schema->dictionary = nullptr;
    schema->release = nullptr;
    schema->private_data = nullptr;
}

template <typename CType>
inline int BuildFixedLengthArrowArrayC(common::Vector* vec, uint32_t row_count,
                                       ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    bool has_null = vec->has_null();
    size_t type_size = sizeof(CType);
    // Arrow C Data Interface: fixed-width types always have 2 buffers
    // buffers[0] = validity bitmap (may be NULL if no nulls)
    // buffers[1] = values
    static constexpr int64_t n_buffers = 2;

    ArrowArrayData* array_data = static_cast<ArrowArrayData*>(
        common::mem_alloc(sizeof(ArrowArrayData), common::MOD_TSBLOCK));
    if (array_data == nullptr) {
        return common::E_OOM;
    }

    array_data->n_buffers = n_buffers;
    array_data->buffers = static_cast<void**>(
        common::mem_alloc(n_buffers * sizeof(void*), common::MOD_TSBLOCK));
    if (array_data->buffers == nullptr) {
        common::mem_free(array_data);
        return common::E_OOM;
    }

    for (int64_t i = 0; i < n_buffers; ++i) {
        array_data->buffers[i] = nullptr;
    }

    uint8_t* null_bitmap = nullptr;
    if (has_null) {
        size_t null_bitmap_size = GetNullBitmapSize(row_count);
        null_bitmap = static_cast<uint8_t*>(
            common::mem_alloc(null_bitmap_size, common::MOD_TSBLOCK));
        if (null_bitmap == nullptr) {
            common::mem_free(array_data->buffers);
            common::mem_free(array_data);
            return common::E_OOM;
        }
        common::BitMap& vec_bitmap = vec->get_bitmap();
        char* vec_bitmap_data = vec_bitmap.get_bitmap();
        for (size_t i = 0; i < null_bitmap_size; ++i) {
            null_bitmap[i] = ~static_cast<uint8_t>(vec_bitmap_data[i]);
        }
        array_data->buffers[0] = null_bitmap;

        int64_t null_count = 0;
        for (uint32_t i = 0; i < row_count; ++i) {
            if (vec_bitmap.test(i)) {
                null_count++;
            }
        }
        out_array->null_count = null_count;
    } else {
        array_data->buffers[0] = nullptr;
        out_array->null_count = 0;
    }

    char* vec_data = vec->get_value_data().get_data();
    void* data_buffer = nullptr;

    if (std::is_same<CType, bool>::value) {
        size_t packed_size = GetNullBitmapSize(row_count);
        uint8_t* packed_buffer = static_cast<uint8_t*>(
            common::mem_alloc(packed_size, common::MOD_TSBLOCK));
        if (packed_buffer == nullptr) {
            if (null_bitmap != nullptr) {
                common::mem_free(null_bitmap);
            }
            common::mem_free(array_data->buffers);
            common::mem_free(array_data);
            return common::E_OOM;
        }

        std::memset(packed_buffer, 0, packed_size);

        const uint8_t* src = reinterpret_cast<const uint8_t*>(vec_data);
        for (uint32_t i = 0; i < row_count; ++i) {
            if (src[i] != 0) {
                uint32_t byte_idx = i / 8;
                uint32_t bit_idx = i % 8;
                packed_buffer[byte_idx] |= (1 << bit_idx);
            }
        }

        data_buffer = packed_buffer;
    } else {
        size_t data_size = type_size * row_count;
        data_buffer = common::mem_alloc(data_size, common::MOD_TSBLOCK);
        if (data_buffer == nullptr) {
            if (null_bitmap != nullptr) {
                common::mem_free(null_bitmap);
            }
            common::mem_free(array_data->buffers);
            common::mem_free(array_data);
            return common::E_OOM;
        }
        std::memcpy(data_buffer, vec_data, data_size);
    }

    array_data->buffers[1] = data_buffer;

    out_array->length = row_count;
    out_array->offset = 0;
    out_array->n_buffers = n_buffers;
    out_array->n_children = 0;
    out_array->buffers = const_cast<const void**>(array_data->buffers);
    out_array->children = nullptr;
    out_array->dictionary = nullptr;
    out_array->release = ReleaseArrowArray;
    out_array->private_data = array_data;

    return common::E_OK;
}

static int BuildStringArrowArrayC(common::Vector* vec, uint32_t row_count,
                                  ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    bool has_null = vec->has_null();
    int64_t n_buffers = 3;
    ArrowArrayData* array_data = static_cast<ArrowArrayData*>(
        common::mem_alloc(sizeof(ArrowArrayData), common::MOD_TSBLOCK));
    if (array_data == nullptr) {
        return common::E_OOM;
    }

    array_data->n_buffers = n_buffers;
    array_data->buffers = static_cast<void**>(
        common::mem_alloc(n_buffers * sizeof(void*), common::MOD_TSBLOCK));
    if (array_data->buffers == nullptr) {
        common::mem_free(array_data);
        return common::E_OOM;
    }

    for (int64_t i = 0; i < n_buffers; ++i) {
        array_data->buffers[i] = nullptr;
    }

    uint8_t* null_bitmap = nullptr;
    if (has_null) {
        size_t null_bitmap_size = GetNullBitmapSize(row_count);
        null_bitmap = static_cast<uint8_t*>(
            common::mem_alloc(null_bitmap_size, common::MOD_TSBLOCK));
        if (null_bitmap == nullptr) {
            common::mem_free(array_data->buffers);
            common::mem_free(array_data);
            return common::E_OOM;
        }
        common::BitMap& vec_bitmap = vec->get_bitmap();
        char* vec_bitmap_data = vec_bitmap.get_bitmap();
        for (size_t i = 0; i < null_bitmap_size; ++i) {
            null_bitmap[i] = ~static_cast<uint8_t>(vec_bitmap_data[i]);
        }
        array_data->buffers[0] = null_bitmap;

        int64_t null_count = 0;
        for (uint32_t i = 0; i < row_count; ++i) {
            if (vec_bitmap.test(i)) {
                null_count++;
            }
        }
        out_array->null_count = null_count;
    } else {
        array_data->buffers[0] = nullptr;
        out_array->null_count = 0;
    }
    size_t offsets_size = sizeof(int32_t) * (row_count + 1);
    int32_t* offsets = static_cast<int32_t*>(
        common::mem_alloc(offsets_size, common::MOD_TSBLOCK));
    if (offsets == nullptr) {
        if (null_bitmap != nullptr) {
            common::mem_free(null_bitmap);
        }
        common::mem_free(array_data->buffers);
        common::mem_free(array_data);
        return common::E_OOM;
    }

    offsets[0] = 0;
    uint32_t current_offset = 0;
    char* vec_data = vec->get_value_data().get_data();
    uint32_t vec_offset = 0;

    // 获取 vec_bitmap 用于后续检查
    common::BitMap& vec_bitmap = vec->get_bitmap();

    for (uint32_t i = 0; i < row_count; ++i) {
        if (has_null && vec_bitmap.test(i)) {
            offsets[i + 1] = current_offset;
        } else {
            uint32_t len = 0;
            std::memcpy(&len, vec_data + vec_offset, sizeof(uint32_t));
            vec_offset += sizeof(uint32_t);

            current_offset += len;
            offsets[i + 1] = current_offset;
            vec_offset += len;
        }
    }

    array_data->buffers[1] = offsets;

    size_t data_size = current_offset;
    uint8_t* data_buffer = static_cast<uint8_t*>(
        common::mem_alloc(data_size, common::MOD_TSBLOCK));
    if (data_buffer == nullptr) {
        if (null_bitmap != nullptr) {
            common::mem_free(null_bitmap);
        }
        common::mem_free(offsets);
        common::mem_free(array_data->buffers);
        common::mem_free(array_data);
        return common::E_OOM;
    }

    vec_offset = 0;
    uint32_t data_offset = 0;
    for (uint32_t i = 0; i < row_count; ++i) {
        if (!has_null || !vec_bitmap.test(i)) {
            uint32_t len = 0;
            std::memcpy(&len, vec_data + vec_offset, sizeof(uint32_t));
            vec_offset += sizeof(uint32_t);

            if (len > 0) {
                std::memcpy(data_buffer + data_offset, vec_data + vec_offset,
                            len);
                data_offset += len;
            }
            vec_offset += len;
        }
    }

    array_data->buffers[2] = data_buffer;

    out_array->length = row_count;
    out_array->offset = 0;
    out_array->n_buffers = n_buffers;
    out_array->n_children = 0;
    out_array->buffers = const_cast<const void**>(array_data->buffers);
    out_array->children = nullptr;
    out_array->dictionary = nullptr;
    out_array->release = ReleaseArrowArray;
    out_array->private_data = array_data;

    return common::E_OK;
}

// Convert TsFile YYYYMMDD integer to days since Unix epoch (1970-01-01)
static int32_t YYYYMMDDToDaysSinceEpoch(int32_t yyyymmdd) {
    int year = yyyymmdd / 10000;
    int month = (yyyymmdd % 10000) / 100;
    int day = yyyymmdd % 100;

    std::tm date = {};
    date.tm_year = year - 1900;
    date.tm_mon = month - 1;
    date.tm_mday = day;
    date.tm_hour = 12;
    date.tm_isdst = -1;

    std::tm epoch = {};
    epoch.tm_year = 70;
    epoch.tm_mon = 0;
    epoch.tm_mday = 1;
    epoch.tm_hour = 12;
    epoch.tm_isdst = -1;

    time_t t1 = mktime(&date);
    time_t t2 = mktime(&epoch);
    return static_cast<int32_t>((t1 - t2) / (60 * 60 * 24));
}

static int BuildDateArrowArrayC(common::Vector* vec, uint32_t row_count,
                                ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    bool has_null = vec->has_null();
    static constexpr int64_t n_buffers = 2;

    ArrowArrayData* array_data = static_cast<ArrowArrayData*>(
        common::mem_alloc(sizeof(ArrowArrayData), common::MOD_TSBLOCK));
    if (array_data == nullptr) return common::E_OOM;

    array_data->n_buffers = n_buffers;
    array_data->buffers = static_cast<void**>(
        common::mem_alloc(n_buffers * sizeof(void*), common::MOD_TSBLOCK));
    if (array_data->buffers == nullptr) {
        common::mem_free(array_data);
        return common::E_OOM;
    }
    for (int64_t i = 0; i < n_buffers; ++i) array_data->buffers[i] = nullptr;

    common::BitMap& vec_bitmap = vec->get_bitmap();
    uint8_t* null_bitmap = nullptr;
    if (has_null) {
        size_t null_bitmap_size = GetNullBitmapSize(row_count);
        null_bitmap = static_cast<uint8_t*>(
            common::mem_alloc(null_bitmap_size, common::MOD_TSBLOCK));
        if (null_bitmap == nullptr) {
            common::mem_free(array_data->buffers);
            common::mem_free(array_data);
            return common::E_OOM;
        }
        char* vec_bitmap_data = vec_bitmap.get_bitmap();
        for (size_t i = 0; i < null_bitmap_size; ++i) {
            null_bitmap[i] = ~static_cast<uint8_t>(vec_bitmap_data[i]);
        }
        int64_t null_count = 0;
        for (uint32_t i = 0; i < row_count; ++i) {
            if (vec_bitmap.test(i)) null_count++;
        }
        out_array->null_count = null_count;
        array_data->buffers[0] = null_bitmap;
    } else {
        out_array->null_count = 0;
        array_data->buffers[0] = nullptr;
    }

    int32_t* data_buffer = static_cast<int32_t*>(
        common::mem_alloc(sizeof(int32_t) * row_count, common::MOD_TSBLOCK));
    if (data_buffer == nullptr) {
        if (null_bitmap) common::mem_free(null_bitmap);
        common::mem_free(array_data->buffers);
        common::mem_free(array_data);
        return common::E_OOM;
    }

    char* vec_data = vec->get_value_data().get_data();
    for (uint32_t i = 0; i < row_count; ++i) {
        if (has_null && vec_bitmap.test(i)) {
            data_buffer[i] = 0;
        } else {
            int32_t yyyymmdd = 0;
            std::memcpy(&yyyymmdd, vec_data + i * sizeof(int32_t),
                        sizeof(int32_t));
            data_buffer[i] = YYYYMMDDToDaysSinceEpoch(yyyymmdd);
        }
    }

    array_data->buffers[1] = data_buffer;
    out_array->length = row_count;
    out_array->offset = 0;
    out_array->n_buffers = n_buffers;
    out_array->n_children = 0;
    out_array->buffers = const_cast<const void**>(array_data->buffers);
    out_array->children = nullptr;
    out_array->dictionary = nullptr;
    out_array->release = ReleaseArrowArray;
    out_array->private_data = array_data;
    return common::E_OK;
}

// Helper function to build ArrowArray for a single column
static int BuildColumnArrowArray(common::Vector* vec, uint32_t row_count,
                                 ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    common::TSDataType data_type = vec->get_vector_type();
    const char* format = GetArrowFormatString(data_type);
    if (format == nullptr) {
        return common::E_TYPE_NOT_SUPPORTED;
    }

    int ret = common::E_OK;
    switch (data_type) {
        case common::BOOLEAN:
            ret = BuildFixedLengthArrowArrayC<bool>(vec, row_count, out_array);
            break;
        case common::INT32:
            ret =
                BuildFixedLengthArrowArrayC<int32_t>(vec, row_count, out_array);
            break;
        case common::DATE:
            ret = BuildDateArrowArrayC(vec, row_count, out_array);
            break;
        case common::INT64:
        case common::TIMESTAMP:
            ret =
                BuildFixedLengthArrowArrayC<int64_t>(vec, row_count, out_array);
            break;
        case common::FLOAT:
            ret = BuildFixedLengthArrowArrayC<float>(vec, row_count, out_array);
            break;
        case common::DOUBLE:
            ret =
                BuildFixedLengthArrowArrayC<double>(vec, row_count, out_array);
            break;
        case common::TEXT:
        case common::STRING:
            ret = BuildStringArrowArrayC(vec, row_count, out_array);
            break;
        default:
            return common::E_TYPE_NOT_SUPPORTED;
    }
    return ret;
}

// Build ArrowSchema for a single column
static int BuildColumnArrowSchema(common::TSDataType data_type,
                                  const std::string& column_name,
                                  ArrowSchema* out_schema) {
    if (out_schema == nullptr) {
        return common::E_INVALID_ARG;
    }

    const char* format = GetArrowFormatString(data_type);
    if (format == nullptr) {
        return common::E_TYPE_NOT_SUPPORTED;
    }

    ArrowSchemaData* schema_data = new ArrowSchemaData();
    schema_data->format_strings = new std::vector<std::string>();
    schema_data->name_strings = new std::vector<std::string>();
    schema_data->children = nullptr;
    schema_data->n_children = 0;

    schema_data->format_strings->push_back(format);
    schema_data->name_strings->push_back(column_name);

    out_schema->format = schema_data->format_strings->back().c_str();
    out_schema->name = schema_data->name_strings->back().c_str();
    out_schema->metadata = nullptr;
    out_schema->flags = ARROW_FLAG_NULLABLE;
    out_schema->n_children = 0;
    out_schema->children = nullptr;
    out_schema->dictionary = nullptr;
    out_schema->release = ReleaseArrowSchema;
    out_schema->private_data = schema_data;

    return common::E_OK;
}

int TsBlockToArrowStruct(common::TsBlock& tsblock, ArrowArray* out_array,
                         ArrowSchema* out_schema) {
    if (out_array == nullptr || out_schema == nullptr) {
        return common::E_INVALID_ARG;
    }

    uint32_t row_count = tsblock.get_row_count();
    uint32_t column_count = tsblock.get_column_count();
    common::TupleDesc* tuple_desc = tsblock.get_tuple_desc();

    if (row_count == 0 || column_count == 0) {
        return common::E_INVALID_ARG;
    }

    // Build ArrowSchema for struct type
    ArrowSchemaData* schema_data = new ArrowSchemaData();
    schema_data->format_strings = new std::vector<std::string>();
    schema_data->name_strings = new std::vector<std::string>();
    schema_data->n_children = column_count;
    schema_data->children = static_cast<ArrowSchema**>(common::mem_alloc(
        column_count * sizeof(ArrowSchema*), common::MOD_TSBLOCK));
    if (schema_data->children == nullptr) {
        delete schema_data->format_strings;
        delete schema_data->name_strings;
        delete schema_data;
        return common::E_OOM;
    }

    // Store format string for struct type
    schema_data->format_strings->push_back("+s");
    schema_data->name_strings->push_back("");

    // Build schema for each column
    for (uint32_t i = 0; i < column_count; ++i) {
        schema_data->children[i] = static_cast<ArrowSchema*>(
            common::mem_alloc(sizeof(ArrowSchema), common::MOD_TSBLOCK));
        if (schema_data->children[i] == nullptr) {
            for (uint32_t j = 0; j < i; ++j) {
                if (schema_data->children[j] != nullptr &&
                    schema_data->children[j]->release != nullptr) {
                    schema_data->children[j]->release(schema_data->children[j]);
                }
            }
            common::mem_free(schema_data->children);
            delete schema_data->format_strings;
            delete schema_data->name_strings;
            delete schema_data;
            return common::E_OOM;
        }

        common::TSDataType col_type = tuple_desc->get_column_type(i);
        std::string col_name = tuple_desc->get_column_name(i);

        int ret = BuildColumnArrowSchema(col_type, col_name,
                                         schema_data->children[i]);
        if (ret != common::E_OK) {
            for (uint32_t j = 0; j <= i; ++j) {
                if (schema_data->children[j] != nullptr &&
                    schema_data->children[j]->release != nullptr) {
                    schema_data->children[j]->release(schema_data->children[j]);
                }
            }
            common::mem_free(schema_data->children);
            delete schema_data->format_strings;
            delete schema_data->name_strings;
            delete schema_data;
            return ret;
        }
    }

    out_schema->format = schema_data->format_strings->at(0).c_str();
    out_schema->name = schema_data->name_strings->at(0).c_str();
    out_schema->metadata = nullptr;
    out_schema->flags = 0;
    out_schema->n_children = column_count;
    out_schema->children = schema_data->children;
    out_schema->dictionary = nullptr;
    out_schema->release = ReleaseArrowSchema;
    out_schema->private_data = schema_data;

    ArrowArray** children_arrays = static_cast<ArrowArray**>(common::mem_alloc(
        column_count * sizeof(ArrowArray*), common::MOD_TSBLOCK));
    if (children_arrays == nullptr) {
        ReleaseArrowSchema(out_schema);
        return common::E_OOM;
    }

    for (uint32_t i = 0; i < column_count; ++i) {
        children_arrays[i] = static_cast<ArrowArray*>(
            common::mem_alloc(sizeof(ArrowArray), common::MOD_TSBLOCK));
        if (children_arrays[i] == nullptr) {
            for (uint32_t j = 0; j < i; ++j) {
                if (children_arrays[j] != nullptr &&
                    children_arrays[j]->release != nullptr) {
                    children_arrays[j]->release(children_arrays[j]);
                }
            }
            common::mem_free(children_arrays);
            ReleaseArrowSchema(out_schema);
            return common::E_OOM;
        }

        common::Vector* vec = tsblock.get_vector(i);
        int ret = BuildColumnArrowArray(vec, row_count, children_arrays[i]);
        if (ret != common::E_OK) {
            for (uint32_t j = 0; j <= i; ++j) {
                if (children_arrays[j] != nullptr &&
                    children_arrays[j]->release != nullptr) {
                    children_arrays[j]->release(children_arrays[j]);
                }
            }
            common::mem_free(children_arrays);
            ReleaseArrowSchema(out_schema);
            return ret;
        }
    }

    StructArrayData* struct_data = new StructArrayData();
    struct_data->children = children_arrays;
    struct_data->n_children = column_count;

    // Arrow C Data Interface: struct type requires n_buffers = 1 (validity
    // bitmap) buffers[0] may be NULL if there are no nulls at the struct level
    static const void* struct_buffers[1] = {nullptr};

    out_array->length = row_count;
    out_array->null_count = 0;  // struct itself is never null
    out_array->offset = 0;
    out_array->n_buffers = 1;
    out_array->n_children = column_count;
    out_array->buffers = struct_buffers;
    out_array->children = children_arrays;
    out_array->dictionary = nullptr;
    out_array->release = ReleaseStructArrowArray;
    out_array->private_data = struct_data;

    return common::E_OK;
}

}  // namespace arrow
