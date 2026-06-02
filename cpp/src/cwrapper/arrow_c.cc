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
#include "common/tablet.h"
#include "common/tsblock/tsblock.h"
#include "common/tsblock/tuple_desc.h"
#include "common/tsblock/vector/vector.h"
#include "cwrapper/tsfile_cwrapper.h"
#include "utils/date_utils.h"
#include "utils/errno_define.h"

namespace arrow {

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

// Arrow C Data Interface: a table is represented as a paired ArrowSchema +
// ArrowArray (struct type). The schema describes the column headers, and the
// struct array holds per-column data arrays.
//
//  ArrowSchema ("+s")          ArrowArray (struct, length=N)
//  ┌──────────────────┐        ┌──────────────────────────────────────┐
//  │ children[0]:     │        │ children[0]:    ArrowArray (time)    │
//  │  name="time"     │        │  buffers[0] = null bitmap           │
//  │  format="tsn:"   │        │  buffers[1] = [t0, t1, t2, ...]    │
//  ├──────────────────┤        ├──────────────────────────────────────┤
//  │ children[1]:     │        │ children[1]:    ArrowArray (col_a)  │
//  │  name="col_a"    │        │  buffers[0] = null bitmap           │
//  │  format="i"      │        │  buffers[1] = [10, 20, NULL, ...]  │
//  ├──────────────────┤        ├──────────────────────────────────────┤
//  │ children[2]:     │        │ children[2]:    ArrowArray (col_b)  │
//  │  name="col_b"    │        │  buffers[0] = null bitmap           │
//  │  format="g"      │        │  buffers[1] = [1.1, 2.2, 3.3, ...] │
//  └──────────────────┘        └──────────────────────────────────────┘
//       (table header)                    (table data)
//
// Memory ownership: each ArrowArray/ArrowSchema stores a private_data pointer
// to a producer-owned struct (ArrowArrayData / ArrowSchemaData /
// StructArrayData) that holds the actual allocated memory. The release()
// callback frees it. This design allows safe cross-library transfer (e.g. to
// PyArrow).

// Owns the buffers array and each buffer pointer within it.
// Stored in ArrowArray.private_data; freed by ReleaseArrowArray.
struct ArrowArrayData {
    void** buffers;
    size_t n_buffers;
};

// Owns format/name strings and children schemas.
// Stored in ArrowSchema.private_data; freed by ReleaseArrowSchema.
struct ArrowSchemaData {
    std::string format_string;
    std::string name_string;
    ArrowSchema** children;
    size_t n_children;
};

// Owns children arrays for struct-type ArrowArray.
// Stored in ArrowArray.private_data; freed by ReleaseStructArrowArray.
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
        case common::BLOB:
            return "z";
        case common::DATE:
            return "tdD";  // date32: days since Unix epoch, stored as int32
        default:
            return nullptr;
    }
}

static size_t GetNullBitmapSize(int64_t length) { return (length + 7) / 8; }

// Build Arrow validity bitmap from TsFile Vector and store it in
// array_data->buffers[0]. Sets out_array->null_count.
// Returns E_OK on success, E_OOM on allocation failure.
static int BuildNullBitmap(common::Vector* vec, uint32_t row_count,
                           ArrowArrayData* array_data, ArrowArray* out_array) {
    if (!vec->has_null()) {
        array_data->buffers[0] = nullptr;
        out_array->null_count = 0;
        return common::E_OK;
    }
    size_t null_bitmap_size = GetNullBitmapSize(row_count);
    uint8_t* null_bitmap = static_cast<uint8_t*>(
        common::mem_alloc(null_bitmap_size, common::MOD_TSBLOCK));
    if (null_bitmap == nullptr) {
        return common::E_OOM;
    }
    common::BitMap& vec_bitmap = vec->get_bitmap();
    char* vec_bitmap_data = vec_bitmap.get_bitmap();
    for (size_t i = 0; i < null_bitmap_size; ++i) {
        null_bitmap[i] = ~static_cast<uint8_t>(vec_bitmap_data[i]);
    }
    array_data->buffers[0] = null_bitmap;
    out_array->null_count = vec_bitmap.count_set_bits();
    return common::E_OK;
}

// Reset all fields of an ArrowArray to zero/null after releasing.
static void ResetArrowArray(ArrowArray* array) {
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

// Release children arrays: call each child's release(), then free the pointer.
// Used by both ReleaseStructArrowArray and error cleanup paths.
static void ReleaseArrowChildren(ArrowArray** children, size_t n_children) {
    if (children == nullptr) return;
    for (size_t i = 0; i < n_children; ++i) {
        if (children[i] != nullptr) {
            if (children[i]->release != nullptr) {
                children[i]->release(children[i]);
            }
            common::mem_free(children[i]);
        }
    }
    common::mem_free(children);
}

// Free an ArrowArrayData and all buffers it owns.
static void FreeArrowArrayData(ArrowArrayData* data) {
    if (data == nullptr) return;
    if (data->buffers != nullptr) {
        for (size_t i = 0; i < data->n_buffers; ++i) {
            if (data->buffers[i] != nullptr) {
                common::mem_free(data->buffers[i]);
            }
        }
        common::mem_free(data->buffers);
    }
    common::mem_free(data);
}

// Release a single-column ArrowArray (owns buffers via ArrowArrayData).
static void ReleaseArrowArray(ArrowArray* array) {
    if (array == nullptr || array->private_data == nullptr) {
        return;
    }
    FreeArrowArrayData(static_cast<ArrowArrayData*>(array->private_data));
    ResetArrowArray(array);
}

// Release a struct-level ArrowArray (owns children via StructArrayData).
static void ReleaseStructArrowArray(ArrowArray* array) {
    if (array == nullptr || array->private_data == nullptr) {
        return;
    }
    StructArrayData* data = static_cast<StructArrayData*>(array->private_data);
    ReleaseArrowChildren(data->children, data->n_children);
    delete data;
    ResetArrowArray(array);
}

// Release an ArrowSchema (owns strings and children via ArrowSchemaData).
// Free an ArrowSchemaData and all resources it owns (children, strings).
static void FreeArrowSchemaData(ArrowSchemaData* data) {
    if (data == nullptr) return;
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
}

static void ReleaseArrowSchema(ArrowSchema* schema) {
    if (schema == nullptr || schema->private_data == nullptr) {
        return;
    }
    FreeArrowSchemaData(static_cast<ArrowSchemaData*>(schema->private_data));

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

static ArrowArrayData* AllocArrowArrayData(int64_t n_buffers) {
    ArrowArrayData* data = static_cast<ArrowArrayData*>(
        common::mem_alloc(sizeof(ArrowArrayData), common::MOD_TSBLOCK));
    if (data == nullptr) return nullptr;
    data->n_buffers = n_buffers;
    data->buffers = static_cast<void**>(
        common::mem_alloc(n_buffers * sizeof(void*), common::MOD_TSBLOCK));
    if (data->buffers == nullptr) {
        common::mem_free(data);
        return nullptr;
    }
    for (int64_t i = 0; i < n_buffers; ++i) {
        data->buffers[i] = nullptr;
    }
    return data;
}

static void FinalizeArrowArray(ArrowArray* out_array,
                               ArrowArrayData* array_data, uint32_t row_count) {
    out_array->length = row_count;
    out_array->offset = 0;
    out_array->n_buffers = array_data->n_buffers;
    out_array->n_children = 0;
    out_array->buffers = const_cast<const void**>(array_data->buffers);
    out_array->children = nullptr;
    out_array->dictionary = nullptr;
    out_array->release = ReleaseArrowArray;
    out_array->private_data = array_data;
}

template <typename CType>
inline int BuildFixedLengthArrowArrayC(common::Vector* vec, uint32_t row_count,
                                       ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    bool has_null = vec->has_null();
    size_t type_size = sizeof(CType);

    ArrowArrayData* array_data = AllocArrowArrayData(2);
    if (array_data == nullptr) return common::E_OOM;

    int bm_ret = BuildNullBitmap(vec, row_count, array_data, out_array);
    if (bm_ret != common::E_OK) {
        FreeArrowArrayData(array_data);
        return bm_ret;
    }

    char* vec_data = vec->get_value_data().get_data();
    void* data_buffer = nullptr;

    if (std::is_same<CType, bool>::value) {
        size_t packed_size = GetNullBitmapSize(row_count);
        uint8_t* packed_buffer = static_cast<uint8_t*>(
            common::mem_alloc(packed_size, common::MOD_TSBLOCK));
        if (packed_buffer == nullptr) {
            FreeArrowArrayData(array_data);
            return common::E_OOM;
        }

        std::memset(packed_buffer, 0, packed_size);

        // Vector stores booleans as one byte each, densely packed
        // (null rows have no entry). Scatter into Arrow bit-packed format.
        // Use next_set_bit to skip null rows without per-row bitmap testing.
        const uint8_t* src = reinterpret_cast<const uint8_t*>(vec_data);
        uint32_t src_idx = 0;
        if (has_null) {
            common::BitMap& bm = vec->get_bitmap();
            uint32_t pos = 0;
            while (pos < row_count) {
                uint32_t null_pos = bm.next_set_bit(pos, row_count);
                // Process non-null run [pos, null_pos)
                for (uint32_t i = pos; i < null_pos; ++i) {
                    if (src[src_idx] != 0) {
                        packed_buffer[i / 8] |= (1 << (i & 7));
                    }
                    src_idx++;
                }
                if (null_pos >= row_count) break;
                // Skip null row (no source data, packed_buffer already zeroed)
                pos = null_pos + 1;
            }
        } else {
            for (uint32_t i = 0; i < row_count; ++i) {
                if (src[src_idx] != 0) {
                    packed_buffer[i / 8] |= (1 << (i & 7));
                }
                src_idx++;
            }
        }

        data_buffer = packed_buffer;
    } else {
        size_t data_size = type_size * row_count;
        data_buffer = common::mem_alloc(data_size, common::MOD_TSBLOCK);
        if (data_buffer == nullptr) {
            FreeArrowArrayData(array_data);
            return common::E_OOM;
        }

        if (has_null) {
            // Value buffer is densely packed (no slots for null rows).
            // Scatter non-null values into their correct Arrow positions.
            // Use next_set_bit to jump between null positions and bulk-copy
            // contiguous non-null runs in between.
            common::BitMap& bm = vec->get_bitmap();
            char* dst = static_cast<char*>(data_buffer);
            uint32_t src_offset = 0;
            uint32_t pos = 0;

            while (pos < row_count) {
                uint32_t null_pos = bm.next_set_bit(pos, row_count);
                // Copy the non-null run [pos, null_pos)
                if (null_pos > pos) {
                    uint32_t run = null_pos - pos;
                    std::memcpy(dst + pos * type_size, vec_data + src_offset,
                                run * type_size);
                    src_offset += run * type_size;
                }
                if (null_pos >= row_count) break;
                // Zero-fill the null slot
                std::memset(dst + null_pos * type_size, 0, type_size);
                pos = null_pos + 1;
            }
        } else {
            // No nulls: value buffer is dense and complete, direct copy
            std::memcpy(data_buffer, vec_data, data_size);
        }
    }

    array_data->buffers[1] = data_buffer;
    FinalizeArrowArray(out_array, array_data, row_count);
    return common::E_OK;
}

static int BuildStringArrowArrayC(common::Vector* vec, uint32_t row_count,
                                  ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    bool has_null = vec->has_null();

    ArrowArrayData* array_data = AllocArrowArrayData(3);
    if (array_data == nullptr) return common::E_OOM;

    int bm_ret = BuildNullBitmap(vec, row_count, array_data, out_array);
    if (bm_ret != common::E_OK) {
        FreeArrowArrayData(array_data);
        return bm_ret;
    }

    size_t offsets_size = sizeof(int32_t) * (row_count + 1);
    int32_t* offsets = static_cast<int32_t*>(
        common::mem_alloc(offsets_size, common::MOD_TSBLOCK));
    if (offsets == nullptr) {
        FreeArrowArrayData(array_data);
        return common::E_OOM;
    }

    // Total string data = vec buffer bytes - length prefixes of non-null rows.
    uint32_t nonnull_count =
        row_count - static_cast<uint32_t>(out_array->null_count);
    common::ByteBuffer& value_buf = vec->get_value_data();
    char* vec_data = value_buf.get_data();
    uint32_t vec_offset = 0;
    common::BitMap& vec_bitmap = vec->get_bitmap();
    uint32_t total_data_size =
        value_buf.get_data_size() - nonnull_count * sizeof(uint32_t);

    uint8_t* data_buffer = static_cast<uint8_t*>(common::mem_alloc(
        total_data_size > 0 ? total_data_size : 1, common::MOD_TSBLOCK));
    if (data_buffer == nullptr) {
        common::mem_free(offsets);
        FreeArrowArrayData(array_data);
        return common::E_OOM;
    }

    // Single pass: build offsets and copy string data together.
    // Use next_set_bit to skip null rows without per-row bitmap testing.
    offsets[0] = 0;
    uint32_t data_offset = 0;
    if (has_null) {
        uint32_t pos = 0;
        while (pos < row_count) {
            uint32_t null_pos = vec_bitmap.next_set_bit(pos, row_count);
            // Process non-null run [pos, null_pos)
            for (uint32_t i = pos; i < null_pos; ++i) {
                uint32_t len = 0;
                std::memcpy(&len, vec_data + vec_offset, sizeof(uint32_t));
                vec_offset += sizeof(uint32_t);
                if (len > 0) {
                    std::memcpy(data_buffer + data_offset,
                                vec_data + vec_offset, len);
                }
                vec_offset += len;
                data_offset += len;
                offsets[i + 1] = data_offset;
            }
            if (null_pos >= row_count) break;
            // Null row: no source data, offset stays the same
            offsets[null_pos + 1] = data_offset;
            pos = null_pos + 1;
        }
    } else {
        for (uint32_t i = 0; i < row_count; ++i) {
            uint32_t len = 0;
            std::memcpy(&len, vec_data + vec_offset, sizeof(uint32_t));
            vec_offset += sizeof(uint32_t);
            if (len > 0) {
                std::memcpy(data_buffer + data_offset, vec_data + vec_offset,
                            len);
            }
            vec_offset += len;
            data_offset += len;
            offsets[i + 1] = data_offset;
        }
    }
    array_data->buffers[1] = offsets;
    array_data->buffers[2] = data_buffer;
    FinalizeArrowArray(out_array, array_data, row_count);
    return common::E_OK;
}

static int BuildDateArrowArrayC(common::Vector* vec, uint32_t row_count,
                                ArrowArray* out_array) {
    if (vec == nullptr || out_array == nullptr || row_count == 0) {
        return common::E_INVALID_ARG;
    }

    bool has_null = vec->has_null();

    ArrowArrayData* array_data = AllocArrowArrayData(2);
    if (array_data == nullptr) return common::E_OOM;

    common::BitMap& vec_bitmap = vec->get_bitmap();
    int bm_ret = BuildNullBitmap(vec, row_count, array_data, out_array);
    if (bm_ret != common::E_OK) {
        FreeArrowArrayData(array_data);
        return bm_ret;
    }

    int32_t* data_buffer = static_cast<int32_t*>(
        common::mem_alloc(sizeof(int32_t) * row_count, common::MOD_TSBLOCK));
    if (data_buffer == nullptr) {
        FreeArrowArrayData(array_data);
        return common::E_OOM;
    }

    // Use next_set_bit to skip null rows without per-row bitmap testing.
    char* vec_data = vec->get_value_data().get_data();
    uint32_t src_offset = 0;
    if (has_null) {
        uint32_t pos = 0;
        while (pos < row_count) {
            uint32_t null_pos = vec_bitmap.next_set_bit(pos, row_count);
            // Process non-null run [pos, null_pos)
            for (uint32_t i = pos; i < null_pos; ++i) {
                int32_t yyyymmdd = 0;
                std::memcpy(&yyyymmdd, vec_data + src_offset, sizeof(int32_t));
                src_offset += sizeof(int32_t);
                data_buffer[i] = common::YYYYMMDDToDaysSinceEpoch(yyyymmdd);
            }
            if (null_pos >= row_count) break;
            // Null row: zero fill
            data_buffer[null_pos] = 0;
            pos = null_pos + 1;
        }
    } else {
        for (uint32_t i = 0; i < row_count; ++i) {
            int32_t yyyymmdd = 0;
            std::memcpy(&yyyymmdd, vec_data + src_offset, sizeof(int32_t));
            src_offset += sizeof(int32_t);
            data_buffer[i] = common::YYYYMMDDToDaysSinceEpoch(yyyymmdd);
        }
    }

    array_data->buffers[1] = data_buffer;
    FinalizeArrowArray(out_array, array_data, row_count);
    return common::E_OK;
}

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
        case common::BLOB:
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
    schema_data->format_string = format;
    schema_data->name_string = column_name;
    schema_data->children = nullptr;
    schema_data->n_children = 0;

    out_schema->format = schema_data->format_string.c_str();
    out_schema->name = schema_data->name_string.c_str();
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
    schema_data->format_string = "+s";
    schema_data->name_string = "";
    schema_data->n_children = column_count;
    schema_data->children = static_cast<ArrowSchema**>(common::mem_alloc(
        column_count * sizeof(ArrowSchema*), common::MOD_TSBLOCK));
    if (schema_data->children == nullptr) {
        FreeArrowSchemaData(schema_data);
        return common::E_OOM;
    }

    for (uint32_t i = 0; i < column_count; ++i) {
        schema_data->children[i] = nullptr;
    }

    // Build schema for each column
    for (uint32_t i = 0; i < column_count; ++i) {
        schema_data->children[i] = static_cast<ArrowSchema*>(
            common::mem_alloc(sizeof(ArrowSchema), common::MOD_TSBLOCK));
        if (schema_data->children[i] == nullptr) {
            FreeArrowSchemaData(schema_data);
            return common::E_OOM;
        }
        schema_data->children[i]->release = nullptr;

        common::TSDataType col_type = tuple_desc->get_column_type(i);
        std::string col_name = tuple_desc->get_column_name(i);

        int ret = BuildColumnArrowSchema(col_type, col_name,
                                         schema_data->children[i]);
        if (ret != common::E_OK) {
            FreeArrowSchemaData(schema_data);
            return ret;
        }
    }

    out_schema->format = schema_data->format_string.c_str();
    out_schema->name = schema_data->name_string.c_str();
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
        children_arrays[i] = nullptr;
    }

    for (uint32_t i = 0; i < column_count; ++i) {
        children_arrays[i] = static_cast<ArrowArray*>(
            common::mem_alloc(sizeof(ArrowArray), common::MOD_TSBLOCK));
        if (children_arrays[i] == nullptr) {
            ReleaseArrowChildren(children_arrays, column_count);
            ReleaseArrowSchema(out_schema);
            return common::E_OOM;
        }
        children_arrays[i]->release = nullptr;

        common::Vector* vec = tsblock.get_vector(i);
        int ret = BuildColumnArrowArray(vec, row_count, children_arrays[i]);
        if (ret != common::E_OK) {
            ReleaseArrowChildren(children_arrays, column_count);
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

// Allocate and return a TsFile null bitmap (bit=1=null) by inverting an Arrow
// validity bitmap (bit=1=valid). bit_offset is the Arrow array's offset field;
// bits [bit_offset, bit_offset+n_rows) are extracted and inverted.
// Returns nullptr if validity is nullptr (all rows valid, no allocation needed)
// or on OOM. Caller must mem_free the result.
// To distinguish OOM from "no validity": OOM only when validity!=nullptr &&
// result==nullptr.
static uint8_t* InvertArrowBitmap(const uint8_t* validity, int64_t bit_offset,
                                  uint32_t n_rows) {
    if (validity == nullptr) {
        return nullptr;
    }
    uint32_t bm_bytes = (n_rows + 7) / 8;
    uint8_t* null_bm =
        static_cast<uint8_t*>(common::mem_alloc(bm_bytes, common::MOD_TSBLOCK));
    if (null_bm == nullptr) {
        return nullptr;
    }
    if (bit_offset == 0) {
        // Fast path: byte-level invert when there is no bit misalignment.
        for (uint32_t b = 0; b < bm_bytes; b++) {
            null_bm[b] = ~validity[b];
        }
    } else {
        // Sliced array: extract one bit at a time starting at bit_offset.
        std::memset(null_bm, 0, bm_bytes);
        for (uint32_t i = 0; i < n_rows; i++) {
            int64_t src = bit_offset + i;
            uint8_t valid = (validity[src / 8] >> (src % 8)) & 1;
            if (!valid) {
                null_bm[i / 8] |= static_cast<uint8_t>(1u << (i % 8));
            }
        }
    }
    return null_bm;
}

// Check if Arrow row is valid (non-null) based on validity bitmap
static bool ArrowIsValid(const ArrowArray* arr, int64_t row) {
    if (arr->null_count == 0 || arr->buffers[0] == nullptr) return true;
    int64_t bit_idx = arr->offset + row;
    const uint8_t* bitmap = static_cast<const uint8_t*>(arr->buffers[0]);
    return (bitmap[bit_idx / 8] >> (bit_idx % 8)) & 1;
}

// Map Arrow format string to TSDataType
static common::TSDataType ArrowFormatToDataType(const char* format) {
    if (strcmp(format, "b") == 0) return common::BOOLEAN;
    if (strcmp(format, "i") == 0) return common::INT32;
    if (strcmp(format, "l") == 0) return common::INT64;
    if (strcmp(format, "tsn:") == 0) return common::TIMESTAMP;
    if (strcmp(format, "f") == 0) return common::FLOAT;
    if (strcmp(format, "g") == 0) return common::DOUBLE;
    if (strcmp(format, "u") == 0) return common::TEXT;
    if (strcmp(format, "z") == 0) return common::BLOB;
    if (strcmp(format, "tdD") == 0) return common::DATE;
    return common::INVALID_DATATYPE;
}

// Convert Arrow C Data Interface struct array to storage::Tablet.
// time_col_index specifies which column in the Arrow struct to use as the
// timestamp column.
// All other columns become data columns in the Tablet.
// reg_schema: optional registered TableSchema; when provided its column types
// are used in the Tablet (so they match the writer's registered schema
// exactly).
// Arrow format strings are still used to decode the actual buffers.
int ArrowStructToTablet(const char* table_name, const ArrowArray* in_array,
                        const ArrowSchema* in_schema,
                        const storage::TableSchema* reg_schema,
                        storage::Tablet** out_tablet, int time_col_index) {
    if (!in_array || !in_schema || !out_tablet) return common::E_INVALID_ARG;
    if (strcmp(in_schema->format, "+s") != 0) return common::E_INVALID_ARG;

    int64_t n_rows = in_array->length;
    int64_t n_cols = in_schema->n_children;
    if (n_rows <= 0 || n_cols == 0) return common::E_INVALID_ARG;

    if (time_col_index < 0 || time_col_index >= n_cols)
        return common::E_INVALID_ARG;

    std::vector<std::string> col_names;
    std::vector<common::TSDataType> col_types;
    std::vector<common::TSDataType> read_modes;
    std::vector<int> data_col_indices;

    std::vector<common::TSDataType> reg_data_types;
    if (reg_schema) {
        reg_data_types = reg_schema->get_data_types();
    }

    for (int64_t i = 0; i < n_cols; i++) {
        if (static_cast<int>(i) == time_col_index) continue;
        const ArrowSchema* child = in_schema->children[i];
        common::TSDataType read_mode = ArrowFormatToDataType(child->format);
        if (read_mode == common::INVALID_DATATYPE)
            return common::E_TYPE_NOT_SUPPORTED;
        std::string col_name = child->name ? child->name : "";
        common::TSDataType col_type = read_mode;
        if (reg_schema) {
            int reg_idx = const_cast<storage::TableSchema*>(reg_schema)
                              ->find_column_index(col_name);
            if (reg_idx >= 0 &&
                reg_idx < static_cast<int>(reg_data_types.size())) {
                col_type = reg_data_types[reg_idx];
            }
        }
        col_names.emplace_back(std::move(col_name));
        col_types.push_back(col_type);
        read_modes.push_back(read_mode);
        data_col_indices.push_back(static_cast<int>(i));
    }

    if (col_names.empty()) return common::E_INVALID_ARG;

    std::string tname = table_name ? table_name : "default_table";
    auto* tablet = new storage::Tablet(tname, &col_names, &col_types,
                                       static_cast<int>(n_rows));
    if (tablet->err_code_ != common::E_OK) {
        int err = tablet->err_code_;
        delete tablet;
        return err;
    }

    // Fill timestamps from the time column
    {
        const ArrowArray* ts_arr = in_array->children[time_col_index];
        const int64_t* ts_buf =
            static_cast<const int64_t*>(ts_arr->buffers[1]) + ts_arr->offset;
        tablet->set_timestamps(ts_buf, static_cast<uint32_t>(n_rows));
    }

    // Fill data columns from Arrow children (use read_modes to decode buffers)
    for (size_t ci = 0; ci < data_col_indices.size(); ci++) {
        const ArrowArray* col_arr = in_array->children[data_col_indices[ci]];
        common::TSDataType dtype = read_modes[ci];
        uint32_t tcol = static_cast<uint32_t>(ci);
        // ArrowArray::offset is non-zero when the array is a slice of a larger
        // buffer — for example, when Python pandas/PyArrow passes a column that
        // was created via slice(), take(), or filter() without a copy, or when
        // RecordBatch::Slice() is used to split a batch. In those cases the
        // underlying buffer starts at element 0 of the original allocation, so
        // all buffer accesses (data, offsets, validity bitmap) must be shifted
        // by `off` before reading the `length` visible elements.
        int64_t off = col_arr->offset;

        const uint8_t* validity =
            (col_arr->null_count > 0 && col_arr->buffers[0] != nullptr)
                ? static_cast<const uint8_t*>(col_arr->buffers[0])
                : nullptr;

        switch (dtype) {
            case common::BOOLEAN: {
                const uint8_t* vals =
                    static_cast<const uint8_t*>(col_arr->buffers[1]);
                for (int64_t r = 0; r < n_rows; r++) {
                    if (!ArrowIsValid(col_arr, r)) continue;
                    int64_t bit = off + r;
                    bool v = (vals[bit / 8] >> (bit % 8)) & 1;
                    tablet->add_value<bool>(static_cast<uint32_t>(r), tcol, v);
                }
                break;
            }
            case common::INT32:
            case common::INT64:
            case common::FLOAT:
            case common::DOUBLE: {
                size_t elem_size =
                    (dtype == common::INT64 || dtype == common::DOUBLE) ? 8 : 4;
                const void* data =
                    static_cast<const char*>(col_arr->buffers[1]) +
                    off * elem_size;
                uint8_t* null_bm = InvertArrowBitmap(
                    validity, off, static_cast<uint32_t>(n_rows));
                if (validity != nullptr && null_bm == nullptr) {
                    delete tablet;
                    return common::E_OOM;
                }
                tablet->set_column_values(tcol, data, null_bm,
                                          static_cast<uint32_t>(n_rows));
                if (null_bm != nullptr) {
                    common::mem_free(null_bm);
                }
                break;
            }
            case common::DATE: {
                // Arrow stores date as int32 days-since-epoch; convert to
                // YYYYMMDD
                const int32_t* vals =
                    static_cast<const int32_t*>(col_arr->buffers[1]);
                for (int64_t r = 0; r < n_rows; r++) {
                    if (!ArrowIsValid(col_arr, r)) continue;
                    int32_t yyyymmdd =
                        common::DaysSinceEpochToYYYYMMDD(vals[off + r]);
                    tablet->add_value<int32_t>(static_cast<uint32_t>(r), tcol,
                                               yyyymmdd);
                }
                break;
            }
            case common::TEXT:
            case common::STRING:
            case common::BLOB: {
                // set_column_string_values requires offsets[0] == 0.
                // When off > 0 (sliced Arrow array), normalize here: shift
                // offsets down by base and advance the data pointer
                // accordingly.
                const int32_t* raw_offsets =
                    static_cast<const int32_t*>(col_arr->buffers[1]) + off;
                const char* raw_data =
                    static_cast<const char*>(col_arr->buffers[2]);
                uint32_t nrows = static_cast<uint32_t>(n_rows);
                const int32_t* offsets = raw_offsets;
                const char* data = raw_data;
                int32_t* norm_offsets = nullptr;
                if (off > 0) {
                    int32_t base = raw_offsets[0];
                    norm_offsets = static_cast<int32_t*>(common::mem_alloc(
                        (nrows + 1) * sizeof(int32_t), common::MOD_TSBLOCK));
                    if (norm_offsets == nullptr) {
                        delete tablet;
                        return common::E_OOM;
                    }
                    for (uint32_t i = 0; i <= nrows; i++) {
                        norm_offsets[i] = raw_offsets[i] - base;
                    }
                    offsets = norm_offsets;
                    data = raw_data + base;
                }
                uint8_t* null_bm = InvertArrowBitmap(validity, off, nrows);
                if (validity != nullptr && null_bm == nullptr) {
                    common::mem_free(norm_offsets);
                    delete tablet;
                    return common::E_OOM;
                }
                tablet->set_column_string_values(tcol, offsets, data, null_bm,
                                                 nrows);
                if (null_bm != nullptr) {
                    common::mem_free(null_bm);
                }
                if (norm_offsets != nullptr) {
                    common::mem_free(norm_offsets);
                }
                break;
            }
            default:
                delete tablet;
                return common::E_TYPE_NOT_SUPPORTED;
        }
    }

    *out_tablet = tablet;
    return common::E_OK;
}

}  // namespace arrow
