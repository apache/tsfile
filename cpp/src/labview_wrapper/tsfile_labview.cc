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

#include "tsfile_labview.h"

#include <climits>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <new>
#include <string>
#include <unordered_map>
#include <vector>

#include "async_close.h"
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"

// The cwrapper headers document status names as E_*, but errno_define_c.h
// defines them as RET_*. Alias the two codes this shim relies on.
#ifndef E_OK
#define E_OK RET_OK
#endif
#ifndef E_INVALID_ARG
#define E_INVALID_ARG RET_INVALID_ARG
#endif

namespace {

/* ---------- handle registry ---------- */
enum class Kind {
    kSchemaBuilder,
    kWriter,
    kTablet,
    kReader,
    kResultSet,
    kCloseTask
};

struct SchemaBuilderCtx {
    std::string table_name;
    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    std::vector<ColumnCategory> col_cats;
};

struct WriterCtx {
    WriteFile wf = nullptr;
    TsFileWriter writer = nullptr;
    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    Tablet block_tablet = nullptr;
    uint32_t block_capacity = 0;
    std::vector<int32_t> i32_scratch;
    std::vector<float> f32_scratch;
    std::vector<double> f64_scratch;
};

struct CloseTaskCtx {
    std::shared_ptr<labview::AsyncCloseTask> task;
};

struct TabletCtx {
    uint32_t max_rows = 0;
    std::vector<std::string> col_names;
    std::vector<TSDataType> col_types;
    Tablet tablet = nullptr;  // built on finalize
    bool finalized = false;
};

struct ReaderCtx {
    TsFileReader reader = nullptr;
};

enum class ResultSetMode { kRow, kBatch };

struct ResultSetCtx {
    ResultSet rs = nullptr;
    std::vector<TSDataType> col_types;  // cached from metadata
    ResultSetMode mode = ResultSetMode::kRow;
    int32_t batch_rows = 0;
    int32_t value_column_count = 0;
};

struct Entry {
    Kind kind;
    void* ptr;
};

std::mutex g_mtx;
std::unordered_map<uint64_t, Entry> g_registry;
uint64_t g_next_id = 1;

labview::AsyncCloseCoordinator& close_coordinator() {
    static labview::AsyncCloseCoordinator coordinator;
    return coordinator;
}

uint64_t register_handle(Kind kind, void* ptr) {
    std::lock_guard<std::mutex> lock(g_mtx);
    uint64_t id = g_next_id++;
    g_registry[id] = Entry{kind, ptr};
    return id;
}

void* lookup(uint64_t id, Kind kind) {
    std::lock_guard<std::mutex> lock(g_mtx);
    auto it = g_registry.find(id);
    if (it == g_registry.end() || it->second.kind != kind) {
        return nullptr;
    }
    return it->second.ptr;
}

void* unregister(uint64_t id, Kind kind) {
    std::lock_guard<std::mutex> lock(g_mtx);
    auto it = g_registry.find(id);
    if (it == g_registry.end() || it->second.kind != kind) {
        return nullptr;
    }
    void* p = it->second.ptr;
    g_registry.erase(it);
    return p;
}

LV_Status close_writer(WriterCtx* ctx) noexcept {
    if (ctx == nullptr) {
        return E_INVALID_ARG;
    }

    LV_Status status = E_OK;
    try {
        if (ctx->writer != nullptr) {
            status = tsfile_writer_close(ctx->writer);
        }
    } catch (const std::bad_alloc&) {
        status = RET_OOM;
    } catch (...) {
        status = RET_FILE_CLOSE_ERR;
    }

    if (ctx->wf != nullptr) {
        free_write_file(&ctx->wf);
    }
    if (ctx->block_tablet != nullptr) {
        free_tablet(&ctx->block_tablet);
    }
    delete ctx;
    return status;
}

std::vector<int32_t>& block_scratch(WriterCtx* ctx, const int32_t*) {
    return ctx->i32_scratch;
}

std::vector<float>& block_scratch(WriterCtx* ctx, const float*) {
    return ctx->f32_scratch;
}

std::vector<double>& block_scratch(WriterCtx* ctx, const double*) {
    return ctx->f64_scratch;
}

ERRNO ensure_block_tablet(WriterCtx* ctx, uint32_t rows, uint32_t cols) {
    if (ctx->block_tablet != nullptr && ctx->block_capacity >= rows) {
        return E_OK;
    }

    std::vector<char*> names(cols);
    for (uint32_t col = 0; col < cols; ++col) {
        names[col] = const_cast<char*>(ctx->col_names[col].c_str());
    }
    Tablet replacement =
        tablet_new(names.data(), ctx->col_types.data(), cols, rows);
    if (replacement == nullptr) {
        return RET_OOM;
    }
    if (ctx->block_tablet != nullptr) {
        free_tablet(&ctx->block_tablet);
    }
    ctx->block_tablet = replacement;
    ctx->block_capacity = rows;
    return E_OK;
}

template <typename T>
LV_Status write_block(LV_Handle writer, const int64_t* ts, const T* data,
                      int32_t nrows, int32_t ncols, TSDataType expected_type) {
    auto* wctx = static_cast<WriterCtx*>(lookup(writer, Kind::kWriter));
    if (wctx == nullptr || wctx->writer == nullptr || ts == nullptr ||
        data == nullptr || nrows <= 0 || ncols <= 0 ||
        static_cast<size_t>(ncols) != wctx->col_names.size() ||
        wctx->col_names.size() != wctx->col_types.size()) {
        return E_INVALID_ARG;
    }

    for (TSDataType type : wctx->col_types) {
        if (type != expected_type) {
            return RET_TYPE_NOT_MATCH;
        }
    }

    const size_t rows = static_cast<size_t>(nrows);
    const size_t cols = static_cast<size_t>(ncols);
    const size_t max_size = std::numeric_limits<size_t>::max();
    if (rows > max_size / cols || rows * cols > max_size / sizeof(T)) {
        return E_INVALID_ARG;
    }

    try {
        ERRNO err = ensure_block_tablet(wctx, static_cast<uint32_t>(rows),
                                        static_cast<uint32_t>(cols));
        if (err != E_OK) {
            return err;
        }

        err = tablet_reset(wctx->block_tablet, 0);
        if (err != E_OK) {
            return err;
        }
        err = tablet_set_timestamps(wctx->block_tablet, ts,
                                    static_cast<uint32_t>(rows));
        if (err != E_OK) {
            return err;
        }

        std::vector<T>& scratch = block_scratch(wctx, static_cast<T*>(nullptr));
        scratch.resize(rows * cols);
        for (size_t col = 0; col < cols; ++col) {
            T* column = scratch.data() + col * rows;
            for (size_t row = 0; row < rows; ++row) {
                column[row] = data[row * cols + col];
            }
            err = tablet_set_column_values(
                wctx->block_tablet, static_cast<uint32_t>(col), column, nullptr,
                static_cast<uint32_t>(rows));
            if (err != E_OK) {
                return err;
            }
        }

        return tsfile_writer_write(wctx->writer, wctx->block_tablet);
    } catch (const std::bad_alloc&) {
        return RET_OOM;
    } catch (...) {
        return RET_FILE_WRITE_ERR;
    }
}

}  // namespace

/* ===================== global configuration ===================== */
LV_Status lv_tsfile_set_global_compression(uint8_t compression) {
    return set_global_compression(compression);
}

LV_Status lv_tsfile_set_datatype_encoding(uint8_t dtype, uint8_t enc) {
    return set_datatype_encoding(dtype, enc);
}

/* ===================== schema builder ===================== */
LV_Handle lv_tsfile_schema_builder_new(const char* table_name) {
    auto* ctx = new SchemaBuilderCtx();
    if (table_name != nullptr) {
        ctx->table_name = table_name;
    }
    return register_handle(Kind::kSchemaBuilder, ctx);
}

LV_Status lv_tsfile_schema_builder_add_column(LV_Handle builder,
                                              const char* name,
                                              uint8_t data_type,
                                              uint8_t category) {
    auto* ctx =
        static_cast<SchemaBuilderCtx*>(lookup(builder, Kind::kSchemaBuilder));
    if (ctx == nullptr || name == nullptr) {
        return E_INVALID_ARG;
    }
    ctx->col_names.emplace_back(name);
    ctx->col_types.push_back(static_cast<TSDataType>(data_type));
    ctx->col_cats.push_back(static_cast<ColumnCategory>(category));
    return E_OK;
}

void lv_tsfile_schema_builder_free(LV_Handle builder) {
    auto* ctx = static_cast<SchemaBuilderCtx*>(
        unregister(builder, Kind::kSchemaBuilder));
    delete ctx;
}

/* ===================== writer ===================== */
LV_Status lv_tsfile_writer_open(const char* path, LV_Handle schema_builder,
                                uint64_t mem_threshold_bytes,
                                LV_Handle* out_writer) {
    if (path == nullptr || out_writer == nullptr) {
        return E_INVALID_ARG;
    }
    *out_writer = 0;
    auto* sb = static_cast<SchemaBuilderCtx*>(
        lookup(schema_builder, Kind::kSchemaBuilder));
    if (sb == nullptr || sb->col_names.empty()) {
        return E_INVALID_ARG;
    }

    ERRNO err = E_OK;
    WriteFile wf = write_file_new(path, &err);
    if (wf == nullptr || err != E_OK) {
        return err != E_OK ? err : E_INVALID_ARG;
    }

    // Build a transient TableSchema; cwrapper copies it internally.
    const int n = static_cast<int>(sb->col_names.size());
    std::vector<ColumnSchema> cols(n);
    for (int i = 0; i < n; ++i) {
        cols[i].column_name = const_cast<char*>(sb->col_names[i].c_str());
        cols[i].data_type = sb->col_types[i];
        cols[i].column_category = sb->col_cats[i];
    }
    TableSchema schema;
    schema.table_name = const_cast<char*>(sb->table_name.c_str());
    schema.column_schemas = cols.data();
    schema.column_num = n;

    TsFileWriter writer = nullptr;
    if (mem_threshold_bytes > 0) {
        writer = tsfile_writer_new_with_memory_threshold(
            wf, &schema, mem_threshold_bytes, &err);
    } else {
        writer = tsfile_writer_new(wf, &schema, &err);
    }
    if (writer == nullptr || err != E_OK) {
        free_write_file(&wf);
        return err != E_OK ? err : E_INVALID_ARG;
    }

    WriterCtx* ctx = nullptr;
    try {
        ctx = new WriterCtx();
        ctx->wf = wf;
        ctx->writer = writer;
        ctx->col_names = sb->col_names;
        ctx->col_types = sb->col_types;
        *out_writer = register_handle(Kind::kWriter, ctx);
        return E_OK;
    } catch (...) {
        delete ctx;
        tsfile_writer_close(writer);
        free_write_file(&wf);
        return RET_OOM;
    }
}

LV_Status lv_tsfile_writer_write(LV_Handle writer, LV_Handle tablet) {
    auto* wctx = static_cast<WriterCtx*>(lookup(writer, Kind::kWriter));
    auto* tctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet));
    if (wctx == nullptr || tctx == nullptr || tctx->tablet == nullptr) {
        return E_INVALID_ARG;
    }
    return tsfile_writer_write(wctx->writer, tctx->tablet);
}

LV_Status lv_tsfile_write_block_i32(LV_Handle writer, const int64_t* ts,
                                    const int32_t* data, int32_t nrows,
                                    int32_t ncols) {
    return write_block(writer, ts, data, nrows, ncols, TS_DATATYPE_INT32);
}

LV_Status lv_tsfile_write_block_f32(LV_Handle writer, const int64_t* ts,
                                    const float* data, int32_t nrows,
                                    int32_t ncols) {
    return write_block(writer, ts, data, nrows, ncols, TS_DATATYPE_FLOAT);
}

LV_Status lv_tsfile_write_block_f64(LV_Handle writer, const int64_t* ts,
                                    const double* data, int32_t nrows,
                                    int32_t ncols) {
    return write_block(writer, ts, data, nrows, ncols, TS_DATATYPE_DOUBLE);
}

LV_Status lv_tsfile_writer_flush(LV_Handle writer) {
    auto* ctx = static_cast<WriterCtx*>(lookup(writer, Kind::kWriter));
    if (ctx == nullptr || ctx->writer == nullptr) {
        return E_INVALID_ARG;
    }
    try {
        return tsfile_writer_flush(ctx->writer);
    } catch (const std::bad_alloc&) {
        return RET_OOM;
    } catch (...) {
        return RET_FILE_WRITE_ERR;
    }
}

LV_Status lv_tsfile_writer_close_ex(LV_Handle writer, int32_t async_close,
                                    LV_Handle* out_close_task) {
    if (out_close_task == nullptr) {
        return E_INVALID_ARG;
    }
    *out_close_task = 0;
    if (async_close != 0 && async_close != 1) {
        return E_INVALID_ARG;
    }

    auto* ctx = static_cast<WriterCtx*>(unregister(writer, Kind::kWriter));
    if (ctx == nullptr) {
        return E_INVALID_ARG;
    }

    if (async_close == 0) {
        return close_writer(ctx);
    }

    std::shared_ptr<labview::AsyncCloseTask> task;
    LV_Status submit_status = E_OK;
    try {
        submit_status = close_coordinator().Submit(
            [ctx] { return close_writer(ctx); }, &task);
    } catch (...) {
        return close_writer(ctx);
    }
    if (task == nullptr) {
        return submit_status;
    }

    CloseTaskCtx* task_ctx = nullptr;
    try {
        task_ctx = new CloseTaskCtx();
        task_ctx->task = task;
        *out_close_task = register_handle(Kind::kCloseTask, task_ctx);
        return E_OK;
    } catch (...) {
        delete task_ctx;
        return task->Wait();
    }
}

LV_Status lv_tsfile_close_task_wait(LV_Handle close_task) {
    auto* ctx =
        static_cast<CloseTaskCtx*>(unregister(close_task, Kind::kCloseTask));
    if (ctx == nullptr || ctx->task == nullptr) {
        delete ctx;
        return E_INVALID_ARG;
    }

    LV_Status status = E_OK;
    try {
        status = ctx->task->Wait();
    } catch (const std::bad_alloc&) {
        status = RET_OOM;
    } catch (...) {
        status = RET_FILE_CLOSE_ERR;
    }
    delete ctx;
    return status;
}

LV_Status lv_tsfile_writer_close(LV_Handle writer) {
    LV_Handle unused_task = 0;
    return lv_tsfile_writer_close_ex(writer, 0, &unused_task);
}

/* ===================== tablet builder ===================== */
LV_Handle lv_tsfile_tablet_new(uint32_t max_rows) {
    auto* ctx = new TabletCtx();
    ctx->max_rows = max_rows;
    return register_handle(Kind::kTablet, ctx);
}

LV_Status lv_tsfile_tablet_add_column(LV_Handle tablet, const char* name,
                                      uint8_t dtype) {
    auto* ctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet));
    if (ctx == nullptr || name == nullptr || ctx->finalized) {
        return E_INVALID_ARG;
    }
    ctx->col_names.emplace_back(name);
    ctx->col_types.push_back(static_cast<TSDataType>(dtype));
    return E_OK;
}

LV_Status lv_tsfile_tablet_finalize_columns(LV_Handle tablet) {
    auto* ctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet));
    if (ctx == nullptr || ctx->finalized || ctx->col_names.empty() ||
        ctx->max_rows == 0) {
        return E_INVALID_ARG;
    }
    const uint32_t n = static_cast<uint32_t>(ctx->col_names.size());
    std::vector<char*> names(n);
    for (uint32_t i = 0; i < n; ++i) {
        names[i] = const_cast<char*>(ctx->col_names[i].c_str());
    }
    ctx->tablet =
        tablet_new(names.data(), ctx->col_types.data(), n, ctx->max_rows);
    if (ctx->tablet == nullptr) {
        return E_INVALID_ARG;
    }
    ctx->finalized = true;
    return E_OK;
}

LV_Status lv_tsfile_tablet_set_timestamp(LV_Handle tablet, uint32_t row,
                                         int64_t ts) {
    auto* ctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet));
    if (ctx == nullptr || ctx->tablet == nullptr) {
        return E_INVALID_ARG;
    }
    return tablet_add_timestamp(ctx->tablet, row, ts);
}

#define LV_TABLET_SET_IMPL(suffix, ctype, cwrap)                            \
    LV_Status lv_tsfile_tablet_set_##suffix(LV_Handle tablet, uint32_t row, \
                                            uint32_t col, ctype v) {        \
        auto* ctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet)); \
        if (ctx == nullptr || ctx->tablet == nullptr) {                     \
            return E_INVALID_ARG;                                           \
        }                                                                   \
        return cwrap(ctx->tablet, row, col, v);                             \
    }

LV_TABLET_SET_IMPL(i32, int32_t, tablet_add_value_by_index_int32_t)
LV_TABLET_SET_IMPL(i64, int64_t, tablet_add_value_by_index_int64_t)
LV_TABLET_SET_IMPL(f32, float, tablet_add_value_by_index_float)
LV_TABLET_SET_IMPL(f64, double, tablet_add_value_by_index_double)

#undef LV_TABLET_SET_IMPL

LV_Status lv_tsfile_tablet_set_bool(LV_Handle tablet, uint32_t row,
                                    uint32_t col, int32_t v) {
    auto* ctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet));
    if (ctx == nullptr || ctx->tablet == nullptr) {
        return E_INVALID_ARG;
    }
    return tablet_add_value_by_index_bool(ctx->tablet, row, col, v != 0);
}

LV_Status lv_tsfile_tablet_set_str(LV_Handle tablet, uint32_t row, uint32_t col,
                                   const char* v, int32_t len) {
    auto* ctx = static_cast<TabletCtx*>(lookup(tablet, Kind::kTablet));
    if (ctx == nullptr || ctx->tablet == nullptr || v == nullptr) {
        return E_INVALID_ARG;
    }
    return tablet_add_value_by_index_string_with_len(ctx->tablet, row, col, v,
                                                     len);
}

void lv_tsfile_tablet_free(LV_Handle tablet) {
    auto* ctx = static_cast<TabletCtx*>(unregister(tablet, Kind::kTablet));
    if (ctx == nullptr) {
        return;
    }
    if (ctx->tablet != nullptr) {
        free_tablet(&ctx->tablet);
    }
    delete ctx;
}

/* ===================== reader ===================== */
LV_Status lv_tsfile_reader_open(const char* path, LV_Handle* out_reader) {
    if (path == nullptr || out_reader == nullptr) {
        return E_INVALID_ARG;
    }
    *out_reader = 0;
    ERRNO err = E_OK;
    TsFileReader reader = tsfile_reader_new(path, &err);
    if (reader == nullptr || err != E_OK) {
        return err != E_OK ? err : E_INVALID_ARG;
    }
    auto* ctx = new ReaderCtx();
    ctx->reader = reader;
    *out_reader = register_handle(Kind::kReader, ctx);
    return E_OK;
}

LV_Status lv_tsfile_reader_close(LV_Handle reader) {
    auto* ctx = static_cast<ReaderCtx*>(unregister(reader, Kind::kReader));
    if (ctx == nullptr) {
        return E_INVALID_ARG;
    }
    ERRNO err = E_OK;
    if (ctx->reader != nullptr) {
        err = tsfile_reader_close(ctx->reader);
    }
    delete ctx;
    return err;
}

namespace {
// Split a newline-separated list into owned strings.
std::vector<std::string> split_lines(const char* s) {
    std::vector<std::string> out;
    if (s == nullptr) {
        return out;
    }
    std::string cur;
    for (const char* p = s; *p != '\0'; ++p) {
        if (*p == '\n') {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(*p);
        }
    }
    out.push_back(cur);
    return out;
}
}  // namespace

LV_Status lv_tsfile_write_file_f64(const char* tsfile_path,
                                   const char* table_name,
                                   const char* column_names_newline_separated,
                                   const int64_t* ts, const double* data,
                                   int32_t nrows, int32_t ncols) {
    if (tsfile_path == nullptr || table_name == nullptr ||
        column_names_newline_separated == nullptr || ts == nullptr ||
        data == nullptr || *tsfile_path == '\0' || *table_name == '\0' ||
        nrows <= 0 || ncols <= 0) {
        return E_INVALID_ARG;
    }

    LV_Handle builder = 0;
    LV_Handle writer = 0;
    try {
        const std::vector<std::string> names =
            split_lines(column_names_newline_separated);
        if (names.size() != static_cast<size_t>(ncols)) {
            return E_INVALID_ARG;
        }
        for (const std::string& name : names) {
            if (name.empty()) {
                return E_INVALID_ARG;
            }
        }

        builder = lv_tsfile_schema_builder_new(table_name);
        if (builder == 0) {
            return RET_OOM;
        }
        for (const std::string& name : names) {
            const LV_Status add_status = lv_tsfile_schema_builder_add_column(
                builder, name.c_str(), LV_TYPE_DOUBLE, LV_CAT_FIELD);
            if (add_status != E_OK) {
                lv_tsfile_schema_builder_free(builder);
                return add_status;
            }
        }

        LV_Status status =
            lv_tsfile_writer_open(tsfile_path, builder, 0, &writer);
        lv_tsfile_schema_builder_free(builder);
        builder = 0;
        if (status != E_OK) {
            return status;
        }

        status = lv_tsfile_write_block_f64(writer, ts, data, nrows, ncols);
        const LV_Status close_status = lv_tsfile_writer_close(writer);
        writer = 0;
        return status != E_OK ? status : close_status;
    } catch (const std::bad_alloc&) {
        if (builder != 0) {
            lv_tsfile_schema_builder_free(builder);
        }
        if (writer != 0) {
            lv_tsfile_writer_close(writer);
        }
        return RET_OOM;
    } catch (...) {
        if (builder != 0) {
            lv_tsfile_schema_builder_free(builder);
        }
        if (writer != 0) {
            lv_tsfile_writer_close(writer);
        }
        return RET_FILE_WRITE_ERR;
    }
}

LV_Status lv_tsfile_write_demo(const char* tsfile_path, int32_t nrows) {
    if (tsfile_path == nullptr || *tsfile_path == '\0' || nrows <= 0 ||
        nrows > INT32_MAX / 10) {
        return E_INVALID_ARG;
    }

    LV_Handle builder = 0;
    LV_Handle writer = 0;
    LV_Handle tablet = 0;
    LV_Status status = E_OK;

    try {
        builder = lv_tsfile_schema_builder_new("demo");
        if (builder == 0) {
            status = RET_OOM;
            goto cleanup;
        }
        status = lv_tsfile_schema_builder_add_column(
            builder, "device", LV_TYPE_STRING, LV_CAT_TAG);
        if (status != E_OK) {
            goto cleanup;
        }
        status = lv_tsfile_schema_builder_add_column(
            builder, "temp", LV_TYPE_DOUBLE, LV_CAT_FIELD);
        if (status != E_OK) {
            goto cleanup;
        }
        status = lv_tsfile_schema_builder_add_column(
            builder, "cnt", LV_TYPE_INT32, LV_CAT_FIELD);
        if (status != E_OK) {
            goto cleanup;
        }
        status = lv_tsfile_writer_open(tsfile_path, builder, 0, &writer);
        if (status != E_OK) {
            goto cleanup;
        }

        tablet = lv_tsfile_tablet_new(static_cast<uint32_t>(nrows));
        if (tablet == 0) {
            status = RET_OOM;
            goto cleanup;
        }
        status = lv_tsfile_tablet_add_column(tablet, "device", LV_TYPE_STRING);
        if (status != E_OK) {
            goto cleanup;
        }
        status = lv_tsfile_tablet_add_column(tablet, "temp", LV_TYPE_DOUBLE);
        if (status != E_OK) {
            goto cleanup;
        }
        status = lv_tsfile_tablet_add_column(tablet, "cnt", LV_TYPE_INT32);
        if (status != E_OK) {
            goto cleanup;
        }
        status = lv_tsfile_tablet_finalize_columns(tablet);
        if (status != E_OK) {
            goto cleanup;
        }

        for (int32_t row = 0; row < nrows; ++row) {
            status = lv_tsfile_tablet_set_timestamp(tablet, row, row + 1);
            if (status != E_OK) {
                goto cleanup;
            }
            status = lv_tsfile_tablet_set_str(tablet, row, 0, "sensorA", 7);
            if (status != E_OK) {
                goto cleanup;
            }
            status = lv_tsfile_tablet_set_f64(tablet, row, 1,
                                              20.0 + 0.5 * std::sin(row));
            if (status != E_OK) {
                goto cleanup;
            }
            status = lv_tsfile_tablet_set_i32(tablet, row, 2, row * 10);
            if (status != E_OK) {
                goto cleanup;
            }
        }
        status = lv_tsfile_writer_write(writer, tablet);
    } catch (const std::bad_alloc&) {
        status = RET_OOM;
    } catch (...) {
        status = RET_FILE_WRITE_ERR;
    }

cleanup:
    if (tablet != 0) {
        lv_tsfile_tablet_free(tablet);
    }
    if (writer != 0) {
        const LV_Status close_status = lv_tsfile_writer_close(writer);
        if (status == E_OK) {
            status = close_status;
        }
    }
    if (builder != 0) {
        lv_tsfile_schema_builder_free(builder);
    }
    return status;
}

LV_Status lv_tsfile_query_table(LV_Handle reader, const char* table_name,
                                const char* columns_newline_separated,
                                int64_t start_time, int64_t end_time,
                                LV_Handle* out_result_set) {
    if (out_result_set == nullptr) {
        return E_INVALID_ARG;
    }
    *out_result_set = 0;
    auto* rctx = static_cast<ReaderCtx*>(lookup(reader, Kind::kReader));
    if (rctx == nullptr || table_name == nullptr) {
        return E_INVALID_ARG;
    }
    std::vector<std::string> cols = split_lines(columns_newline_separated);
    if (cols.empty()) {
        return E_INVALID_ARG;
    }
    std::vector<char*> col_ptrs(cols.size());
    for (size_t i = 0; i < cols.size(); ++i) {
        col_ptrs[i] = const_cast<char*>(cols[i].c_str());
    }

    ERRNO err = E_OK;
    ResultSet rs = tsfile_query_table(rctx->reader, table_name, col_ptrs.data(),
                                      static_cast<uint32_t>(col_ptrs.size()),
                                      start_time, end_time, &err);
    if (rs == nullptr || err != E_OK) {
        return err != E_OK ? err : E_INVALID_ARG;
    }

    auto* ctx = new ResultSetCtx();
    ctx->rs = rs;
    ctx->mode = ResultSetMode::kRow;
    ctx->value_column_count = static_cast<int32_t>(cols.size());
    // Cache column types from metadata for fast type queries.
    // cwrapper metadata/result indexing is 1-based; column 1 is the
    // timestamp. This shim exposes 0-based indices to LabVIEW (column 0 =
    // timestamp), translating to cwrapper's 1-based indices internally.
    ResultSetMetaData meta = tsfile_result_set_get_metadata(rs);
    int cnum = tsfile_result_set_metadata_get_column_num(meta);
    ctx->col_types.reserve(cnum > 0 ? cnum : 0);
    for (int i = 1; i <= cnum; ++i) {
        ctx->col_types.push_back(
            tsfile_result_set_metadata_get_data_type(meta, i));
    }
    free_result_set_meta_data(meta);

    *out_result_set = register_handle(Kind::kResultSet, ctx);
    return E_OK;
}

LV_Status lv_tsfile_query_table_batch(LV_Handle reader, const char* table_name,
                                      const char* columns_newline_separated,
                                      int64_t start_time, int64_t end_time,
                                      int32_t batch_rows,
                                      LV_Handle* out_result_set) {
    if (out_result_set == nullptr) {
        return E_INVALID_ARG;
    }
    *out_result_set = 0;
    auto* rctx = static_cast<ReaderCtx*>(lookup(reader, Kind::kReader));
    if (rctx == nullptr || table_name == nullptr || batch_rows <= 0) {
        return E_INVALID_ARG;
    }

    try {
        std::vector<std::string> cols = split_lines(columns_newline_separated);
        if (cols.empty() ||
            cols.size() >
                static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
            return E_INVALID_ARG;
        }
        std::vector<char*> col_ptrs(cols.size());
        for (size_t i = 0; i < cols.size(); ++i) {
            if (cols[i].empty()) {
                return E_INVALID_ARG;
            }
            col_ptrs[i] = const_cast<char*>(cols[i].c_str());
        }

        ERRNO err = E_OK;
        ResultSet result_set = tsfile_query_table_batch(
            rctx->reader, table_name, col_ptrs.data(),
            static_cast<uint32_t>(col_ptrs.size()), start_time, end_time,
            nullptr, batch_rows, &err);
        if (result_set == nullptr || err != E_OK) {
            return err != E_OK ? err : E_INVALID_ARG;
        }

        ResultSetCtx* ctx = nullptr;
        try {
            ctx = new ResultSetCtx();
            ctx->rs = result_set;
            ctx->mode = ResultSetMode::kBatch;
            ctx->batch_rows = batch_rows;
            ctx->value_column_count = static_cast<int32_t>(cols.size());

            ResultSetMetaData meta = tsfile_result_set_get_metadata(result_set);
            int column_count = tsfile_result_set_metadata_get_column_num(meta);
            ctx->col_types.reserve(column_count > 0 ? column_count : 0);
            for (int i = 1; i <= column_count; ++i) {
                ctx->col_types.push_back(
                    tsfile_result_set_metadata_get_data_type(meta, i));
            }
            free_result_set_meta_data(meta);
            *out_result_set = register_handle(Kind::kResultSet, ctx);
            return E_OK;
        } catch (...) {
            delete ctx;
            free_tsfile_result_set(&result_set);
            throw;
        }
    } catch (const std::bad_alloc&) {
        return RET_OOM;
    } catch (...) {
        return E_INVALID_ARG;
    }
}

/* ===================== result set ===================== */
int32_t lv_tsfile_rs_next(LV_Handle rs, LV_Status* out_err) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        if (out_err != nullptr) {
            *out_err = E_INVALID_ARG;
        }
        return 0;
    }
    ERRNO err = E_OK;
    bool has = tsfile_result_set_next(ctx->rs, &err);
    if (out_err != nullptr) {
        *out_err = err;
    }
    return has ? 1 : 0;
}

int32_t lv_tsfile_rs_column_count(LV_Handle rs) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr) {
        return -1;
    }
    return static_cast<int32_t>(ctx->col_types.size());
}

uint8_t lv_tsfile_rs_column_type(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || col >= ctx->col_types.size()) {
        return 255;  // TS_DATATYPE_INVALID
    }
    return static_cast<uint8_t>(ctx->col_types[col]);
}

int32_t lv_tsfile_rs_is_null(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return 1;
    }
    return tsfile_result_set_is_null_by_index(ctx->rs, col + 1) ? 1 : 0;
}

int32_t lv_tsfile_rs_get_i32(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return 0;
    }
    return tsfile_result_set_get_value_by_index_int32_t(ctx->rs, col + 1);
}

int64_t lv_tsfile_rs_get_i64(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return 0;
    }
    return tsfile_result_set_get_value_by_index_int64_t(ctx->rs, col + 1);
}

float lv_tsfile_rs_get_f32(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return 0.0f;
    }
    return tsfile_result_set_get_value_by_index_float(ctx->rs, col + 1);
}

double lv_tsfile_rs_get_f64(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return 0.0;
    }
    return tsfile_result_set_get_value_by_index_double(ctx->rs, col + 1);
}

int32_t lv_tsfile_rs_get_bool(LV_Handle rs, uint32_t col) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return 0;
    }
    return tsfile_result_set_get_value_by_index_bool(ctx->rs, col + 1) ? 1 : 0;
}

LV_Status lv_tsfile_rs_get_str(LV_Handle rs, uint32_t col, char* out_buf,
                               int32_t buf_size, int32_t* out_actual_len) {
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kRow) {
        return E_INVALID_ARG;
    }
    char* val = tsfile_result_set_get_value_by_index_string(ctx->rs, col + 1);
    if (val == nullptr) {
        if (out_actual_len != nullptr) {
            *out_actual_len = 0;
        }
        if (out_buf != nullptr && buf_size > 0) {
            out_buf[0] = '\0';
        }
        return E_OK;
    }
    int32_t full = static_cast<int32_t>(std::strlen(val));
    if (out_actual_len != nullptr) {
        *out_actual_len = full;
    }
    if (out_buf != nullptr && buf_size > 0) {
        int32_t copy = full < (buf_size - 1) ? full : (buf_size - 1);
        std::memcpy(out_buf, val, copy);
        out_buf[copy] = '\0';
    }
    std::free(val);  // cwrapper transfers ownership of this string
    return E_OK;
}

namespace {

template <typename T>
LV_Status read_numeric_block(LV_Handle rs, int64_t* out_ts, T* out_data,
                             uint8_t* out_is_null, int32_t capacity_rows,
                             int32_t ncols, int32_t* out_rows,
                             TSDataType expected_type) {
    if (out_rows == nullptr) {
        return E_INVALID_ARG;
    }
    *out_rows = 0;
    auto* ctx = static_cast<ResultSetCtx*>(lookup(rs, Kind::kResultSet));
    if (ctx == nullptr || ctx->mode != ResultSetMode::kBatch ||
        out_ts == nullptr || out_data == nullptr || capacity_rows <= 0 ||
        ncols <= 0 || capacity_rows < ctx->batch_rows ||
        ncols != ctx->value_column_count) {
        return E_INVALID_ARG;
    }
    const size_t rows = static_cast<size_t>(capacity_rows);
    const size_t cols = static_cast<size_t>(ncols);
    if (rows > std::numeric_limits<size_t>::max() / cols ||
        rows * cols > std::numeric_limits<size_t>::max() / sizeof(T) ||
        ctx->col_types.size() != cols + 1) {
        return E_INVALID_ARG;
    }
    for (size_t col = 1; col < ctx->col_types.size(); ++col) {
        if (ctx->col_types[col] != expected_type) {
            return RET_TYPE_NOT_MATCH;
        }
    }

    try {
        uint32_t copied_rows = 0;
        ERRNO err = tsfile_result_set_read_numeric_block(
            ctx->rs, expected_type, out_ts, out_data, out_is_null,
            static_cast<uint32_t>(capacity_rows), static_cast<uint32_t>(ncols),
            &copied_rows);
        if (err == E_OK) {
            *out_rows = static_cast<int32_t>(copied_rows);
        }
        return err;
    } catch (const std::bad_alloc&) {
        return RET_OOM;
    } catch (...) {
        return E_INVALID_ARG;
    }
}

}  // namespace

LV_Status lv_tsfile_rs_read_block_i32(LV_Handle rs, int64_t* out_ts,
                                      int32_t* out_data, uint8_t* out_is_null,
                                      int32_t capacity_rows, int32_t ncols,
                                      int32_t* out_rows) {
    return read_numeric_block(rs, out_ts, out_data, out_is_null, capacity_rows,
                              ncols, out_rows, TS_DATATYPE_INT32);
}

LV_Status lv_tsfile_rs_read_block_f32(LV_Handle rs, int64_t* out_ts,
                                      float* out_data, uint8_t* out_is_null,
                                      int32_t capacity_rows, int32_t ncols,
                                      int32_t* out_rows) {
    return read_numeric_block(rs, out_ts, out_data, out_is_null, capacity_rows,
                              ncols, out_rows, TS_DATATYPE_FLOAT);
}

LV_Status lv_tsfile_rs_read_block_f64(LV_Handle rs, int64_t* out_ts,
                                      double* out_data, uint8_t* out_is_null,
                                      int32_t capacity_rows, int32_t ncols,
                                      int32_t* out_rows) {
    return read_numeric_block(rs, out_ts, out_data, out_is_null, capacity_rows,
                              ncols, out_rows, TS_DATATYPE_DOUBLE);
}

void lv_tsfile_rs_free(LV_Handle rs) {
    auto* ctx = static_cast<ResultSetCtx*>(unregister(rs, Kind::kResultSet));
    if (ctx == nullptr) {
        return;
    }
    if (ctx->rs != nullptr) {
        free_tsfile_result_set(&ctx->rs);
    }
    delete ctx;
}

/* ===================== one-call convenience ===================== */
LV_Status lv_tsfile_dump_to_csv(const char* tsfile_path,
                                const char* csv_out_path) {
    if (tsfile_path == nullptr || csv_out_path == nullptr) {
        return E_INVALID_ARG;
    }

    ERRNO err = E_OK;
    TsFileReader reader = tsfile_reader_new(tsfile_path, &err);
    if (reader == nullptr || err != E_OK) {
        return err != E_OK ? err : E_INVALID_ARG;
    }

    // 1) Discover the first table name via the device list.
    DeviceID* devices = nullptr;
    uint32_t ndev = 0;
    err = tsfile_reader_get_all_devices(reader, &devices, &ndev);
    if (err != E_OK || devices == nullptr || ndev == 0) {
        if (devices != nullptr) {
            tsfile_free_device_id_array(devices, ndev);
        }
        tsfile_reader_close(reader);
        return err != E_OK ? err : E_INVALID_ARG;
    }
    std::string table_name =
        devices[0].table_name != nullptr ? devices[0].table_name : "";
    tsfile_free_device_id_array(devices, ndev);
    if (table_name.empty()) {
        tsfile_reader_close(reader);
        return E_INVALID_ARG;
    }

    // 2) Fetch the table schema to learn every column name.
    TableSchema schema =
        tsfile_reader_get_table_schema(reader, table_name.c_str());
    std::vector<std::string> cols;
    for (int i = 0; i < schema.column_num; ++i) {
        if (schema.column_schemas != nullptr &&
            schema.column_schemas[i].column_name != nullptr) {
            cols.emplace_back(schema.column_schemas[i].column_name);
        }
    }
    free_table_schema(schema);
    if (cols.empty()) {
        tsfile_reader_close(reader);
        return E_INVALID_ARG;
    }

    std::vector<char*> col_ptrs(cols.size());
    for (size_t i = 0; i < cols.size(); ++i) {
        col_ptrs[i] = const_cast<char*>(cols[i].c_str());
    }

    // 3) Query the whole time range for those columns.
    ResultSet rs = tsfile_query_table(
        reader, table_name.c_str(), col_ptrs.data(),
        static_cast<uint32_t>(col_ptrs.size()), LLONG_MIN, LLONG_MAX, &err);
    if (rs == nullptr || err != E_OK) {
        tsfile_reader_close(reader);
        return err != E_OK ? err : E_INVALID_ARG;
    }

    // 4) Read result-set metadata (cwrapper indexing is 1-based; column 1 is
    // the timestamp).
    ResultSetMetaData meta = tsfile_result_set_get_metadata(rs);
    int cnum = tsfile_result_set_metadata_get_column_num(meta);
    if (cnum <= 0) {
        free_result_set_meta_data(meta);
        free_tsfile_result_set(&rs);
        tsfile_reader_close(reader);
        return E_INVALID_ARG;
    }
    std::vector<TSDataType> types(static_cast<size_t>(cnum) + 1,
                                  TS_DATATYPE_INVALID);

    FILE* f = std::fopen(csv_out_path, "w");
    if (f == nullptr) {
        free_result_set_meta_data(meta);
        free_tsfile_result_set(&rs);
        tsfile_reader_close(reader);
        return E_INVALID_ARG;
    }

    // Header row + cache column types.
    for (int i = 1; i <= cnum; ++i) {
        char* nm = tsfile_result_set_metadata_get_column_name(meta, i);
        types[i] = tsfile_result_set_metadata_get_data_type(meta, i);
        if (i > 1) {
            std::fputc(',', f);
        }
        if (nm != nullptr) {
            std::fputs(nm, f);
        }
    }
    std::fputc('\n', f);
    free_result_set_meta_data(meta);

    // 5) Stream every row.
    err = E_OK;
    while (tsfile_result_set_next(rs, &err) && err == E_OK) {
        for (int i = 1; i <= cnum; ++i) {
            if (i > 1) {
                std::fputc(',', f);
            }
            if (tsfile_result_set_is_null_by_index(rs, i)) {
                continue;
            }
            switch (types[i]) {
                case TS_DATATYPE_BOOLEAN:
                    std::fprintf(
                        f, "%d",
                        tsfile_result_set_get_value_by_index_bool(rs, i) ? 1
                                                                         : 0);
                    break;
                case TS_DATATYPE_INT32:
                    std::fprintf(
                        f, "%d",
                        tsfile_result_set_get_value_by_index_int32_t(rs, i));
                    break;
                case TS_DATATYPE_INT64:
                case TS_DATATYPE_TIMESTAMP:
                    std::fprintf(
                        f, "%lld",
                        static_cast<long long>(
                            tsfile_result_set_get_value_by_index_int64_t(rs,
                                                                         i)));
                    break;
                case TS_DATATYPE_FLOAT:
                    std::fprintf(
                        f, "%g",
                        static_cast<double>(
                            tsfile_result_set_get_value_by_index_float(rs, i)));
                    break;
                case TS_DATATYPE_DOUBLE:
                    std::fprintf(
                        f, "%g",
                        tsfile_result_set_get_value_by_index_double(rs, i));
                    break;
                case TS_DATATYPE_TEXT:
                case TS_DATATYPE_STRING:
                case TS_DATATYPE_BLOB: {
                    char* sv =
                        tsfile_result_set_get_value_by_index_string(rs, i);
                    if (sv != nullptr) {
                        std::fputs(sv, f);
                        std::free(sv);
                    }
                    break;
                }
                default:
                    break;
            }
        }
        std::fputc('\n', f);
    }

    std::fclose(f);
    free_tsfile_result_set(&rs);
    tsfile_reader_close(reader);
    return err;
}
