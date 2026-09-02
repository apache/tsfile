/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif

#include "common/schema.h"
#include "common/tablet.h"
#include "common/tsfile_common.h"
#include "file/write_file.h"
#include "reader/filter/tag_filter.h"
#include "reader/result_set.h"
#include "reader/tsfile_reader.h"
#include "utils/db_utils.h"
#include "writer/tsfile_table_writer.h"

namespace {

using common::ColumnCategory;
using common::ColumnSchema;
using common::TSDataType;
using storage::ResultSet;
using storage::TableSchema;
using storage::Tablet;
using storage::TsFileReader;
using storage::TsFileTableWriter;
using storage::WriteFile;

const int64_t kNoWatermark = std::numeric_limits<int64_t>::min();

struct Column {
    std::string name;
    TSDataType type;
    ColumnCategory category;
};

struct Value {
    TSDataType type = common::NULL_TYPE;
    bool is_null = true;
    bool b = false;
    int32_t i32 = 0;
    int64_t i64 = 0;
    float f = 0;
    double d = 0;
    std::string bytes;
};

struct Row {
    sqlite3_int64 rowid = 0;
    std::vector<Value> values;
};

struct PendingFile {
    std::string temporary;
    std::string final_path;
    bool renamed = false;
};

struct HybridTable;

struct HybridCursor : sqlite3_vtab_cursor {
    HybridTable* table = nullptr;
    std::vector<Row> rows;
    size_t pos = 0;
};

struct HybridTable : sqlite3_vtab {
    sqlite3* db = nullptr;
    std::string db_name;
    std::string table_name;
    std::string directory;
    std::string precision;
    std::vector<Column> columns;
    int time_index = -1;
    std::vector<int> tag_indexes;
    int64_t watermark = kNoWatermark;
    std::vector<PendingFile> pending;
    std::vector<size_t> savepoint_marks;
    uint64_t file_counter = 0;
};

struct ConstraintSpec {
    int column;
    int op;
    int argv_index;
    ConstraintSpec(int column_value, int op_value, int argv_value)
        : column(column_value), op(op_value), argv_index(argv_value) {}
};

std::string lower(std::string value) {
    for (char& c : value)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return value;
}

std::string quote_id(const std::string& value) {
    std::string out = "\"";
    for (char c : value) {
        if (c == '"')
            out += "\"\"";
        else
            out += c;
    }
    out += '"';
    return out;
}

std::string quote_sql(const std::string& value) {
    std::string out = "'";
    for (char c : value) {
        if (c == '\'')
            out += "''";
        else
            out += c;
    }
    out += '\'';
    return out;
}

void copy_bytes(std::string& destination, const void* data, int length) {
    destination.clear();
    if (data != nullptr && length > 0)
        destination.assign(static_cast<const char*>(data),
                           static_cast<size_t>(length));
}

void set_error(sqlite3_vtab* vtab, const std::string& message) {
    if (vtab == nullptr) return;
    if (vtab->zErrMsg != nullptr) sqlite3_free(vtab->zErrMsg);
    vtab->zErrMsg = sqlite3_mprintf("%s", message.c_str());
}

int exec_sql(sqlite3* db, const std::string& sql,
             sqlite3_vtab* vtab = nullptr) {
    char* error = nullptr;
    int rc = sqlite3_exec(db, sql.c_str(), nullptr, nullptr, &error);
    if (rc != SQLITE_OK && vtab != nullptr) {
        set_error(vtab, error == nullptr ? "SQLite error" : error);
    }
    sqlite3_free(error);
    return rc;
}

bool parse_key_value(const char* arg, std::string& key, std::string& value) {
    if (arg == nullptr) return false;
    const char* equal = std::strchr(arg, '=');
    if (equal == nullptr) return false;
    key.assign(arg, static_cast<size_t>(equal - arg));
    value.assign(equal + 1);
    if (value.size() >= 2 && ((value.front() == '\'' && value.back() == '\'') ||
                              (value.front() == '"' && value.back() == '"'))) {
        value = value.substr(1, value.size() - 2);
    }
    return true;
}

bool parse_column(const std::string& spec, Column& column, std::string& error) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (start <= spec.size()) {
        size_t end = spec.find(':', start);
        parts.push_back(spec.substr(
            start, end == std::string::npos ? std::string::npos : end - start));
        if (end == std::string::npos) break;
        start = end + 1;
    }
    if (parts.size() != 3 || parts[0].empty()) {
        error = "column must be name:TYPE:CATEGORY";
        return false;
    }
    TSDataType type;
    if (!common::parse_data_type_name(parts[1], type)) {
        error = "unsupported TsFile column type: " + parts[1];
        return false;
    }
    std::string category = lower(parts[2]);
    ColumnCategory col_category;
    if (category == "time")
        col_category = ColumnCategory::TIME;
    else if (category == "tag")
        col_category = ColumnCategory::TAG;
    else if (category == "field")
        col_category = ColumnCategory::FIELD;
    else {
        error = "column category must be TIME, TAG, or FIELD";
        return false;
    }
    column = {parts[0], type, col_category};
    return true;
}

std::string sqlite_type(TSDataType type) {
    switch (type) {
        case common::BOOLEAN:
            return "INTEGER";
        case common::INT32:
        case common::INT64:
        case common::DATE:
        case common::TIMESTAMP:
            return "INTEGER";
        case common::FLOAT:
        case common::DOUBLE:
            return "REAL";
        case common::TEXT:
        case common::STRING:
            return "TEXT";
        case common::BLOB:
            return "BLOB";
        default:
            return "";
    }
}

std::string shadow_name(const HybridTable* table, const char* suffix) {
    return quote_id(table->db_name) + "." +
           quote_id(table->table_name + suffix);
}

std::string schema_signature(const HybridTable* table) {
    std::ostringstream out;
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (i) out << ';';
        out << table->columns[i].name << ':'
            << static_cast<int>(table->columns[i].type) << ':'
            << static_cast<int>(table->columns[i].category);
    }
    return out.str();
}

std::string file_stem(const std::string& table_name) {
    std::string stem = "tsfile";
    for (char c : table_name) {
        if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '-')
            stem += c;
        else
            stem += '_';
    }
    return stem;
}

bool ensure_directory(const std::string& path) {
    if (path.empty() || path[0] != '/') return false;
    size_t pos = 1;
    while (pos <= path.size()) {
        pos = path.find('/', pos);
        std::string part =
            path.substr(0, pos == std::string::npos ? path.size() : pos);
        if (!part.empty() && mkdir(part.c_str(), 0755) != 0 && errno != EEXIST)
            return false;
        if (pos == std::string::npos) break;
        ++pos;
    }
    struct stat st {};
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool directory_exists(const std::string& path) {
    struct stat st {};
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

int sync_directory(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) return SQLITE_IOERR_FSYNC;
    int rc = fsync(fd) == 0 ? SQLITE_OK : SQLITE_IOERR_FSYNC;
    close(fd);
    return rc;
}

int close_writer_and_sync(WriteFile& write_file, TsFileTableWriter& writer) {
    // Keep a duplicate descriptor because TsFileTableWriter::close() writes
    // the footer and closes the original descriptor. This explicit fsync is
    // independent of libtsfile's process-global sync_on_close setting.
    int sync_fd = dup(write_file.get_fd());
    if (sync_fd < 0) return SQLITE_IOERR_FSYNC;
    int close_rc = writer.close();
    int sync_rc = (close_rc == common::E_OK && fsync(sync_fd) == 0)
                      ? SQLITE_OK
                      : SQLITE_IOERR_FSYNC;
    int close_fd_rc = close(sync_fd) == 0 ? SQLITE_OK : SQLITE_IOERR;
    if (close_rc != common::E_OK) return SQLITE_IOERR;
    if (sync_rc != SQLITE_OK) return sync_rc;
    return close_fd_rc;
}

bool value_from_sqlite(sqlite3_value* value, const Column& column, Value& out,
                       std::string& error) {
    int type = sqlite3_value_type(value);
    if (type == SQLITE_NULL) {
        out.type = column.type;
        out.is_null = true;
        return true;
    }
    out.type = column.type;
    out.is_null = false;
    switch (column.type) {
        case common::BOOLEAN:
            if (type != SQLITE_INTEGER) {
                error = "BOOLEAN requires INTEGER";
                return false;
            }
            out.b = sqlite3_value_int(value) != 0;
            break;
        case common::INT32:
        case common::DATE:
            if (type != SQLITE_INTEGER) {
                error = "INT32/DATE requires INTEGER";
                return false;
            }
            {
                sqlite3_int64 number = sqlite3_value_int64(value);
                if (number < std::numeric_limits<int32_t>::min() ||
                    number > std::numeric_limits<int32_t>::max()) {
                    error = "INT32/DATE value is out of range";
                    return false;
                }
                out.i32 = static_cast<int32_t>(number);
            }
            break;
        case common::INT64:
        case common::TIMESTAMP:
            if (type != SQLITE_INTEGER) {
                error = "INT64/TIMESTAMP requires INTEGER";
                return false;
            }
            out.i64 = sqlite3_value_int64(value);
            break;
        case common::FLOAT:
            if (type != SQLITE_INTEGER && type != SQLITE_FLOAT) {
                error = "FLOAT requires numeric value";
                return false;
            }
            out.f = static_cast<float>(sqlite3_value_double(value));
            break;
        case common::DOUBLE:
            if (type != SQLITE_INTEGER && type != SQLITE_FLOAT) {
                error = "DOUBLE requires numeric value";
                return false;
            }
            out.d = sqlite3_value_double(value);
            break;
        case common::TEXT:
        case common::STRING: {
            if (type != SQLITE_TEXT) {
                error = "TEXT/STRING requires TEXT";
                return false;
            }
            const unsigned char* text = sqlite3_value_text(value);
            copy_bytes(out.bytes, text, sqlite3_value_bytes(value));
            break;
        }
        case common::BLOB: {
            if (type != SQLITE_BLOB) {
                error = "BLOB requires BLOB";
                return false;
            }
            const void* blob = sqlite3_value_blob(value);
            copy_bytes(out.bytes, blob, sqlite3_value_bytes(value));
            break;
        }
        default:
            error = "unsupported column type";
            return false;
    }
    return true;
}

void bind_value(sqlite3_stmt* stmt, int index, const Value& value) {
    if (value.is_null) {
        sqlite3_bind_null(stmt, index);
        return;
    }
    switch (value.type) {
        case common::BOOLEAN:
            sqlite3_bind_int(stmt, index, value.b ? 1 : 0);
            break;
        case common::INT32:
        case common::DATE:
            sqlite3_bind_int(stmt, index, value.i32);
            break;
        case common::INT64:
        case common::TIMESTAMP:
            sqlite3_bind_int64(stmt, index, value.i64);
            break;
        case common::FLOAT:
            sqlite3_bind_double(stmt, index, value.f);
            break;
        case common::DOUBLE:
            sqlite3_bind_double(stmt, index, value.d);
            break;
        case common::TEXT:
        case common::STRING:
            sqlite3_bind_text(stmt, index, value.bytes.data(),
                              static_cast<int>(value.bytes.size()),
                              SQLITE_TRANSIENT);
            break;
        case common::BLOB:
            sqlite3_bind_blob(stmt, index, value.bytes.data(),
                              static_cast<int>(value.bytes.size()),
                              SQLITE_TRANSIENT);
            break;
        default:
            sqlite3_bind_null(stmt, index);
            break;
    }
}

bool row_value_from_sqlite(sqlite3_stmt* stmt, int index, const Column& column,
                           Value& out) {
    int type = sqlite3_column_type(stmt, index);
    out.type = column.type;
    out.is_null = type == SQLITE_NULL;
    if (out.is_null) return true;
    switch (column.type) {
        case common::BOOLEAN:
            out.b = sqlite3_column_int(stmt, index) != 0;
            break;
        case common::INT32:
        case common::DATE:
            out.i32 = sqlite3_column_int(stmt, index);
            break;
        case common::INT64:
        case common::TIMESTAMP:
            out.i64 = sqlite3_column_int64(stmt, index);
            break;
        case common::FLOAT:
            out.f = static_cast<float>(sqlite3_column_double(stmt, index));
            break;
        case common::DOUBLE:
            out.d = sqlite3_column_double(stmt, index);
            break;
        case common::TEXT:
        case common::STRING: {
            const unsigned char* text = sqlite3_column_text(stmt, index);
            copy_bytes(out.bytes, text, sqlite3_column_bytes(stmt, index));
            break;
        }
        case common::BLOB: {
            const void* blob = sqlite3_column_blob(stmt, index);
            copy_bytes(out.bytes, blob, sqlite3_column_bytes(stmt, index));
            break;
        }
        default:
            return false;
    }
    return true;
}

int create_shadow_tables(HybridTable* table, bool insert_config) {
    std::ostringstream data;
    data << "CREATE TABLE " << shadow_name(table, "_data") << " (";
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (i) data << ',';
        data << quote_id(table->columns[i].name) << ' '
             << sqlite_type(table->columns[i].type);
        if (table->columns[i].category == ColumnCategory::TIME ||
            table->columns[i].category == ColumnCategory::TAG)
            data << " NOT NULL";
    }
    data << ", UNIQUE(";
    bool first = true;
    for (int index : table->tag_indexes) {
        if (!first) data << ',';
        first = false;
        data << quote_id(table->columns[index].name);
    }
    data << ',' << quote_id(table->columns[table->time_index].name) << "))";
    int rc = exec_sql(table->db, data.str(), table);
    if (rc != SQLITE_OK) return rc;

    std::ostringstream segments;
    segments << "CREATE TABLE " << shadow_name(table, "_segments")
             << " (path TEXT PRIMARY KEY, cutoff INTEGER NOT NULL, row_count "
                "INTEGER NOT NULL)";
    rc = exec_sql(table->db, segments.str(), table);
    if (rc != SQLITE_OK) return rc;

    std::ostringstream config;
    config << "CREATE TABLE " << shadow_name(table, "_config")
           << " (id INTEGER PRIMARY KEY CHECK(id=1), watermark INTEGER NOT "
              "NULL, precision TEXT NOT NULL, directory TEXT NOT NULL, schema "
              "TEXT NOT NULL)";
    rc = exec_sql(table->db, config.str(), table);
    if (rc != SQLITE_OK) return rc;
    if (insert_config) {
        std::ostringstream insert;
        insert << "INSERT INTO " << shadow_name(table, "_config")
               << "(id,watermark,precision,directory,schema) VALUES(1,"
               << kNoWatermark << ',' << quote_sql(table->precision) << ','
               << quote_sql(table->directory) << ','
               << quote_sql(schema_signature(table)) << ')';
        rc = exec_sql(table->db, insert.str(), table);
        if (rc != SQLITE_OK) return rc;
    }
    return SQLITE_OK;
}

int load_config(HybridTable* table) {
    std::string sql = "SELECT watermark,precision,directory,schema FROM " +
                      shadow_name(table, "_config") + " WHERE id=1";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return rc;
    rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        table->watermark = sqlite3_column_int64(stmt, 0);
        const unsigned char* precision = sqlite3_column_text(stmt, 1);
        std::string stored_precision =
            precision == nullptr ? ""
                                 : reinterpret_cast<const char*>(precision);
        const unsigned char* directory = sqlite3_column_text(stmt, 2);
        const unsigned char* schema = sqlite3_column_text(stmt, 3);
        std::string stored_directory =
            directory == nullptr ? ""
                                 : reinterpret_cast<const char*>(directory);
        std::string stored_schema =
            schema == nullptr ? "" : reinterpret_cast<const char*>(schema);
        if (stored_precision != table->precision ||
            stored_directory != table->directory ||
            stored_schema != schema_signature(table)) {
            rc = SQLITE_CORRUPT;
        } else {
            table->precision = stored_precision;
            rc = SQLITE_OK;
        }
    } else if (rc == SQLITE_DONE) {
        rc = SQLITE_CORRUPT;
    }
    sqlite3_finalize(stmt);
    return rc;
}

int declare_table(HybridTable* table) {
    std::ostringstream sql;
    sql << "CREATE TABLE x(";
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (i) sql << ',';
        sql << quote_id(table->columns[i].name) << ' '
            << sqlite_type(table->columns[i].type);
        if (table->columns[i].category == ColumnCategory::TIME ||
            table->columns[i].category == ColumnCategory::TAG)
            sql << " NOT NULL";
    }
    sql << ",_tsfile_command TEXT HIDDEN,_tsfile_cutoff INTEGER HIDDEN)";
    return sqlite3_declare_vtab(table->db, sql.str().c_str());
}

bool parse_args(HybridTable* table, int argc, const char* const* argv,
                std::string& error) {
    for (int i = 3; i < argc; ++i) {
        std::string key, value;
        if (!parse_key_value(argv[i], key, value)) {
            error = "module arguments must be key=value";
            return false;
        }
        key = lower(key);
        if (key == "directory")
            table->directory = value;
        else if (key == "timestamp_precision")
            table->precision = lower(value);
        else if (key == "column") {
            Column column;
            if (!parse_column(value, column, error)) return false;
            table->columns.push_back(column);
        } else {
            error = "unknown tsfile_hybrid option: " + key;
            return false;
        }
    }
    if (table->directory.empty() || table->directory[0] != '/') {
        error = "directory must be an absolute path";
        return false;
    }
    if (table->precision != "ms" && table->precision != "us" &&
        table->precision != "ns") {
        error = "timestamp_precision must be ms, us, or ns";
        return false;
    }
    if (table->columns.empty() ||
        table->columns[0].category != ColumnCategory::TIME ||
        table->columns[0].type != common::TIMESTAMP) {
        error = "the first column must be TIMESTAMP:TIME";
        return false;
    }
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (table->columns[i].name.empty()) {
            error = "column name cannot be empty";
            return false;
        }
        if (lower(table->columns[i].name) == "_tsfile_command" ||
            lower(table->columns[i].name) == "_tsfile_cutoff") {
            error = "column name is reserved by tsfile_hybrid";
            return false;
        }
        for (size_t j = 0; j < i; ++j)
            if (lower(table->columns[i].name) ==
                lower(table->columns[j].name)) {
                error = "duplicate column name";
                return false;
            }
        if (table->columns[i].category == ColumnCategory::TIME) {
            if (table->time_index != -1) {
                error = "exactly one TIME column is required";
                return false;
            }
            table->time_index = static_cast<int>(i);
        } else if (table->columns[i].category == ColumnCategory::TAG) {
            if (table->columns[i].type != common::STRING) {
                error = "TAG columns must use STRING";
                return false;
            }
            table->tag_indexes.push_back(static_cast<int>(i));
        }
    }
    if (table->time_index != 0) {
        error = "TIME column must be the first column";
        return false;
    }
    if (table->tag_indexes.empty()) {
        error = "at least one TAG column is required";
        return false;
    }
    return true;
}

int init_table(HybridTable* table, int argc, const char* const* argv,
               bool create, char** error_message) {
    table->db_name = argv[1] == nullptr ? "main" : argv[1];
    table->table_name = argv[2] == nullptr ? "" : argv[2];
    std::string error;
    if (!parse_args(table, argc, argv, error)) {
        if (error_message)
            *error_message = sqlite3_mprintf("%s", error.c_str());
        return SQLITE_ERROR;
    }
    if (create && !ensure_directory(table->directory)) {
        if (error_message)
            *error_message = sqlite3_mprintf("cannot create TsFile directory");
        return SQLITE_CANTOPEN;
    }
    if (!create && !directory_exists(table->directory)) {
        if (error_message)
            *error_message = sqlite3_mprintf("TsFile directory does not exist");
        return SQLITE_CANTOPEN;
    }
    if (declare_table(table) != SQLITE_OK) return SQLITE_ERROR;
    sqlite3_vtab_config(table->db, SQLITE_VTAB_DIRECTONLY);
    sqlite3_vtab_config(table->db, SQLITE_VTAB_CONSTRAINT_SUPPORT, 1);
    if (create) {
        int rc = create_shadow_tables(table, true);
        if (rc != SQLITE_OK) return rc;
    } else {
        int rc = load_config(table);
        if (rc != SQLITE_OK) return rc;
    }
    return SQLITE_OK;
}

int create_or_connect(sqlite3* db, void* aux, int argc, const char* const* argv,
                      sqlite3_vtab** vtab, char** error_message, bool create) {
    (void)aux;
    std::unique_ptr<HybridTable> table(new HybridTable());
    table->db = db;
    int rc = init_table(table.get(), argc, argv, create, error_message);
    if (rc != SQLITE_OK) return rc;
    *vtab = table.release();
    return SQLITE_OK;
}

int xCreate(sqlite3* db, void* aux, int argc, const char* const* argv,
            sqlite3_vtab** vtab, char** error) {
    return create_or_connect(db, aux, argc, argv, vtab, error, true);
}
int xConnect(sqlite3* db, void* aux, int argc, const char* const* argv,
             sqlite3_vtab** vtab, char** error) {
    return create_or_connect(db, aux, argc, argv, vtab, error, false);
}

int xDisconnect(sqlite3_vtab* vtab) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    delete table;
    return SQLITE_OK;
}

int xDestroy(sqlite3_vtab* vtab) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    int rc =
        exec_sql(table->db,
                 "DROP TABLE IF EXISTS " + shadow_name(table, "_data"), table);
    if (rc == SQLITE_OK)
        rc = exec_sql(table->db,
                      "DROP TABLE IF EXISTS " + shadow_name(table, "_segments"),
                      table);
    if (rc == SQLITE_OK)
        rc = exec_sql(table->db,
                      "DROP TABLE IF EXISTS " + shadow_name(table, "_config"),
                      table);
    delete table;
    return rc;
}

int xBestIndex(sqlite3_vtab* vtab, sqlite3_index_info* info) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    std::vector<ConstraintSpec> specs;
    int next_arg = 1;
    for (int i = 0; i < info->nConstraint; ++i) {
        const auto& constraint = info->aConstraint[i];
        if (!constraint.usable || constraint.iColumn < 0 ||
            constraint.iColumn >= static_cast<int>(table->columns.size()))
            continue;
        bool accepted = constraint.iColumn == table->time_index &&
                        (constraint.op == SQLITE_INDEX_CONSTRAINT_GE ||
                         constraint.op == SQLITE_INDEX_CONSTRAINT_GT ||
                         constraint.op == SQLITE_INDEX_CONSTRAINT_LE ||
                         constraint.op == SQLITE_INDEX_CONSTRAINT_LT);
        accepted =
            accepted ||
            (constraint.op == SQLITE_INDEX_CONSTRAINT_EQ &&
             std::find(table->tag_indexes.begin(), table->tag_indexes.end(),
                       constraint.iColumn) != table->tag_indexes.end() &&
             (sqlite3_vtab_collation(info, i) == nullptr ||
              lower(sqlite3_vtab_collation(info, i)) == "binary"));
        if (!accepted) continue;
        info->aConstraintUsage[i].argvIndex = next_arg++;
        info->aConstraintUsage[i].omit = 0;
        specs.push_back({constraint.iColumn, constraint.op,
                         info->aConstraintUsage[i].argvIndex});
    }
    std::vector<int> projection;
    const sqlite3_uint64 used_columns = info->colUsed;
    for (size_t i = 0; i < table->columns.size(); ++i) {
        const bool used = i >= 64 || (used_columns & (1ULL << i)) != 0;
        if (used) projection.push_back(static_cast<int>(i));
    }
    auto add_projection_column = [&projection](int column) {
        if (std::find(projection.begin(), projection.end(), column) ==
            projection.end())
            projection.push_back(column);
    };
    // The time column is needed to apply the residual half-open range check;
    // accepted constraints are also kept available for the second check.
    add_projection_column(table->time_index);
    for (const ConstraintSpec& spec : specs) add_projection_column(spec.column);
    std::sort(projection.begin(), projection.end());
    if (specs.empty())
        info->estimatedCost = 1000000.0;
    else
        info->estimatedCost = 1000.0;
    std::ostringstream encoded;
    for (size_t i = 0; i < specs.size(); ++i) {
        if (i) encoded << ';';
        encoded << specs[i].column << ':' << specs[i].op << ':'
                << specs[i].argv_index;
    }
    encoded << "|p:";
    for (size_t i = 0; i < projection.size(); ++i) {
        if (i) encoded << ',';
        encoded << projection[i];
    }
    std::string text = encoded.str();
    if (!text.empty()) {
        info->idxStr = static_cast<char*>(sqlite3_malloc(text.size() + 1));
        if (info->idxStr == nullptr) return SQLITE_NOMEM;
        std::memcpy(info->idxStr, text.c_str(), text.size() + 1);
        info->needToFreeIdxStr = 1;
    }
    return SQLITE_OK;
}

int parse_specs(const char* encoded, std::vector<ConstraintSpec>& specs,
                std::vector<int>& projection) {
    if (encoded == nullptr || *encoded == '\0') return SQLITE_OK;
    std::string plan(encoded);
    const size_t projection_marker = plan.find("|p:");
    std::string spec_text = projection_marker == std::string::npos
                                ? plan
                                : plan.substr(0, projection_marker);
    if (projection_marker != std::string::npos) {
        std::stringstream projection_input(plan.substr(projection_marker + 3));
        std::string part;
        while (std::getline(projection_input, part, ',')) {
            if (part.empty()) return SQLITE_ERROR;
            projection.push_back(std::atoi(part.c_str()));
        }
    }
    std::stringstream input(spec_text);
    std::string token;
    while (std::getline(input, token, ';')) {
        std::stringstream item(token);
        std::string part;
        std::vector<int> values;
        while (std::getline(item, part, ':'))
            values.push_back(std::atoi(part.c_str()));
        if (values.size() != 3) return SQLITE_ERROR;
        specs.push_back({values[0], values[1], values[2]});
    }
    return SQLITE_OK;
}

void apply_constraint_bounds(const std::vector<ConstraintSpec>& specs,
                             sqlite3_value** argv, int64_t& lower_bound,
                             bool& lower_inclusive, int64_t& upper_bound,
                             bool& upper_inclusive,
                             std::vector<std::pair<int, std::string>>& tag_eq) {
    lower_bound = kNoWatermark;
    upper_bound = std::numeric_limits<int64_t>::max();
    lower_inclusive = true;
    upper_inclusive = true;
    for (const ConstraintSpec& spec : specs) {
        sqlite3_value* value = argv[spec.argv_index - 1];
        if (spec.column == 0 && sqlite3_value_type(value) == SQLITE_INTEGER) {
            int64_t number = sqlite3_value_int64(value);
            if (spec.op == SQLITE_INDEX_CONSTRAINT_GE ||
                spec.op == SQLITE_INDEX_CONSTRAINT_GT) {
                if (number > lower_bound ||
                    (number == lower_bound &&
                     spec.op == SQLITE_INDEX_CONSTRAINT_GE)) {
                    lower_bound = number;
                    lower_inclusive = spec.op == SQLITE_INDEX_CONSTRAINT_GE;
                }
            } else if (spec.op == SQLITE_INDEX_CONSTRAINT_LE ||
                       spec.op == SQLITE_INDEX_CONSTRAINT_LT) {
                if (number < upper_bound ||
                    (number == upper_bound &&
                     spec.op == SQLITE_INDEX_CONSTRAINT_LE)) {
                    upper_bound = number;
                    upper_inclusive = spec.op == SQLITE_INDEX_CONSTRAINT_LE;
                }
            }
        } else if (spec.op == SQLITE_INDEX_CONSTRAINT_EQ &&
                   sqlite3_value_type(value) == SQLITE_TEXT) {
            tag_eq.emplace_back(
                spec.column,
                std::string(
                    reinterpret_cast<const char*>(sqlite3_value_text(value)),
                    static_cast<size_t>(sqlite3_value_bytes(value))));
        }
    }
}

bool row_matches(const Row& row, const HybridTable* table, int64_t lower,
                 bool lower_inclusive, int64_t upper, bool upper_inclusive,
                 const std::vector<std::pair<int, std::string>>& tag_eq) {
    const Value& time = row.values[table->time_index];
    int64_t timestamp = time.type == common::TIMESTAMP ? time.i64 : 0;
    if (timestamp < lower || (!lower_inclusive && timestamp == lower))
        return false;
    if (timestamp > upper || (!upper_inclusive && timestamp == upper))
        return false;
    for (const auto& condition : tag_eq) {
        const Value& value = row.values[condition.first];
        if (value.is_null || value.bytes != condition.second) return false;
    }
    return true;
}

int read_hot(HybridCursor* cursor, const std::vector<ConstraintSpec>& specs,
             sqlite3_value** argv, int64_t lower, bool lower_inclusive,
             int64_t upper, bool upper_inclusive,
             const std::vector<std::pair<int, std::string>>& tag_eq,
             const std::vector<int>& projection) {
    HybridTable* table = cursor->table;
    std::ostringstream sql;
    sql << "SELECT ";
    for (size_t i = 0; i < projection.size(); ++i) {
        const int column = projection[i];
        if (i) sql << ',';
        sql << quote_id(table->columns[column].name);
    }
    sql << ",rowid FROM " << shadow_name(table, "_data");
    if (!specs.empty()) {
        sql << " WHERE ";
        for (size_t i = 0; i < specs.size(); ++i) {
            if (i) sql << " AND ";
            sql << quote_id(table->columns[specs[i].column].name);
            switch (specs[i].op) {
                case SQLITE_INDEX_CONSTRAINT_EQ:
                    sql << "=?";
                    break;
                case SQLITE_INDEX_CONSTRAINT_GE:
                    sql << ">=?";
                    break;
                case SQLITE_INDEX_CONSTRAINT_GT:
                    sql << ">?";
                    break;
                case SQLITE_INDEX_CONSTRAINT_LE:
                    sql << "<=?";
                    break;
                case SQLITE_INDEX_CONSTRAINT_LT:
                    sql << "<?";
                    break;
                default:
                    sql << "=?";
                    break;
            }
        }
    }
    sql << " ORDER BY rowid";
    sqlite3_stmt* stmt = nullptr;
    int rc =
        sqlite3_prepare_v2(table->db, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return rc;
    for (const ConstraintSpec& spec : specs)
        sqlite3_bind_value(stmt, spec.argv_index, argv[spec.argv_index - 1]);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        Row row;
        row.rowid =
            sqlite3_column_int64(stmt, static_cast<int>(projection.size()));
        row.values.resize(table->columns.size());
        bool valid = true;
        for (size_t i = 0; i < projection.size(); ++i) {
            const int column = projection[i];
            valid = valid && row_value_from_sqlite(stmt, static_cast<int>(i),
                                                   table->columns[column],
                                                   row.values[column]);
        }
        if (valid && row_matches(row, table, lower, lower_inclusive, upper,
                                 upper_inclusive, tag_eq))
            cursor->rows.push_back(std::move(row));
    }
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}

std::shared_ptr<TableSchema> build_tsfile_schema(const HybridTable* table) {
    std::vector<ColumnSchema> columns;
    for (size_t i = 1; i < table->columns.size(); ++i) {
        const Column& column = table->columns[i];
        columns.emplace_back(column.name, column.type, column.category);
    }
    return std::make_shared<TableSchema>(table->table_name, columns);
}

sqlite3_int64 cold_rowid(uint64_t segment_ordinal, uint64_t row_ordinal) {
    // Keep cold rowids negative and deterministic for a segment traversal.
    // The high/low split leaves enough room for ordinary segment and row
    // counts while avoiding collisions with SQLite's positive hot rowids.
    const uint64_t max_code =
        static_cast<uint64_t>(std::numeric_limits<sqlite3_int64>::max()) - 1;
    uint64_t code = (segment_ordinal << 32) | (row_ordinal & 0xffffffffULL);
    if (code > max_code) code = max_code;
    return -static_cast<sqlite3_int64>(code + 1);
}

int append_result_row(HybridCursor* cursor, ResultSet* result,
                      const std::vector<int>& value_indexes,
                      sqlite3_int64 rowid) {
    HybridTable* table = cursor->table;
    Row row;
    row.rowid = rowid;
    row.values.resize(table->columns.size());
    row.values[0].type = common::TIMESTAMP;
    row.values[0].is_null = result->is_null(1);
    if (!row.values[0].is_null)
        row.values[0].i64 = result->get_value<int64_t>(1);
    for (size_t i = 0; i < value_indexes.size(); ++i) {
        int table_index = value_indexes[i];
        uint32_t result_index = static_cast<uint32_t>(i + 2);
        Value& value = row.values[table_index];
        value.type = table->columns[table_index].type;
        value.is_null = result->is_null(result_index);
        if (value.is_null) continue;
        switch (value.type) {
            case common::BOOLEAN:
                value.b = result->get_value<bool>(result_index);
                break;
            case common::INT32:
                value.i32 = result->get_value<int32_t>(result_index);
                break;
            case common::DATE:
                value.i32 = result->get_value<int32_t>(result_index);
                break;
            case common::INT64:
            case common::TIMESTAMP:
                value.i64 = result->get_value<int64_t>(result_index);
                break;
            case common::FLOAT:
                value.f = result->get_value<float>(result_index);
                break;
            case common::DOUBLE:
                value.d = result->get_value<double>(result_index);
                break;
            case common::TEXT:
            case common::STRING:
            case common::BLOB: {
                common::String* string =
                    result->get_value<common::String*>(result_index);
                if (string != nullptr)
                    copy_bytes(value.bytes, string->buf_, string->len_);
                break;
            }
            default:
                return SQLITE_ERROR;
        }
    }
    if (row_matches(row, table, kNoWatermark, true,
                    std::numeric_limits<int64_t>::max(), true, {}))
        cursor->rows.push_back(std::move(row));
    return SQLITE_OK;
}

int read_cold(HybridCursor* cursor, const std::vector<ConstraintSpec>& specs,
              sqlite3_value** argv, int64_t lower, bool lower_inclusive,
              int64_t upper, bool upper_inclusive,
              const std::vector<std::pair<int, std::string>>& tag_eq,
              const std::vector<int>& projection) {
    HybridTable* table = cursor->table;
    std::string sql = "SELECT path FROM " + shadow_name(table, "_segments") +
                      " ORDER BY path";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return rc;
    std::shared_ptr<TableSchema> schema = build_tsfile_schema(table);
    std::vector<std::string> value_columns;
    std::vector<int> value_indexes;
    for (int column : projection) {
        if (column == table->time_index) continue;
        value_columns.push_back(table->columns[column].name);
        value_indexes.push_back(column);
    }
    uint64_t segment_ordinal = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const unsigned char* path = sqlite3_column_text(stmt, 0);
        if (path == nullptr) continue;
        TsFileReader reader;
        int reader_rc = reader.open(reinterpret_cast<const char*>(path));
        if (reader_rc != common::E_OK) {
            sqlite3_finalize(stmt);
            return SQLITE_IOERR;
        }
        storage::Filter* tag_filter = nullptr;
        storage::TagFilterBuilder tag_builder(schema.get());
        for (const auto& condition : tag_eq) {
            storage::Filter* next = tag_builder.eq(
                table->columns[condition.first].name, condition.second);
            if (next == nullptr) continue;
            if (tag_filter == nullptr)
                tag_filter = next;
            else
                tag_filter =
                    storage::TagFilterBuilder::and_filter(tag_filter, next);
        }
        ResultSet* result = nullptr;
        int64_t query_lower =
            lower == kNoWatermark ? std::numeric_limits<int64_t>::min() : lower;
        if (!lower_inclusive &&
            query_lower != std::numeric_limits<int64_t>::max())
            ++query_lower;
        int64_t query_upper = upper;
        if (!upper_inclusive &&
            query_upper != std::numeric_limits<int64_t>::min())
            --query_upper;
        if (query_lower > query_upper) {
            delete tag_filter;
            reader.close();
            continue;
        }
        int query_rc =
            reader.query(table->table_name, value_columns, query_lower,
                         query_upper, result, tag_filter);
        if (query_rc != common::E_OK) {
            delete tag_filter;
            reader.close();
            sqlite3_finalize(stmt);
            return SQLITE_IOERR;
        }
        bool has_next = false;
        int next_rc = common::E_OK;
        uint64_t row_ordinal = 0;
        while ((next_rc = result->next(has_next)) == common::E_OK && has_next) {
            if (append_result_row(cursor, result, value_indexes,
                                  cold_rowid(segment_ordinal, row_ordinal++)) !=
                SQLITE_OK) {
                reader.destroy_query_data_set(result);
                delete tag_filter;
                reader.close();
                sqlite3_finalize(stmt);
                return SQLITE_ERROR;
            }
        }
        reader.destroy_query_data_set(result);
        delete tag_filter;
        reader.close();
        if (next_rc != common::E_OK) {
            sqlite3_finalize(stmt);
            return SQLITE_IOERR;
        }
        ++segment_ordinal;
    }
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}

int xOpen(sqlite3_vtab* vtab, sqlite3_vtab_cursor** cursor) {
    std::unique_ptr<HybridCursor> result(new HybridCursor());
    result->table = static_cast<HybridTable*>(vtab);
    *cursor = result.release();
    return SQLITE_OK;
}
int xClose(sqlite3_vtab_cursor* cursor) {
    delete static_cast<HybridCursor*>(cursor);
    return SQLITE_OK;
}

int xFilter(sqlite3_vtab_cursor* cursor_base, int idx_num, const char* idx_str,
            int argc, sqlite3_value** argv) {
    (void)idx_num;
    (void)argc;
    HybridCursor* cursor = static_cast<HybridCursor*>(cursor_base);
    cursor->rows.clear();
    cursor->pos = 0;
    std::vector<ConstraintSpec> specs;
    std::vector<int> projection;
    int rc = parse_specs(idx_str, specs, projection);
    if (rc != SQLITE_OK) return rc;
    if (projection.empty()) {
        for (size_t i = 0; i < cursor->table->columns.size(); ++i)
            projection.push_back(static_cast<int>(i));
    }
    int64_t lower, upper;
    bool lower_inclusive, upper_inclusive;
    std::vector<std::pair<int, std::string>> tag_eq;
    apply_constraint_bounds(specs, argv, lower, lower_inclusive, upper,
                            upper_inclusive, tag_eq);
    rc = read_hot(cursor, specs, argv, lower, lower_inclusive, upper,
                  upper_inclusive, tag_eq, projection);
    if (rc == SQLITE_OK)
        rc = read_cold(cursor, specs, argv, lower, lower_inclusive, upper,
                       upper_inclusive, tag_eq, projection);
    return rc;
}
int xNext(sqlite3_vtab_cursor* cursor_base) {
    ++static_cast<HybridCursor*>(cursor_base)->pos;
    return SQLITE_OK;
}
int xEof(sqlite3_vtab_cursor* cursor_base) {
    HybridCursor* cursor = static_cast<HybridCursor*>(cursor_base);
    return cursor->pos >= cursor->rows.size();
}

int xColumn(sqlite3_vtab_cursor* cursor_base, sqlite3_context* context,
            int column) {
    HybridCursor* cursor = static_cast<HybridCursor*>(cursor_base);
    if (column >= static_cast<int>(cursor->table->columns.size())) {
        sqlite3_result_null(context);
        return SQLITE_OK;
    }
    const Value& value = cursor->rows[cursor->pos].values[column];
    if (value.is_null) {
        sqlite3_result_null(context);
        return SQLITE_OK;
    }
    switch (value.type) {
        case common::BOOLEAN:
            sqlite3_result_int(context, value.b ? 1 : 0);
            break;
        case common::INT32:
        case common::DATE:
            sqlite3_result_int(context, value.i32);
            break;
        case common::INT64:
        case common::TIMESTAMP:
            sqlite3_result_int64(context, value.i64);
            break;
        case common::FLOAT:
            sqlite3_result_double(context, value.f);
            break;
        case common::DOUBLE:
            sqlite3_result_double(context, value.d);
            break;
        case common::TEXT:
        case common::STRING:
            sqlite3_result_text(context, value.bytes.data(),
                                static_cast<int>(value.bytes.size()),
                                SQLITE_TRANSIENT);
            break;
        case common::BLOB:
            sqlite3_result_blob(context, value.bytes.data(),
                                static_cast<int>(value.bytes.size()),
                                SQLITE_TRANSIENT);
            break;
        default:
            sqlite3_result_null(context);
            break;
    }
    return SQLITE_OK;
}
int xRowid(sqlite3_vtab_cursor* cursor_base, sqlite3_int64* rowid) {
    HybridCursor* cursor = static_cast<HybridCursor*>(cursor_base);
    *rowid = cursor->rows[cursor->pos].rowid;
    return SQLITE_OK;
}

int check_mutable(HybridTable* table, const std::vector<Value>& values) {
    const Value& time = values[table->time_index];
    if (time.is_null || time.type != common::TIMESTAMP ||
        time.i64 < table->watermark)
        return SQLITE_CONSTRAINT;
    for (int index : table->tag_indexes)
        if (values[index].is_null) return SQLITE_CONSTRAINT_NOTNULL;
    return SQLITE_OK;
}

int insert_hot(HybridTable* table, int argc, sqlite3_value** argv,
               sqlite3_int64* rowid) {
    std::vector<Value> values(table->columns.size());
    std::string error;
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (!value_from_sqlite(argv[i + 2], table->columns[i], values[i],
                               error)) {
            set_error(table, error);
            return SQLITE_MISMATCH;
        }
    }
    int rc = check_mutable(table, values);
    if (rc != SQLITE_OK) {
        set_error(table,
                  "row is outside the mutable watermark or violates NOT NULL");
        return rc;
    }
    std::ostringstream sql;
    sql << "INSERT INTO " << shadow_name(table, "_data") << '(';
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (i) sql << ',';
        sql << quote_id(table->columns[i].name);
    }
    sql << ") VALUES(";
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (i) sql << ',';
        sql << '?';
    }
    sql << ')';
    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(table->db, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc == SQLITE_OK) {
        for (size_t i = 0; i < values.size(); ++i)
            bind_value(stmt, static_cast<int>(i + 1), values[i]);
        rc = sqlite3_step(stmt);
    }
    if (rc == SQLITE_DONE) {
        *rowid = sqlite3_last_insert_rowid(table->db);
        rc = SQLITE_OK;
    }
    sqlite3_finalize(stmt);
    return rc;
}

int delete_hot(HybridTable* table, sqlite3_int64 rowid) {
    if (rowid < 0) return SQLITE_CONSTRAINT;
    std::string sql =
        "DELETE FROM " + shadow_name(table, "_data") + " WHERE rowid=?";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
    if (rc == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, rowid);
        rc = sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? SQLITE_OK : rc;
}

int update_hot(HybridTable* table, int argc, sqlite3_value** argv,
               sqlite3_int64 rowid) {
    if (rowid < 0) return SQLITE_CONSTRAINT;
    std::vector<Value> values(table->columns.size());
    std::string error;
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (!value_from_sqlite(argv[i + 2], table->columns[i], values[i],
                               error)) {
            set_error(table, error);
            return SQLITE_MISMATCH;
        }
    }
    int rc = check_mutable(table, values);
    if (rc != SQLITE_OK) return rc;
    std::ostringstream sql;
    sql << "UPDATE " << shadow_name(table, "_data") << " SET ";
    for (size_t i = 0; i < table->columns.size(); ++i) {
        if (i) sql << ',';
        sql << quote_id(table->columns[i].name) << "=?";
    }
    sql << " WHERE rowid=?";
    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(table->db, sql.str().c_str(), -1, &stmt, nullptr);
    if (rc == SQLITE_OK) {
        for (size_t i = 0; i < values.size(); ++i)
            bind_value(stmt, static_cast<int>(i + 1), values[i]);
        sqlite3_bind_int64(stmt, static_cast<int>(values.size() + 1), rowid);
        rc = sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
    if (rc == SQLITE_DONE) return SQLITE_OK;
    return rc;
}

int write_segment(HybridTable* table, int64_t cutoff, PendingFile& file,
                  sqlite3_int64& row_count) {
    if (!ensure_directory(table->directory)) return SQLITE_CANTOPEN;
    std::ostringstream base;
    do {
        base.str("");
        base.clear();
        base << table->directory << '/' << file_stem(table->table_name) << '-'
             << static_cast<long long>(getpid()) << '-'
             << table->file_counter++;
        file.temporary = base.str() + ".tmp";
        file.final_path = base.str() + ".tsfile";
    } while (access(file.temporary.c_str(), F_OK) == 0 ||
             access(file.final_path.c_str(), F_OK) == 0);
    WriteFile write_file;
    int flags = O_WRONLY | O_CREAT | O_EXCL | O_TRUNC;
    int rc = write_file.create(file.temporary, flags, 0644);
    if (rc != common::E_OK) return SQLITE_CANTOPEN;
    std::shared_ptr<TableSchema> schema = build_tsfile_schema(table);
    TsFileTableWriter writer(&write_file, schema.get());
    std::vector<std::string> names;
    std::vector<TSDataType> types;
    std::vector<ColumnCategory> categories;
    for (size_t i = 1; i < table->columns.size(); ++i) {
        names.push_back(table->columns[i].name);
        types.push_back(table->columns[i].type);
        categories.push_back(table->columns[i].category);
    }
    const int max_rows = 1024;
    Tablet tablet(table->table_name, names, types, categories, max_rows);
    std::string sql = "SELECT " + quote_id(table->columns[0].name);
    for (size_t i = 1; i < table->columns.size(); ++i)
        sql += "," + quote_id(table->columns[i].name);
    sql += " FROM " + shadow_name(table, "_data") + " WHERE " +
           quote_id(table->columns[0].name) + " >= ? AND " +
           quote_id(table->columns[0].name) + " < ? ORDER BY ";
    bool first = true;
    for (int index : table->tag_indexes) {
        if (!first) sql += ',';
        first = false;
        sql += quote_id(table->columns[index].name);
    }
    sql += ',' + quote_id(table->columns[0].name);
    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return rc;
    sqlite3_bind_int64(stmt, 1, table->watermark);
    sqlite3_bind_int64(stmt, 2, cutoff);
    uint32_t tablet_rows = 0;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        int64_t timestamp = sqlite3_column_int64(stmt, 0);
        tablet.add_timestamp(tablet_rows, timestamp);
        for (size_t i = 1; i < table->columns.size(); ++i) {
            const Column& column = table->columns[i];
            Value value;
            if (!row_value_from_sqlite(stmt, static_cast<int>(i), column,
                                       value)) {
                sqlite3_finalize(stmt);
                return SQLITE_ERROR;
            }
            if (value.is_null) continue;
            int add_rc = common::E_OK;
            switch (column.type) {
                case common::BOOLEAN:
                    add_rc = tablet.add_value(
                        tablet_rows, static_cast<uint32_t>(i - 1), value.b);
                    break;
                case common::INT32:
                case common::DATE:
                    add_rc = tablet.add_value(
                        tablet_rows, static_cast<uint32_t>(i - 1), value.i32);
                    break;
                case common::INT64:
                case common::TIMESTAMP:
                    add_rc = tablet.add_value(
                        tablet_rows, static_cast<uint32_t>(i - 1), value.i64);
                    break;
                case common::FLOAT:
                    add_rc = tablet.add_value(
                        tablet_rows, static_cast<uint32_t>(i - 1), value.f);
                    break;
                case common::DOUBLE:
                    add_rc = tablet.add_value(
                        tablet_rows, static_cast<uint32_t>(i - 1), value.d);
                    break;
                case common::TEXT:
                case common::STRING:
                case common::BLOB:
                    add_rc = tablet.add_value(tablet_rows,
                                              static_cast<uint32_t>(i - 1),
                                              common::String(value.bytes));
                    break;
                default:
                    add_rc = common::E_TYPE_NOT_SUPPORTED;
            }
            if (add_rc != common::E_OK) {
                sqlite3_finalize(stmt);
                return SQLITE_ERROR;
            }
        }
        ++tablet_rows;
        ++row_count;
        if (tablet_rows == max_rows) {
            if (writer.write_table(tablet) != common::E_OK) {
                sqlite3_finalize(stmt);
                return SQLITE_IOERR;
            }
            tablet.reset();
            tablet_rows = 0;
        }
    }
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) return rc;
    if (tablet_rows > 0 && writer.write_table(tablet) != common::E_OK)
        return SQLITE_IOERR;
    if (row_count == 0) return close_writer_and_sync(write_file, writer);
    std::vector<uint8_t> precision(table->precision.begin(),
                                   table->precision.end());
    if (writer.add_tsfile_property("tsfile_sqlite.timestamp_precision",
                                   precision) != common::E_OK)
        return SQLITE_IOERR;
    if (writer.flush() != common::E_OK) return SQLITE_IOERR;
    return close_writer_and_sync(write_file, writer);
}

void cleanup_orphans(HybridTable* table) {
    std::vector<std::string> referenced;
    std::string sql = "SELECT path FROM " + shadow_name(table, "_segments");
    sqlite3_stmt* stmt = nullptr;
    if (sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr) ==
        SQLITE_OK) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const unsigned char* path = sqlite3_column_text(stmt, 0);
            if (path != nullptr)
                referenced.emplace_back(reinterpret_cast<const char*>(path));
        }
    }
    sqlite3_finalize(stmt);
    DIR* directory = opendir(table->directory.c_str());
    if (directory == nullptr) return;
    while (dirent* entry = readdir(directory)) {
        std::string name(entry->d_name);
        bool candidate =
            name.size() > 7 && name.substr(name.size() - 7) == ".tsfile";
        candidate = candidate ||
                    (name.size() > 4 && name.substr(name.size() - 4) == ".tmp");
        if (!candidate) continue;
        std::string path = table->directory + "/" + name;
        bool pending = false;
        for (const PendingFile& file : table->pending) {
            if (path == file.temporary || path == file.final_path) {
                pending = true;
                break;
            }
        }
        if (pending) continue;
        if (std::find(referenced.begin(), referenced.end(), path) ==
            referenced.end())
            unlink(path.c_str());
    }
    closedir(directory);
}

int seal(HybridTable* table, int64_t cutoff) {
    if (cutoff < table->watermark) return SQLITE_CONSTRAINT;
    cleanup_orphans(table);
    PendingFile file;
    sqlite3_int64 row_count = 0;
    int rc = write_segment(table, cutoff, file, row_count);
    if (rc != SQLITE_OK) {
        unlink(file.temporary.c_str());
        return rc;
    }
    if (row_count > 0) {
        std::string insert = "INSERT INTO " + shadow_name(table, "_segments") +
                             "(path,cutoff,row_count) VALUES(?,?,?)";
        sqlite3_stmt* stmt = nullptr;
        rc = sqlite3_prepare_v2(table->db, insert.c_str(), -1, &stmt, nullptr);
        if (rc == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, file.final_path.c_str(), -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_int64(stmt, 2, cutoff);
            sqlite3_bind_int64(stmt, 3, row_count);
            rc = sqlite3_step(stmt);
        }
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) {
            unlink(file.temporary.c_str());
            return rc;
        }
        std::string del = "DELETE FROM " + shadow_name(table, "_data") +
                          " WHERE " + quote_id(table->columns[0].name) +
                          " >= ? AND " + quote_id(table->columns[0].name) +
                          " < ?";
        stmt = nullptr;
        rc = sqlite3_prepare_v2(table->db, del.c_str(), -1, &stmt, nullptr);
        if (rc == SQLITE_OK) {
            sqlite3_bind_int64(stmt, 1, table->watermark);
            sqlite3_bind_int64(stmt, 2, cutoff);
            rc = sqlite3_step(stmt);
        }
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) {
            unlink(file.temporary.c_str());
            return rc;
        }
        table->pending.push_back(std::move(file));
    } else {
        unlink(file.temporary.c_str());
    }
    std::string update = "UPDATE " + shadow_name(table, "_config") +
                         " SET watermark=? WHERE id=1";
    sqlite3_stmt* stmt = nullptr;
    rc = sqlite3_prepare_v2(table->db, update.c_str(), -1, &stmt, nullptr);
    if (rc == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, cutoff);
        rc = sqlite3_step(stmt);
    }
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) return rc;
    table->watermark = cutoff;
    return SQLITE_OK;
}

int xUpdate(sqlite3_vtab* vtab, int argc, sqlite3_value** argv,
            sqlite3_int64* rowid) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    int public_count = static_cast<int>(table->columns.size());
    if (argc == 1) return delete_hot(table, sqlite3_value_int64(argv[0]));
    if (argc != public_count + 4) return SQLITE_ERROR;
    sqlite3_value* command = argv[2 + public_count];
    if (sqlite3_value_type(command) != SQLITE_NULL) {
        const unsigned char* text = sqlite3_value_text(command);
        if (text == nullptr ||
            lower(reinterpret_cast<const char*>(text)) != "seal") {
            set_error(vtab, "unknown _tsfile_command");
            return SQLITE_ERROR;
        }
        sqlite3_value* cutoff = argv[3 + public_count];
        if (sqlite3_value_type(cutoff) != SQLITE_INTEGER) {
            set_error(vtab, "_tsfile_cutoff must be INTEGER");
            return SQLITE_MISMATCH;
        }
        return seal(table, sqlite3_value_int64(cutoff));
    }
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL)
        return insert_hot(table, argc, argv, rowid);
    sqlite3_int64 old_rowid = sqlite3_value_int64(argv[0]);
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL)
        return delete_hot(table, old_rowid);
    sqlite3_int64 new_rowid = sqlite3_value_int64(argv[1]);
    if (new_rowid != old_rowid) return SQLITE_CONSTRAINT;
    return update_hot(table, argc, argv, old_rowid);
}

int xBegin(sqlite3_vtab* vtab) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    table->pending.clear();
    table->savepoint_marks.clear();
    return SQLITE_OK;
}
int xSync(sqlite3_vtab* vtab) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    for (PendingFile& file : table->pending) {
        if (file.temporary.empty()) continue;
        TsFileReader reader;
        int open_rc = reader.open(file.temporary);
        if (open_rc != common::E_OK) return SQLITE_IOERR;
        int close_rc = reader.close();
        if (close_rc != common::E_OK) return SQLITE_IOERR;
        if (access(file.final_path.c_str(), F_OK) == 0) return SQLITE_IOERR;
        if (rename(file.temporary.c_str(), file.final_path.c_str()) != 0)
            return SQLITE_IOERR;
        file.renamed = true;
        if (sync_directory(table->directory) != SQLITE_OK)
            return SQLITE_IOERR_FSYNC;
    }
    for (PendingFile& file : table->pending) file.temporary.clear();
    return SQLITE_OK;
}
int xCommit(sqlite3_vtab* vtab) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    table->pending.clear();
    table->savepoint_marks.clear();
    return SQLITE_OK;
}
int xRollback(sqlite3_vtab* vtab) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    for (const PendingFile& file : table->pending) {
        if (!file.temporary.empty()) unlink(file.temporary.c_str());
        if (file.renamed) unlink(file.final_path.c_str());
    }
    table->pending.clear();
    table->savepoint_marks.clear();
    load_config(table);
    return SQLITE_OK;
}
int xSavepoint(sqlite3_vtab* vtab, int) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    table->savepoint_marks.push_back(table->pending.size());
    return SQLITE_OK;
}
int xRelease(sqlite3_vtab* vtab, int) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    if (!table->savepoint_marks.empty()) table->savepoint_marks.pop_back();
    return SQLITE_OK;
}
int xRollbackTo(sqlite3_vtab* vtab, int) {
    HybridTable* table = static_cast<HybridTable*>(vtab);
    if (table->savepoint_marks.empty()) return SQLITE_OK;
    size_t mark = table->savepoint_marks.back();
    while (table->pending.size() > mark) {
        const PendingFile& file = table->pending.back();
        if (!file.temporary.empty()) unlink(file.temporary.c_str());
        if (file.renamed) unlink(file.final_path.c_str());
        table->pending.pop_back();
    }
    load_config(table);
    return SQLITE_OK;
}
int xRename(sqlite3_vtab*, const char*) { return SQLITE_CONSTRAINT; }
int xShadowName(const char* name) {
    if (name == nullptr) return 0;
    const char* suffixes[] = {"_data", "_segments", "_config"};
    for (const char* suffix : suffixes) {
        size_t length = std::strlen(name), suffix_length = std::strlen(suffix);
        if (length > suffix_length &&
            std::strcmp(name + length - suffix_length, suffix) == 0)
            return 1;
    }
    return 0;
}

const sqlite3_module kModule = {
    3,          xCreate,  xConnect,    xBestIndex,  xDisconnect,
    xDestroy,   xOpen,    xClose,      xFilter,     xNext,
    xEof,       xColumn,  xRowid,      xUpdate,     xBegin,
    xSync,      xCommit,  xRollback,   nullptr,     xRename,
    xSavepoint, xRelease, xRollbackTo, xShadowName, nullptr};

}  // namespace

extern "C" int sqlite3_extension_init(sqlite3* db, char** error_message,
                                      const sqlite3_api_routines* api) {
    SQLITE_EXTENSION_INIT2(api);
    static bool initialized = false;
    if (!initialized) {
        if (storage::libtsfile_init() != common::E_OK) {
            if (error_message)
                *error_message =
                    sqlite3_mprintf("libtsfile initialization failed");
            return SQLITE_ERROR;
        }
        initialized = true;
    }
    return sqlite3_create_module_v2(db, "tsfile_hybrid", &kModule, nullptr,
                                    nullptr);
}
