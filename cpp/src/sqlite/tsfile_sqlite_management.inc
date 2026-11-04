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

// Included inside the adapter's anonymous namespace. Management operations use
// the same connection and table objects as the virtual-table transaction hooks.

std::string qualified_name(const HybridTable* table) {
    return quote_id(table->db_name) + '.' + quote_id(table->table_name);
}

HybridTable* resolve_table(sqlite3* db, Connection* connection,
                           const std::string& name) {
    // Split the schema qualifier outside quoted identifiers.
    char quote = 0;
    size_t dot = std::string::npos;
    for (size_t i = 0; i < name.size(); ++i) {
        char c = name[i];
        if (quote) {
            if (c == quote) {
                if (i + 1 < name.size() && name[i + 1] == quote)
                    ++i;
                else
                    quote = 0;
            }
        } else if (c == '"' || c == '`' || c == '[')
            quote = c == '[' ? ']' : c;
        else if (c == '.') {
            if (dot != std::string::npos) return nullptr;
            dot = i;
        }
    }
    if (quote || dot == std::string::npos) return nullptr;
    std::string schema, table;
    size_t pos = 0;
    std::string a = trim(name.substr(0, dot)), b = trim(name.substr(dot + 1));
    if (!sql_token(a, pos, schema) || !trim(a.substr(pos)).empty())
        return nullptr;
    pos = 0;
    if (!sql_token(b, pos, table) || !trim(b.substr(pos)).empty())
        return nullptr;
    sqlite3_stmt* stmt = nullptr;
    const std::string sql = "SELECT * FROM " + quote_id(schema) + '.' +
                            quote_id(table) + " LIMIT 0";
    int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) return nullptr;
    auto it = connection->tables.find(lower(schema) + '.' + lower(table));
    return it == connection->tables.end() ? nullptr : it->second;
}

bool standalone_call(sqlite3* db, const std::string& function) {
    // Management functions are intentionally limited to a standalone SELECT.
    // Reject row queries and compound expressions before any side effect.
    int active = 0;
    for (sqlite3_stmt* stmt = sqlite3_next_stmt(db, nullptr); stmt;
         stmt = sqlite3_next_stmt(db, stmt)) {
        if (!sqlite3_stmt_busy(stmt)) continue;
        ++active;
        std::string sql = trim(sqlite3_sql(stmt));
        size_t p = sql.find_first_of(" \t\r\n");
        if (p == std::string::npos || lower(sql.substr(0, p)) != "select")
            return false;
        sql = trim(sql.substr(p));
        p = sql.find('(');
        if (p == std::string::npos || lower(trim(sql.substr(0, p))) != function)
            return false;
        char quote = 0;
        size_t end = std::string::npos;
        for (size_t i = p + 1; i < sql.size(); ++i) {
            char c = sql[i];
            if (quote) {
                if (c == quote) {
                    if (i + 1 < sql.size() && sql[i + 1] == quote)
                        ++i;
                    else
                        quote = 0;
                }
            } else if (c == '\'' || c == '"')
                quote = c;
            else if (c == '(')
                return false;
            else if (c == ')') {
                end = i;
                break;
            }
        }
        if (end == std::string::npos) return false;
        std::string tail = trim(sql.substr(end + 1));
        if (!tail.empty() && tail != ";") return false;
    }
    return active == 1;
}

void management_error(sqlite3_context* context, int rc,
                      const std::string& detail) {
    sqlite3_result_error(context, detail.c_str(), -1);
    sqlite3_result_error_code(context, rc == SQLITE_OK ? SQLITE_ERROR : rc);
}

struct ManagementGuard {
    Connection* connection;
    explicit ManagementGuard(Connection* c) : connection(c) {
        c->managing = true;
    }
    ~ManagementGuard() { connection->managing = false; }
};

int enlist_table(HybridTable* table) {
    // Even a zero-row virtual UPDATE enlists the module in SQLite's write
    // transaction. The seal then shares xSync/xRollback and savepoint hooks.
    return exec_sql(table->db,
                    "UPDATE " + qualified_name(table) + " SET " +
                        quote_id(table->columns[0].name) + '=' +
                        quote_id(table->columns[0].name) + " WHERE 0",
                    table);
}

void rollback_management(HybridTable* table) {
    exec_sql(table->db, "ROLLBACK TO tsfile_management");
    exec_sql(table->db, "RELEASE tsfile_management");
}

void seal_function(sqlite3_context* context, int, sqlite3_value** argv) {
    auto* connection = static_cast<Connection*>(sqlite3_user_data(context));
    sqlite3* db = sqlite3_context_db_handle(context);
    if (connection->managing || !standalone_call(db, "tsfile_seal") ||
        sqlite3_value_type(argv[0]) != SQLITE_TEXT ||
        sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        management_error(
            context, SQLITE_MISUSE,
            "use SELECT tsfile_seal('schema.table', max_file_size_mb)");
        return;
    }
    ManagementGuard guard(connection);
    auto* table = resolve_table(
        db, connection,
        reinterpret_cast<const char*>(sqlite3_value_text(argv[0])));
    if (!table) {
        management_error(context, SQLITE_ERROR,
                         "TsFile table not found; include its SQLite schema");
        return;
    }
    if (table->readonly) {
        management_error(context, SQLITE_READONLY, "table is readonly");
        return;
    }
    int rc = exec_sql(db, "SAVEPOINT tsfile_management", table);
    if (rc != SQLITE_OK) {
        management_error(context, rc, sqlite3_errmsg(db));
        return;
    }
    rc = enlist_table(table);
    sqlite3_int64 rows = 0;
    if (rc == SQLITE_OK) rc = seal(table, sqlite3_value_int64(argv[1]), &rows);
    if (rc == SQLITE_OK) rc = exec_sql(db, "RELEASE tsfile_management", table);
    if (rc != SQLITE_OK) {
        std::string detail =
            table->zErrMsg
                ? table->zErrMsg
                : "seal failed: invalid size limit or file operation";
        rollback_management(table);
        management_error(context, rc, detail);
        return;
    }
    sqlite3_result_int64(context, rows);
}

std::string canonical_path(const std::string& path) {
    char* resolved = realpath(path.c_str(), nullptr);
    if (!resolved) return "";
    std::string result(resolved);
    free(resolved);
    return result;
}

bool paths_overlap(const std::string& a, const std::string& b) {
    return a == b ||
           (a.size() > b.size() && a.compare(0, b.size() + 1, b + "/") == 0) ||
           (b.size() > a.size() && b.compare(0, a.size() + 1, a + "/") == 0);
}

std::string copy_file_with_identity(const std::string& source,
                                    const std::string& target, int& rc) {
    int in = open(source.c_str(), O_RDONLY);
    if (in < 0) {
        rc = SQLITE_CANTOPEN;
        return "";
    }
    int out = open(target.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (out < 0) {
        close(in);
        rc = SQLITE_CANTOPEN;
        return "";
    }
    uint64_t hash = 14695981039346656037ULL;
    int64_t size = 0;
    unsigned char buffer[16384];
    ssize_t n = 0;
    bool ok = true;
    while ((n = read(in, buffer, sizeof(buffer))) > 0) {
        for (ssize_t i = 0; i < n; ++i) {
            hash ^= buffer[i];
            hash *= 1099511628211ULL;
        }
        size += n;
        ssize_t written = 0;
        while (written < n) {
            ssize_t w =
                write(out, buffer + written, static_cast<size_t>(n - written));
            if (w <= 0) {
                ok = false;
                break;
            }
            written += w;
        }
        if (!ok) break;
    }
    if (n < 0) ok = false;
    if (ok && fsync(out) != 0) ok = false;
    if (close(in) != 0) ok = false;
    if (close(out) != 0) ok = false;
    if (!ok) {
        unlink(target.c_str());
        rc = SQLITE_IOERR;
        return "";
    }
    rc = SQLITE_OK;
    return std::to_string(size) + ":" + std::to_string(hash);
}

void export_function(sqlite3_context* context, int, sqlite3_value** argv) {
    auto* connection = static_cast<Connection*>(sqlite3_user_data(context));
    sqlite3* db = sqlite3_context_db_handle(context);
    if (connection->managing || !standalone_call(db, "tsfile_export") ||
        !sqlite3_get_autocommit(db) ||
        sqlite3_value_type(argv[0]) != SQLITE_TEXT ||
        sqlite3_value_type(argv[1]) != SQLITE_TEXT) {
        management_error(context, SQLITE_MISUSE,
                         "export requires a standalone SELECT outside an "
                         "explicit transaction");
        return;
    }
    ManagementGuard guard(connection);
    auto* table = resolve_table(
        db, connection,
        reinterpret_cast<const char*>(sqlite3_value_text(argv[0])));
    if (!table) {
        management_error(context, SQLITE_ERROR,
                         "TsFile table not found; include its SQLite schema");
        return;
    }
    std::string output =
        reinterpret_cast<const char*>(sqlite3_value_text(argv[1]));
    size_t slash = output.find_last_of('/');
    struct stat st {};
    if (output.empty() || output[0] != '/' || slash == std::string::npos ||
        slash + 1 == output.size() || lstat(output.c_str(), &st) == 0) {
        management_error(context, SQLITE_CANTOPEN,
                         "output must be a new absolute directory");
        return;
    }
    std::string parent = canonical_path(
        output.substr(0, slash).empty() ? "/" : output.substr(0, slash));
    if (parent.empty() || !directory_exists(parent)) {
        management_error(context, SQLITE_CANTOPEN,
                         "output parent directory is unavailable");
        return;
    }
    output = parent + (parent == "/" ? "" : "/") + output.substr(slash + 1);
    std::vector<std::string> sources;
    if (!table->directory.empty())
        sources.push_back(canonical_path(table->directory));
    if (!table->source_file.empty())
        sources.push_back(canonical_path(table->source_file));
    for (const auto& source : sources) {
        if (!source.empty() && paths_overlap(source, output)) {
            management_error(context, SQLITE_CANTOPEN,
                             "output overlaps a source directory");
            return;
        }
    }
    struct ExportFile {
        std::string path;
        std::string identity;
    };
    std::vector<ExportFile> files;
    std::string sql = "SELECT path,identity FROM " +
                      shadow_name(table, "_segments") + " ORDER BY path";
    sqlite3_stmt* stmt = nullptr;
    int rc = sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) {
        management_error(context, rc, sqlite3_errmsg(db));
        return;
    }
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        std::string path =
            reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        if (!table->readonly && !path_is_inside(table->directory, path))
            continue;
        files.push_back({path, reinterpret_cast<const char*>(
                                   sqlite3_column_text(stmt, 1))});
    }
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        management_error(context, rc, sqlite3_errmsg(db));
        return;
    }
    std::string staging = output + ".tsfile-export-XXXXXX";
    std::vector<char> buffer(staging.begin(), staging.end());
    buffer.push_back(0);
    if (!mkdtemp(buffer.data())) {
        management_error(context, SQLITE_CANTOPEN,
                         "cannot create export staging directory");
        return;
    }
    staging = buffer.data();
    std::set<std::string> names;
    std::vector<std::string> written;
    std::string detail;
    rc = SQLITE_OK;
    for (const ExportFile& file : files) {
        size_t name_at = file.path.find_last_of('/');
        std::string name = name_at == std::string::npos
                               ? file.path
                               : file.path.substr(name_at + 1);
        if (name.empty() || !names.insert(name).second) {
            rc = SQLITE_ERROR;
            detail = "duplicate export file name: " + name;
            break;
        }
        std::string target = staging + "/" + name;
        int copy_rc = SQLITE_OK;
        std::string identity =
            copy_file_with_identity(file.path, target, copy_rc);
        if (copy_rc != SQLITE_OK) {
            rc = copy_rc;
            detail = "cannot copy sealed file: " + file.path;
            break;
        }
        written.push_back(target);
        if (identity != file.identity) {
            rc = SQLITE_IOERR;
            detail = "sealed file changed before export: " + file.path;
            break;
        }
    }
    if (rc == SQLITE_OK) rc = sync_directory(staging);
    if (rc == SQLITE_OK) rc = publish_without_replacing(staging, output);
    if (rc != SQLITE_OK) {
        for (const std::string& path : written) unlink(path.c_str());
        rmdir(staging.c_str());
        management_error(
            context, rc,
            detail.empty() ? "export output not published" : detail);
        return;
    }
    if (sync_directory(parent) != SQLITE_OK) {
        management_error(context, SQLITE_IOERR_FSYNC,
                         "complete output published but directory fsync "
                         "failed");
        return;
    }
    cleanup_orphan_temp_files(table);
    sqlite3_result_int64(context, static_cast<sqlite3_int64>(files.size()));
}

void compact_function(sqlite3_context* context, int, sqlite3_value** argv) {
    auto* connection = static_cast<Connection*>(sqlite3_user_data(context));
    sqlite3* db = sqlite3_context_db_handle(context);
    if (connection->managing || !standalone_call(db, "tsfile_compact") ||
        sqlite3_value_type(argv[0]) != SQLITE_TEXT ||
        sqlite3_value_type(argv[1]) != SQLITE_INTEGER ||
        sqlite3_value_type(argv[2]) != SQLITE_INTEGER) {
        management_error(context, SQLITE_MISUSE,
                         "use SELECT tsfile_compact('schema.table', "
                         "min_file_size_mb, target_file_size_mb)");
        return;
    }
    ManagementGuard guard(connection);
    auto* table = resolve_table(
        db, connection,
        reinterpret_cast<const char*>(sqlite3_value_text(argv[0])));
    if (!table) {
        management_error(context, SQLITE_ERROR,
                         "TsFile table not found; include its SQLite schema");
        return;
    }
    if (table->readonly) {
        management_error(context, SQLITE_READONLY, "table is readonly");
        return;
    }
    int rc = exec_sql(db, "SAVEPOINT tsfile_management", table);
    if (rc != SQLITE_OK) {
        management_error(context, rc, sqlite3_errmsg(db));
        return;
    }
    rc = enlist_table(table);
    sqlite3_int64 files = 0;
    if (rc == SQLITE_OK)
        rc = compact(table, sqlite3_value_int64(argv[1]),
                     sqlite3_value_int64(argv[2]), &files);
    if (rc == SQLITE_OK) rc = exec_sql(db, "RELEASE tsfile_management", table);
    if (rc != SQLITE_OK) {
        std::string detail =
            table->zErrMsg ? table->zErrMsg
                           : "compact failed: invalid size limits or file "
                             "operation";
        rollback_management(table);
        management_error(context, rc, detail);
        return;
    }
    sqlite3_result_int64(context, files);
}

enum class DiagnosticKind { TableInfo, DeviceInfo, Schema, Verify };

struct DiagnosticTable : sqlite3_vtab {
    sqlite3* db = nullptr;
    Connection* connection = nullptr;
    DiagnosticKind kind = DiagnosticKind::TableInfo;
};
struct DiagnosticCursor : sqlite3_vtab_cursor {
    std::vector<std::vector<Value>> rows;
    size_t pos = 0;
};
Value text_cell(const std::string& text) {
    Value v;
    v.type = common::STRING;
    v.is_null = false;
    v.bytes = text;
    return v;
}
Value int_cell(sqlite3_int64 value) {
    Value v;
    v.type = common::INT64;
    v.is_null = false;
    v.i64 = value;
    return v;
}

std::string tsfile_type_name(common::TSDataType type) {
    switch (type) {
        case common::BOOLEAN:
            return "BOOLEAN";
        case common::INT32:
            return "INT32";
        case common::INT64:
            return "INT64";
        case common::FLOAT:
            return "FLOAT";
        case common::DOUBLE:
            return "DOUBLE";
        case common::TEXT:
            return "TEXT";
        case common::STRING:
            return "STRING";
        case common::BLOB:
            return "BLOB";
        case common::TIMESTAMP:
            return "TIMESTAMP";
        case common::DATE:
            return "DATE";
        default:
            return "UNKNOWN";
    }
}

std::string column_category_name(common::ColumnCategory category) {
    switch (category) {
        case common::ColumnCategory::TIME:
            return "TIME";
        case common::ColumnCategory::TAG:
            return "TAG";
        case common::ColumnCategory::FIELD:
            return "FIELD";
        default:
            return "UNKNOWN";
    }
}

std::string device_key_text(const std::string& key) {
    std::string result;
    size_t pos = 0;
    size_t tag = 0;
    while (pos < key.size()) {
        std::string value;
        if (key[pos] == 'N' && pos + 1 < key.size() && key[pos + 1] == ';') {
            value = "NULL";
            pos += 2;
        } else if (key[pos] == 'S') {
            size_t colon = key.find(':', pos + 1);
            if (colon == std::string::npos) break;
            unsigned long long length = std::strtoull(
                key.substr(pos + 1, colon - pos - 1).c_str(), nullptr, 10);
            if (colon + 1 + length > key.size()) break;
            value = key.substr(colon + 1, static_cast<size_t>(length));
            pos = colon + 1 + static_cast<size_t>(length);
            if (pos < key.size() && key[pos] == ';') ++pos;
        } else {
            break;
        }
        if (tag++) result += ',';
        result += value;
    }
    return result;
}

int diagnostic_connect(sqlite3* db, void* aux, int, const char* const* argv,
                       sqlite3_vtab** out, char**) {
    std::unique_ptr<DiagnosticTable> table(new DiagnosticTable());
    table->db = db;
    table->connection = static_cast<Connection*>(aux);
    std::string name = argv[0];
    if (name == "tsfile_verify")
        table->kind = DiagnosticKind::Verify;
    else if (name == "tsfile_device_info")
        table->kind = DiagnosticKind::DeviceInfo;
    else if (name == "tsfile_schema")
        table->kind = DiagnosticKind::Schema;
    else
        table->kind = DiagnosticKind::TableInfo;
    const char* schema =
        table->kind == DiagnosticKind::Verify
            ? "CREATE TABLE x(segment_id INTEGER,path TEXT,status TEXT,detail "
              "TEXT,requested_table TEXT HIDDEN)"
            : (table->kind == DiagnosticKind::DeviceInfo
                   ? "CREATE TABLE x(device_key TEXT,watermark "
                     "INTEGER,max_time "
                     "INTEGER,hot_rows INTEGER,requested_table TEXT HIDDEN)"
                   : (table->kind == DiagnosticKind::Schema
                          ? "CREATE TABLE x(cid INTEGER,name TEXT,tsfile_type "
                            "TEXT,category TEXT,sqlite_type TEXT,not_null "
                            "INTEGER,requested_table TEXT HIDDEN)"
                          : "CREATE TABLE x(table_name TEXT,mode TEXT,"
                            "timestamp_precision TEXT,device_count INTEGER,"
                            "watermark INTEGER,source_max_time "
                            "INTEGER,hot_rows "
                            "INTEGER,file_count INTEGER,directory TEXT,"
                            "source_file TEXT,source_table "
                            "TEXT,requested_table "
                            "TEXT HIDDEN)"));
    int rc = sqlite3_declare_vtab(db, schema);
    if (rc != SQLITE_OK) return rc;
    sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
    *out = table.release();
    return SQLITE_OK;
}
int diagnostic_best(sqlite3_vtab* base, sqlite3_index_info* info) {
    DiagnosticKind kind = static_cast<DiagnosticTable*>(base)->kind;
    int hidden = kind == DiagnosticKind::TableInfo
                     ? 11
                     : (kind == DiagnosticKind::Schema ? 6 : 4);
    for (int i = 0; i < info->nConstraint; ++i) {
        if (info->aConstraint[i].iColumn == hidden &&
            info->aConstraint[i].op == SQLITE_INDEX_CONSTRAINT_EQ &&
            info->aConstraint[i].usable) {
            info->aConstraintUsage[i].argvIndex = 1;
            info->aConstraintUsage[i].omit = 1;
            info->idxNum = 1;
            info->estimatedCost = 1;
            return SQLITE_OK;
        }
    }
    return SQLITE_CONSTRAINT;
}
int diagnostic_disconnect(sqlite3_vtab* p) {
    delete static_cast<DiagnosticTable*>(p);
    return SQLITE_OK;
}
int diagnostic_open(sqlite3_vtab*, sqlite3_vtab_cursor** out) {
    *out = new DiagnosticCursor();
    return SQLITE_OK;
}
int diagnostic_close(sqlite3_vtab_cursor* p) {
    delete static_cast<DiagnosticCursor*>(p);
    return SQLITE_OK;
}
int diagnostic_next(sqlite3_vtab_cursor* p) {
    ++static_cast<DiagnosticCursor*>(p)->pos;
    return SQLITE_OK;
}
int diagnostic_eof(sqlite3_vtab_cursor* p) {
    auto* c = static_cast<DiagnosticCursor*>(p);
    return c->pos >= c->rows.size();
}
int diagnostic_rowid(sqlite3_vtab_cursor* p, sqlite3_int64* id) {
    *id = static_cast<DiagnosticCursor*>(p)->pos + 1;
    return SQLITE_OK;
}
int diagnostic_column(sqlite3_vtab_cursor* p, sqlite3_context* ctx,
                      int column) {
    auto* cursor = static_cast<DiagnosticCursor*>(p);
    if (column >= static_cast<int>(cursor->rows[cursor->pos].size())) {
        sqlite3_result_null(ctx);
        return SQLITE_OK;
    }
    const auto& value = cursor->rows[cursor->pos][column];
    if (value.is_null)
        sqlite3_result_null(ctx);
    else if (value.type == common::INT64)
        sqlite3_result_int64(ctx, value.i64);
    else
        sqlite3_result_text(ctx, value.bytes.c_str(), value.bytes.size(),
                            SQLITE_TRANSIENT);
    return SQLITE_OK;
}
int diagnostic_filter(sqlite3_vtab_cursor* base, int, const char*, int argc,
                      sqlite3_value** argv) {
    auto* cursor = static_cast<DiagnosticCursor*>(base);
    auto* diagnostic = static_cast<DiagnosticTable*>(base->pVtab);
    cursor->rows.clear();
    cursor->pos = 0;
    if (argc != 1 || sqlite3_value_type(argv[0]) != SQLITE_TEXT)
        return SQLITE_MISMATCH;
    std::string requested =
        reinterpret_cast<const char*>(sqlite3_value_text(argv[0]));
    auto* table =
        resolve_table(diagnostic->db, diagnostic->connection, requested);
    if (!table) {
        set_error(diagnostic, "TsFile table not found: " + requested);
        return SQLITE_ERROR;
    }
    sqlite3_stmt* stmt = nullptr;
    if (diagnostic->kind == DiagnosticKind::Schema) {
        for (size_t i = 0; i < table->columns.size(); ++i) {
            const Column& column = table->columns[i];
            std::vector<Value> row;
            row.push_back(int_cell(static_cast<sqlite3_int64>(i)));
            row.push_back(text_cell(column.name));
            row.push_back(text_cell(tsfile_type_name(column.type)));
            row.push_back(text_cell(column_category_name(column.category)));
            row.push_back(text_cell(sqlite_type(column.type)));
            row.push_back(
                int_cell(column.category == ColumnCategory::TIME ? 1 : 0));
            row.push_back(text_cell(requested));
            cursor->rows.push_back(std::move(row));
        }
        return SQLITE_OK;
    }
    if (diagnostic->kind == DiagnosticKind::DeviceInfo) {
        std::string sql;
        if (table->readonly) {
            sql = "SELECT device_key,watermark,max_time,0 FROM " +
                  shadow_name(table, "_devices") + " ORDER BY device_key";
        } else {
            std::string hot = "(SELECT " + quote_id(table->device_column) +
                              " AS device_key,max(" +
                              quote_id(table->columns[table->time_index].name) +
                              ") AS hot_max,count(*) AS hot_rows FROM " +
                              shadow_name(table, "_data") + " GROUP BY " +
                              quote_id(table->device_column) + ")";
            sql =
                "SELECT d.device_key,d.watermark,CASE WHEN d.max_time IS "
                "NULL THEN h.hot_max WHEN h.hot_max IS NULL THEN d.max_time "
                "ELSE max(d.max_time,h.hot_max) END,COALESCE(h.hot_rows,0) "
                "FROM " +
                shadow_name(table, "_devices") + " AS d LEFT JOIN " + hot +
                " AS h ON h.device_key=d.device_key ORDER BY d.device_key";
        }
        int rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) return rc;
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::string key =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            std::vector<Value> row;
            row.push_back(text_cell(device_key_text(key)));
            if (table->readonly || sqlite3_column_type(stmt, 1) == SQLITE_NULL)
                row.push_back(Value());
            else
                row.push_back(int_cell(sqlite3_column_int64(stmt, 1)));
            if (sqlite3_column_type(stmt, 2) == SQLITE_NULL)
                row.push_back(Value());
            else
                row.push_back(int_cell(sqlite3_column_int64(stmt, 2)));
            row.push_back(int_cell(sqlite3_column_int64(stmt, 3)));
            row.push_back(text_cell(requested));
            cursor->rows.push_back(std::move(row));
        }
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) return rc;
        if (cursor->rows.empty() && table->tag_indexes.empty()) {
            std::vector<Value> row;
            row.push_back(text_cell(""));
            if (table->readonly)
                row.push_back(Value());
            else
                row.push_back(int_cell(kNoWatermark));
            row.push_back(Value());
            row.push_back(int_cell(0));
            row.push_back(text_cell(requested));
            cursor->rows.push_back(std::move(row));
        }
        return SQLITE_OK;
    }
    if (diagnostic->kind == DiagnosticKind::TableInfo) {
        std::string devices_count =
            "(SELECT count(*) FROM " + shadow_name(table, "_devices") + ")";
        if (table->tag_indexes.empty())
            devices_count = "max(" + devices_count + ",1)";
        std::string watermark =
            table->readonly ? "NULL"
                            : "(CASE WHEN (SELECT count(*) FROM " +
                                  shadow_name(table, "_devices") + ")=0 THEN " +
                                  std::to_string(kNoWatermark) +
                                  " ELSE (SELECT min(watermark) FROM " +
                                  shadow_name(table, "_devices") + ") END)";
        std::string hot_rows =
            table->readonly
                ? "0"
                : "(SELECT count(*) FROM " + shadow_name(table, "_data") + ")";
        std::string sql =
            "SELECT " + quote_sql(table->db_name + '.' + table->table_name) +
            ",mode,precision," + devices_count + "," + watermark +
            ",source_max," + hot_rows + ",(SELECT count(*) FROM " +
            shadow_name(table, "_segments") +
            "),nullif(directory,''),nullif(source_file,''),nullif(source_table,"
            "'') FROM " +
            shadow_name(table, "_config");
        int rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
        if (rc != SQLITE_OK) return rc;
        while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            std::vector<Value> row;
            for (int i = 0; i < 11; ++i) {
                if (sqlite3_column_type(stmt, i) == SQLITE_NULL)
                    row.push_back(Value());
                else if (sqlite3_column_type(stmt, i) == SQLITE_INTEGER)
                    row.push_back(int_cell(sqlite3_column_int64(stmt, i)));
                else
                    row.push_back(text_cell(reinterpret_cast<const char*>(
                        sqlite3_column_text(stmt, i))));
            }
            row.push_back(text_cell(requested));
            cursor->rows.push_back(std::move(row));
        }
        sqlite3_finalize(stmt);
        return rc == SQLITE_DONE ? SQLITE_OK : rc;
    }
    std::string sql = "SELECT rowid,path,identity,source_table FROM " +
                      shadow_name(table, "_segments") + " ORDER BY rowid";
    int rc = sqlite3_prepare_v2(table->db, sql.c_str(), -1, &stmt, nullptr);
    if (rc != SQLITE_OK) return rc;
    std::set<std::string> registered;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        std::string path =
            reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));
        std::string identity =
            reinterpret_cast<const char*>(sqlite3_column_text(stmt, 2));
        std::string source =
            reinterpret_cast<const char*>(sqlite3_column_text(stmt, 3));
        registered.insert(path);
        std::string actual = path;
        for (const auto& pending : table->pending)
            if (pending.final_path == path && !pending.temporary.empty() &&
                !pending.renamed)
                actual = pending.temporary;
        std::string status = "OK",
                    detail = "file and registered identity match";
        struct stat st {};
        if (stat(actual.c_str(), &st) != 0 && errno == ENOENT) {
            status = "MISSING";
            detail = "registered file is missing";
        } else {
            TsFileReader reader;
            if (reader.open(actual) != common::E_OK) {
                status = "CORRUPT";
                detail = "cannot parse registered TsFile";
            } else {
                if (!source_schema(reader, source) ||
                    file_identity(actual) != identity) {
                    status = "MISMATCH";
                    detail = "file or schema differs from registration";
                }
                reader.close();
            }
        }
        cursor->rows.push_back({int_cell(sqlite3_column_int64(stmt, 0)),
                                text_cell(path), text_cell(status),
                                text_cell(detail), text_cell(requested)});
    }
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) return rc;
    DIR* dir =
        table->directory.empty() ? nullptr : opendir(table->directory.c_str());
    if (dir) {
        while (dirent* entry = readdir(dir)) {
            std::string name = entry->d_name,
                        path = table->directory + '/' + name;
            struct stat st {};
            if (name.size() <= 7 || name.substr(name.size() - 7) != ".tsfile" ||
                registered.count(path) || lstat(path.c_str(), &st) != 0 ||
                !S_ISREG(st.st_mode))
                continue;
            cursor->rows.push_back(
                {Value(), text_cell(path), text_cell("UNREGISTERED"),
                 text_cell("unregistered file; left unchanged"),
                 text_cell(requested)});
        }
        closedir(dir);
    }
    return SQLITE_OK;
}
const sqlite3_module kDiagnosticModule = {3,
                                          nullptr,
                                          diagnostic_connect,
                                          diagnostic_best,
                                          diagnostic_disconnect,
                                          nullptr,
                                          diagnostic_open,
                                          diagnostic_close,
                                          diagnostic_filter,
                                          diagnostic_next,
                                          diagnostic_eof,
                                          diagnostic_column,
                                          diagnostic_rowid,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr,
                                          nullptr};

int register_management(sqlite3* db, Connection* connection) {
    int rc = sqlite3_create_function_v2(
        db, "tsfile_seal", 2, SQLITE_UTF8 | SQLITE_DIRECTONLY, connection,
        seal_function, nullptr, nullptr, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_function_v2(
            db, "tsfile_export", 2, SQLITE_UTF8 | SQLITE_DIRECTONLY, connection,
            export_function, nullptr, nullptr, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_function_v2(
            db, "tsfile_compact", 3, SQLITE_UTF8 | SQLITE_DIRECTONLY,
            connection, compact_function, nullptr, nullptr, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_module_v2(db, "tsfile_table_info",
                                      &kDiagnosticModule, connection, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_module_v2(db, "tsfile_device_info",
                                      &kDiagnosticModule, connection, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_module_v2(db, "tsfile_schema", &kDiagnosticModule,
                                      connection, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_module_v2(db, "tsfile_verify", &kDiagnosticModule,
                                      connection, nullptr);
    return rc;
}
