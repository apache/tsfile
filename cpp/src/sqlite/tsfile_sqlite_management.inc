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
    load_config(table);
}

void seal_function(sqlite3_context* context, int, sqlite3_value** argv) {
    auto* connection = static_cast<Connection*>(sqlite3_user_data(context));
    sqlite3* db = sqlite3_context_db_handle(context);
    if (connection->managing || !standalone_call(db, "tsfile_seal") ||
        sqlite3_value_type(argv[0]) != SQLITE_TEXT ||
        sqlite3_value_type(argv[1]) != SQLITE_INTEGER) {
        management_error(
            context, SQLITE_MISUSE,
            "use SELECT tsfile_seal('schema.table', integer_cutoff)");
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
            table->zErrMsg ? table->zErrMsg
                           : "seal failed: invalid boundary or file operation";
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
    int rc = exec_sql(db, "SAVEPOINT tsfile_management", table);
    if (rc != SQLITE_OK) {
        management_error(context, rc, sqlite3_errmsg(db));
        return;
    }
    HybridCursor snapshot;
    snapshot.table = table;
    std::vector<int> projection;
    for (size_t i = 0; i < table->columns.size(); ++i) projection.push_back(i);
    // Validate and capture existing immutable data before sealing changes
    // state.
    rc = read_cold(&snapshot, {}, nullptr, kNoWatermark, true,
                   std::numeric_limits<int64_t>::max(), true, {}, projection);
    if (rc == SQLITE_OK && !table->readonly) rc = enlist_table(table);
    if (rc == SQLITE_OK && !table->readonly) {
        HybridCursor hot;
        hot.table = table;
        rc =
            read_hot(&hot, {}, nullptr, kNoWatermark, true,
                     std::numeric_limits<int64_t>::max(), true, {}, projection);
        if (rc == SQLITE_OK && !hot.rows.empty()) {
            int64_t maximum = kNoWatermark;
            for (const auto& row : hot.rows)
                maximum = std::max(maximum, row.values[0].i64);
            const bool exhausted =
                maximum == std::numeric_limits<int64_t>::max();
            rc = seal(table, exhausted ? maximum : maximum + 1, nullptr,
                      exhausted);
            if (rc == SQLITE_OK)
                snapshot.rows.insert(snapshot.rows.end(), hot.rows.begin(),
                                     hot.rows.end());
        }
    }
    if (rc == SQLITE_OK) rc = exec_sql(db, "RELEASE tsfile_management", table);
    if (rc != SQLITE_OK) {
        std::string detail =
            table->zErrMsg ? table->zErrMsg : "automatic seal failed";
        rollback_management(table);
        management_error(context, rc,
                         "automatic seal not committed: " + detail);
        return;
    }
    std::string staging = output + ".tsfile-export-XXXXXX";
    std::vector<char> buffer(staging.begin(), staging.end());
    buffer.push_back(0);
    if (!mkdtemp(buffer.data())) {
        management_error(
            context, SQLITE_CANTOPEN,
            "automatic seal committed; cannot create export staging directory");
        return;
    }
    staging = buffer.data();
    std::string part = staging + "/part-000001.tsfile";
    bool part_created = false;
    if (!snapshot.rows.empty())
        rc = write_rows(table, std::move(snapshot.rows), part, &part_created);
    if (rc == SQLITE_OK) rc = sync_directory(staging);
    if (rc == SQLITE_OK) {
        rc = publish_without_replacing(staging, output);
    }
    if (rc != SQLITE_OK) {
        if (part_created) unlink(part.c_str());
        rmdir(staging.c_str());
        management_error(
            context, rc,
            "automatic seal committed; export output not published");
        return;
    }
    if (sync_directory(parent) != SQLITE_OK) {
        management_error(context, SQLITE_IOERR_FSYNC,
                         "automatic seal committed; complete output published "
                         "but directory fsync failed");
        return;
    }
    sqlite3_result_int(
        context,
        access((output + "/part-000001.tsfile").c_str(), F_OK) == 0 ? 1 : 0);
}

struct DiagnosticTable : sqlite3_vtab {
    sqlite3* db = nullptr;
    Connection* connection = nullptr;
    bool verify = false;
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

int diagnostic_connect(sqlite3* db, void* aux, int, const char* const* argv,
                       sqlite3_vtab** out, char**) {
    std::unique_ptr<DiagnosticTable> table(new DiagnosticTable());
    table->db = db;
    table->connection = static_cast<Connection*>(aux);
    table->verify = std::string(argv[0]) == "tsfile_verify";
    const char* schema =
        table->verify
            ? "CREATE TABLE x(segment_id INTEGER,path TEXT,status TEXT,detail "
              "TEXT,requested_table TEXT HIDDEN)"
            : "CREATE TABLE x(table_name TEXT,mode TEXT,timestamp_precision "
              "TEXT,watermark INTEGER,append_available INTEGER,"
              "source_max_time INTEGER,hot_rows INTEGER,file_count "
              "INTEGER,directory TEXT,source_file TEXT,source_table "
              "TEXT,requested_table TEXT HIDDEN)";
    int rc = sqlite3_declare_vtab(db, schema);
    if (rc != SQLITE_OK) return rc;
    sqlite3_vtab_config(db, SQLITE_VTAB_DIRECTONLY);
    *out = table.release();
    return SQLITE_OK;
}
int diagnostic_best(sqlite3_vtab* base, sqlite3_index_info* info) {
    int hidden = static_cast<DiagnosticTable*>(base)->verify ? 4 : 11;
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
    if (!diagnostic->verify) {
        std::string sql =
            "SELECT " + quote_sql(table->db_name + '.' + table->table_name) +
            ",mode,precision,watermark,(mode='writable' AND watermark IS NOT "
            "NULL),source_max," +
            (table->readonly ? "0"
                             : "(SELECT count(*) FROM " +
                                   shadow_name(table, "_data") + ")") +
            ",(SELECT count(*) FROM " + shadow_name(table, "_segments") +
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
        rc = sqlite3_create_module_v2(db, "tsfile_table_info",
                                      &kDiagnosticModule, connection, nullptr);
    if (rc == SQLITE_OK)
        rc = sqlite3_create_module_v2(db, "tsfile_verify", &kDiagnosticModule,
                                      connection, nullptr);
    return rc;
}
