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

#include <dirent.h>
#include <gtest/gtest.h>
#include <sqlite3.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include "common/tablet.h"
#include "reader/tsfile_reader.h"
#include "writer/tsfile_writer.h"

namespace {

class TsFileSqliteTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char directory[] = "/tmp/tsfile-sqlite-test-XXXXXX";
        ASSERT_NE(nullptr, mkdtemp(directory));
        directory_ = directory;
        std::string sql = "SELECT load_extension(" +
                          quote(TSFILE_SQLITE_EXTENSION_PATH) + ")";
        ASSERT_EQ(SQLITE_OK, sqlite3_open(":memory:", &db_));
        char* error = nullptr;
        ASSERT_EQ(SQLITE_OK, sqlite3_enable_load_extension(db_, 1));
        ASSERT_EQ(SQLITE_OK,
                  sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error))
            << (error == nullptr ? "" : error);
        sqlite3_free(error);
        ASSERT_EQ(SQLITE_OK,
                  exec("CREATE VIRTUAL TABLE sensor USING tsfile_hybrid("
                       "directory='" +
                       directory_ +
                       "',"
                       "timestamp_precision='ms',"
                       "time TIMESTAMP TIME,"
                       "device STRING TAG,"
                       "temperature DOUBLE FIELD)"));
    }

    void TearDown() override {
        if (db_ != nullptr) sqlite3_close(db_);
        db_ = nullptr;
    }

    int exec(const std::string& sql) {
        char* error = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error);
        if (rc != SQLITE_OK) {
            ADD_FAILURE() << (error == nullptr ? "" : error);
        }
        sqlite3_free(error);
        return rc;
    }

    int exec_raw(const std::string& sql) {
        char* error = nullptr;
        int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error);
        sqlite3_free(error);
        return rc;
    }

    int count(const std::string& sql) {
        sqlite3_stmt* stmt = nullptr;
        EXPECT_EQ(SQLITE_OK,
                  sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr));
        int result = -1;
        if (stmt != nullptr) {
            int rc = sqlite3_step(stmt);
            EXPECT_EQ(SQLITE_ROW, rc) << sqlite3_errmsg(db_);
            if (rc == SQLITE_ROW) result = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
        return result;
    }

    std::string source_file() {
        DIR* dir = opendir(directory_.c_str());
        if (!dir) return "";
        std::string result;
        while (dirent* e = readdir(dir)) {
            std::string name = e->d_name;
            if (name.size() > 7 && name.substr(name.size() - 7) == ".tsfile")
                result = directory_ + "/" + name;
        }
        closedir(dir);
        return result;
    }

    static std::string find_tsfile(const std::string& directory) {
        DIR* dir = opendir(directory.c_str());
        if (!dir) return "";
        std::string result;
        while (dirent* e = readdir(dir)) {
            std::string name = e->d_name;
            if (name.size() > 7 && name.substr(name.size() - 7) == ".tsfile")
                result = directory + "/" + name;
        }
        closedir(dir);
        return result;
    }

    static int count_tsfiles(const std::string& directory) {
        DIR* dir = opendir(directory.c_str());
        if (!dir) return 0;
        int result = 0;
        while (dirent* e = readdir(dir)) {
            std::string name = e->d_name;
            if (name.size() > 7 && name.substr(name.size() - 7) == ".tsfile")
                ++result;
        }
        closedir(dir);
        return result;
    }

    static std::vector<std::string> list_tsfiles(const std::string& directory) {
        std::vector<std::string> result;
        DIR* dir = opendir(directory.c_str());
        if (!dir) return result;
        while (dirent* e = readdir(dir)) {
            std::string name = e->d_name;
            if (name.size() > 7 && name.substr(name.size() - 7) == ".tsfile")
                result.push_back(directory + "/" + name);
        }
        closedir(dir);
        return result;
    }

    std::string scalar_text(const std::string& sql) {
        sqlite3_stmt* stmt = nullptr;
        EXPECT_EQ(SQLITE_OK,
                  sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr));
        std::string result;
        if (stmt && sqlite3_step(stmt) == SQLITE_ROW &&
            sqlite3_column_text(stmt, 0))
            result =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
        sqlite3_finalize(stmt);
        return result;
    }

    static std::string quote(const std::string& value) {
        std::string result = "'";
        for (char c : value) result += c == '\'' ? "''" : std::string(1, c);
        result += '\'';
        return result;
    }

    sqlite3* db_ = nullptr;
    std::string directory_;
};

TEST_F(TsFileSqliteTest, HotCrudAndSealRollback) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2.5)"));
    ASSERT_EQ(2, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK"));
    ASSERT_EQ(2, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(0, count("SELECT count(*) FROM \"sensor_tsfile$segments\""));

    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(2, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(1, count("SELECT count(*) FROM \"sensor_tsfile$segments\""));
    ASSERT_EQ(1, count("SELECT count(*) FROM sensor WHERE time=1"));
    ASSERT_EQ(1, count("SELECT count(*) FROM sensor WHERE time=2"));
    char* error = nullptr;
    int rc = sqlite3_exec(db_, "INSERT INTO sensor VALUES(1,'d0',9.0)", nullptr,
                          nullptr, &error);
    sqlite3_free(error);
    EXPECT_EQ(SQLITE_CONSTRAINT, rc);
}

TEST_F(TsFileSqliteTest, Int32RangeAndColdRowsAreImmutable) {
    ASSERT_EQ(SQLITE_OK, exec("CREATE VIRTUAL TABLE ints USING tsfile_hybrid("
                              "directory='" +
                              directory_ +
                              "-ints',timestamp_precision='ms',"
                              "time TIMESTAMP TIME,"
                              "device STRING TAG,"
                              "reading INT32 FIELD)"));
    EXPECT_EQ(SQLITE_MISMATCH,
              exec_raw("INSERT INTO ints VALUES(1,'d0',2147483648)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO ints VALUES(1,'d0',42)"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.ints',2)"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("UPDATE ints SET reading=43 WHERE time=1"));
    EXPECT_EQ(SQLITE_READONLY, exec_raw("DELETE FROM ints WHERE time=1"));
    EXPECT_EQ(1, count("SELECT count(*) FROM ints "
                       "WHERE time=1 AND reading=42"));
}

TEST_F(TsFileSqliteTest, NullAndBlobSurviveSeal) {
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE payload USING tsfile_hybrid("
                   "directory='" +
                   directory_ +
                   "-payload',timestamp_precision='ms',"
                   "time TIMESTAMP TIME,"
                   "device STRING TAG,"
                   "payload BLOB FIELD,"
                   "note TEXT FIELD)"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO payload VALUES(1,'d0',X'000102','')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO payload VALUES(2,'d0',NULL,NULL)"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.payload',3)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM payload "
                       "WHERE time=1 AND hex(payload)='000102'"));
    EXPECT_EQ(1, count("SELECT count(*) FROM payload "
                       "WHERE time=2 AND payload IS NULL AND note IS NULL"));
    EXPECT_EQ(1, count("SELECT count(*) FROM payload "
                       "WHERE time=1 AND length(note)=0"));
}

TEST_F(TsFileSqliteTest, AttachedDatabaseUsesItsOwnShadowTables) {
    ASSERT_EQ(SQLITE_OK, exec("ATTACH ':memory:' AS aux"));
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE aux.attached USING tsfile_hybrid("
                   "directory='" +
                   directory_ +
                   "-attached',timestamp_precision='ms',"
                   "time TIMESTAMP TIME,"
                   "device STRING TAG,"
                   "reading INT32 FIELD)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO aux.attached VALUES(1,'d0',7)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM aux.\"attached_tsfile$hot\""));
    EXPECT_EQ(0, count("SELECT count(*) FROM main.sqlite_master "
                       "WHERE name='attached_tsfile$hot'"));
}

TEST_F(TsFileSqliteTest, SqlDeclarationsAndNullableLogicalKeys) {
    ASSERT_EQ(SQLITE_OK, exec("CREATE VIRTUAL TABLE modern USING tsfile_hybrid("
                              "time TIMESTAMP TIME, device STRING TAG, value "
                              "DOUBLE FIELD, directory=" +
                              quote(directory_ + "-modern") +
                              ",timestamp_precision='ms')"));
    ASSERT_EQ(
        SQLITE_OK,
        exec(
            "INSERT INTO modern VALUES(1,NULL,1.5),(1,'',2.5),(1,'null',3.5)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO modern VALUES(1,NULL,9)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM modern"));
    EXPECT_EQ(
        1,
        count(
            "SELECT count(*) FROM modern WHERE device IS NULL AND value=1.5"));
    ASSERT_EQ(SQLITE_OK,
              exec("UPDATE modern SET value=4 WHERE device IS NULL"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("UPDATE modern SET device=NULL WHERE device=''"));
    EXPECT_EQ(
        1, count("SELECT count(*) FROM modern WHERE device='' AND value=2.5"));
}

TEST_F(TsFileSqliteTest, NoTagsAndInvalidDeclarations) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE times USING tsfile_hybrid("
             "time TIMESTAMP TIME, value DOUBLE FIELD, directory=" +
             quote(directory_ + "-times") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO times VALUES(1,2)"));
    EXPECT_EQ(SQLITE_CONSTRAINT, exec_raw("INSERT INTO times VALUES(1,3)"));
    EXPECT_NE(
        SQLITE_OK,
        exec_raw("CREATE VIRTUAL TABLE bad USING tsfile_hybrid("
                 "time TIMESTAMP TIME, value DOUBLE, directory=" +
                 quote(directory_ + "-bad") + ",timestamp_precision='ms')"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("CREATE VIRTUAL TABLE bad USING tsfile_hybrid("
                       "time TIMESTAMP TIME, value DOUBLE FIELD, directory=" +
                       quote(directory_ + "-bad") +
                       ",timestamp_precision='ms', timestamp_precision='us')"));
}

TEST_F(TsFileSqliteTest, ExternalFileInferredSchemaAndAppendBoundary) {
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(1,'d0',1.5),(2,'d1',2.5)"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',3)"));
    const std::string path = source_file();
    ASSERT_FALSE(path.empty());
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE history USING tsfile_hybrid(file=" +
                   quote(path) + ",source_table='sensor')"));
    EXPECT_EQ(2, count("SELECT count(*) FROM history"));
    EXPECT_EQ(
        "INTEGER",
        scalar_text(
            "SELECT type FROM pragma_table_info('history') WHERE name='time'"));
    EXPECT_EQ("REAL",
              scalar_text("SELECT type FROM pragma_table_info('history') WHERE "
                          "name='temperature'"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("INSERT INTO history VALUES(3,'d0',9)"));
    EXPECT_EQ(SQLITE_OK, exec_raw("DELETE FROM history WHERE 0"));
    EXPECT_EQ(SQLITE_READONLY, exec_raw("DELETE FROM history WHERE time=1"));
    EXPECT_EQ(SQLITE_OK, exec_raw("UPDATE history SET temperature=8 WHERE 0"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("UPDATE history SET temperature=8 WHERE time=1"));
    EXPECT_EQ(0, count("SELECT count(*) FROM sqlite_master WHERE "
                       "name='history_tsfile$hot'"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE continuation USING tsfile_hybrid(file=" +
             quote(path) + ",source_table='sensor',directory=" +
             quote(directory_ + "-continuation") + ")"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO continuation VALUES(2,'d1',9)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO continuation VALUES(3,NULL,3.5)"));
    ASSERT_EQ(SQLITE_OK,
              exec("UPDATE continuation SET temperature=4.5 WHERE time=3"));
    EXPECT_EQ(3, count("SELECT count(*) FROM continuation"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("UPDATE OR IGNORE continuation SET temperature=7"));
    EXPECT_EQ(1, count("SELECT count(*) FROM continuation WHERE time=3 AND "
                       "temperature=4.5"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("DELETE FROM continuation WHERE time=1"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.continuation',4)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM continuation"));
    EXPECT_EQ(1, count("SELECT count(*) FROM continuation WHERE time=3 AND "
                       "device IS NULL"));
}

TEST_F(TsFileSqliteTest, SourceValidationAndPreserveUnknownFiles) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    std::string path = source_file();
    EXPECT_NE(SQLITE_OK,
              exec_raw("CREATE VIRTUAL TABLE bad USING tsfile_hybrid(file=" +
                       quote(path) + ",source_table='missing')"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("CREATE VIRTUAL TABLE bad USING tsfile_hybrid(file=" +
                       quote(path) +
                       ",source_table='sensor', timestamp_precision='ns')"));
    EXPECT_NE(SQLITE_OK, exec_raw("CREATE VIRTUAL TABLE bad USING "
                                  "tsfile_hybrid(time TIMESTAMP TIME,file=" +
                                  quote(path) + ",source_table='sensor')"));
    const std::string unknown = directory_ + "/unrelated.tsfile";
    {
        std::ofstream file(unknown);
        file << "owned by caller";
    }
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',3)"));
    EXPECT_EQ(0, access(unknown.c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, ManagementSealRespectsTransactionsAndSavepoints) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("INSERT INTO sensor VALUES(1,'d0',1),(2,NULL,2),(3,'d0',3)"));
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    EXPECT_EQ(3, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(0,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    ASSERT_EQ(SQLITE_OK, exec("SAVEPOINT outer_point"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(4,'d0',4)"));
    EXPECT_EQ(1,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    ASSERT_EQ(SQLITE_OK, exec("SAVEPOINT inner_point"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(5,'d0',5)"));
    EXPECT_EQ(2,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK TO inner_point"));
    EXPECT_EQ(1,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    ASSERT_EQ(SQLITE_OK, exec("RELEASE inner_point"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK TO outer_point"));
    EXPECT_EQ(0,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    ASSERT_EQ(SQLITE_OK, exec("RELEASE outer_point"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK"));
    EXPECT_EQ(3,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(3, count("SELECT tsfile_seal('main.sensor',4)"));
    EXPECT_EQ(0,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor"));
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('main.sensor') "
                       "WHERE status='OK'"));
}

TEST_F(TsFileSqliteTest, ExportCopiesSealedFilesAndRoundTrips) {
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(1,NULL,1.5),(2,'d0',2.5)"));
    const std::string early = directory_ + "-export-early";
    EXPECT_EQ(
        0, count("SELECT tsfile_export('main.sensor'," + quote(early) + ")"));
    EXPECT_EQ(2,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_TRUE(find_tsfile(early).empty());
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    const std::string output = directory_ + "-export";
    EXPECT_EQ(
        1, count("SELECT tsfile_export('main.sensor'," + quote(output) + ")"));
    EXPECT_EQ(0,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(2,
              count("SELECT watermark FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(2, count("SELECT count(*) FROM sensor"));
    const std::string exported_file = find_tsfile(output);
    ASSERT_FALSE(exported_file.empty());
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE exported USING tsfile_hybrid(file=" +
                   quote(exported_file) + ",source_table='sensor')"));
    EXPECT_EQ(2, count("SELECT count(*) FROM exported"));
    EXPECT_EQ(1, count("SELECT count(*) FROM exported WHERE device IS NULL AND "
                       "temperature=1.5"));
    EXPECT_EQ(SQLITE_READONLY, exec_raw("UPDATE sensor SET temperature=9"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(3,'d0',3.5)"));
    EXPECT_EQ(2, count("SELECT count(*) FROM exported"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.sensor'," +
                                  quote(output) + ")"));
    EXPECT_EQ(1,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-export-hot") + ")"));
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.sensor'," +
                                  quote(output + "-txn") + ")"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK"));
}

TEST_F(TsFileSqliteTest, DiagnosticsAndInt64Exhaustion) {
    EXPECT_EQ("writable",
              scalar_text("SELECT mode FROM tsfile_table_info('main.sensor')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(9223372036854775807,'d0',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-max") + ")"));
    EXPECT_EQ(1, count("SELECT watermark IS NULL FROM "
                       "tsfile_device_info('main.sensor')"));
    EXPECT_EQ(
        1,
        count(
            "SELECT watermark IS NULL FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor"));
    EXPECT_EQ(
        SQLITE_CONSTRAINT,
        exec_raw("INSERT INTO sensor VALUES(9223372036854775807,'d0',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-again") + ")"));
    const auto path = source_file();
    ASSERT_EQ(0, unlink(path.c_str()));
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('main.sensor') "
                       "WHERE status='MISSING'"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT * FROM sensor"));
}

TEST_F(TsFileSqliteTest, DirectoryOwnershipAndCreationRollback) {
    std::string args =
        "(time TIMESTAMP TIME,value DOUBLE "
        "FIELD,timestamp_precision='ms',directory=";
    EXPECT_NE(SQLITE_OK,
              exec_raw("CREATE VIRTUAL TABLE clash USING tsfile_hybrid" + args +
                       quote(directory_) + ")"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("CREATE VIRTUAL TABLE nested USING tsfile_hybrid" +
                       args + quote(directory_ + "/nested") + ")"));
    std::string rollback_dir = directory_ + "-rollback";
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE aborted USING tsfile_hybrid" + args +
                   quote(rollback_dir) + ")"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK"));
    EXPECT_NE(0, access(rollback_dir.c_str(), F_OK));
    ASSERT_EQ(SQLITE_OK, exec("CREATE TABLE \"collision_tsfile$config\"(x)"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("CREATE VIRTUAL TABLE collision USING tsfile_hybrid" +
                       args + quote(directory_ + "-collision") + ")"));
    EXPECT_EQ(
        1,
        count("SELECT count(*) FROM "
              "pragma_table_info('collision_tsfile$config') WHERE name='x'"));
    EXPECT_NE(0, access((directory_ + "-collision").c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, RawMultiTablePrecisionAndExportIsolation) {
    std::string path = directory_ + "-raw.tsfile";
    storage::TsFileWriter writer;
    ASSERT_EQ(common::E_OK, writer.open(path));
    writer.set_generate_table_schema(false);
    for (const std::string name : {"selected", "unrelated", "empty"}) {
        std::vector<common::ColumnSchema> columns = {
            common::ColumnSchema("device", common::STRING,
                                 common::ColumnCategory::TAG),
            common::ColumnSchema("value", common::DOUBLE,
                                 common::ColumnCategory::FIELD)};
        auto schema = std::make_shared<storage::TableSchema>(name, columns);
        ASSERT_EQ(common::E_OK, writer.register_table(schema));
        if (name == "empty") continue;
        storage::Tablet tablet(name, schema->get_measurement_names(),
                               schema->get_data_types(),
                               schema->get_column_categories(), 2);
        ASSERT_EQ(common::E_OK,
                  tablet.add_timestamp(0, name == "selected" ? 10 : 1000));
        ASSERT_EQ(common::E_OK, tablet.add_value(0, "device", "d0"));
        ASSERT_EQ(common::E_OK, tablet.add_value(0, "value", 1.5));
        ASSERT_EQ(common::E_OK, writer.write_table(tablet));
    }
    ASSERT_EQ(common::E_OK, writer.flush());
    ASSERT_EQ(common::E_OK, writer.close());
    std::string options = "file=" + quote(path) + ",source_table='selected'";
    ASSERT_EQ(SQLITE_OK, exec("CREATE VIRTUAL TABLE raw USING tsfile_hybrid(" +
                              options + ")"));
    EXPECT_EQ(
        "unknown",
        scalar_text(
            "SELECT timestamp_precision FROM tsfile_table_info('main.raw')"));
    EXPECT_NE(
        SQLITE_OK,
        exec_raw("CREATE VIRTUAL TABLE missing_precision USING tsfile_hybrid(" +
                 options + ",directory=" +
                 quote(directory_ + "-missing-precision") + ")"));
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE renamed USING tsfile_hybrid(" +
                   options + ",timestamp_precision='ms',directory=" +
                   quote(directory_ + "-renamed") + ")"));
    EXPECT_EQ(11,
              count("SELECT watermark FROM tsfile_table_info('main.renamed')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO renamed VALUES(11,NULL,2.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.renamed',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.renamed'," +
                       quote(directory_ + "-selected-export") + ")"));
    storage::TsFileReader reader;
    ASSERT_EQ(common::E_OK,
              reader.open(find_tsfile(directory_ + "-selected-export")));
    EXPECT_EQ(1, reader.get_all_table_schemas().size());
    EXPECT_NE(nullptr, reader.get_table_schema("renamed"));
    EXPECT_EQ(nullptr, reader.get_table_schema("unrelated"));
    reader.close();
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE empty_source USING tsfile_hybrid(file=" +
             quote(path) +
             ",source_table='empty',timestamp_precision='ms',directory=" +
             quote(directory_ + "-empty") + ")"));
    EXPECT_EQ(0, count("SELECT count(*) FROM empty_source"));
    EXPECT_EQ(0, count("SELECT tsfile_export('main.empty_source'," +
                       quote(directory_ + "-empty-output") + ")"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("INSERT INTO empty_source VALUES(-9223372036854775808,NULL,1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.empty_source',1)"));
}

TEST_F(TsFileSqliteTest, PersistentReconnectAndConcurrentWatermark) {
    const std::string dbpath = directory_ + "-persistent.db";
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(dbpath) + " AS persisted"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE persisted.data USING tsfile_hybrid(time "
             "TIMESTAMP TIME,value DOUBLE FIELD,directory=" +
             quote(directory_ + "-persisted") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO persisted.data VALUES(1,1.5),(2,2.5)"));
    EXPECT_EQ(2, count("SELECT tsfile_seal('persisted.data',2)"));
    sqlite3* other = nullptr;
    ASSERT_EQ(SQLITE_OK, sqlite3_open(dbpath.c_str(), &other));
    sqlite3_enable_load_extension(other, 1);
    ASSERT_EQ(SQLITE_OK,
              sqlite3_load_extension(other, TSFILE_SQLITE_EXTENSION_PATH,
                                     nullptr, nullptr));
    ASSERT_EQ(SQLITE_OK, sqlite3_exec(other, "SELECT * FROM data", nullptr,
                                      nullptr, nullptr));
    EXPECT_EQ(0, count("SELECT tsfile_seal('persisted.data',3)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              sqlite3_exec(other, "INSERT INTO data VALUES(2,8)", nullptr,
                           nullptr, nullptr));
    ASSERT_EQ(SQLITE_OK, sqlite3_exec(other, "INSERT INTO data VALUES(3,3.5)",
                                      nullptr, nullptr, nullptr));
    EXPECT_EQ(3, count("SELECT count(*) FROM persisted.data"));
    sqlite3_close(other);
    ASSERT_EQ(SQLITE_OK, exec("DETACH persisted"));
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(dbpath) + " AS persisted"));
    EXPECT_EQ(3, count("SELECT count(*) FROM persisted.data"));
    EXPECT_EQ(
        1, count("SELECT hot_rows FROM tsfile_table_info('persisted.data')"));
    EXPECT_EQ(
        3, count("SELECT watermark FROM tsfile_table_info('persisted.data')"));
}

TEST_F(TsFileSqliteTest, ExportFailureRetainsCommittedSealAndManagementGuards) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1)"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_seal('main.sensor',2) FROM sensor"));
    EXPECT_EQ(1,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    const std::string output = "/tmp/" + std::string(240, 'x');
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.sensor'," +
                                  quote(output) + ")"));
    EXPECT_EQ(1,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor"));
    EXPECT_NE(0, access(output.c_str(), F_OK));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-retry") + ")"));
}

TEST_F(TsFileSqliteTest, NoTagSealingAndWholeStatementColdFailures) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE notags USING tsfile_hybrid(time TIMESTAMP "
             "TIME,value DOUBLE FIELD,directory=" +
             quote(directory_ + "-notags") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO notags VALUES(1,1.5),(2,2.5)"));
    EXPECT_EQ(2, count("SELECT tsfile_seal('main.notags',2)"));
    EXPECT_EQ(2, count("SELECT count(*) FROM notags"));
    EXPECT_EQ(SQLITE_READONLY, exec_raw("UPDATE OR FAIL notags SET value=9"));
    EXPECT_EQ(1,
              count("SELECT count(*) FROM notags WHERE time=2 AND value=2.5"));
    EXPECT_EQ(SQLITE_READONLY, exec_raw("DELETE FROM notags"));
    EXPECT_EQ(2, count("SELECT count(*) FROM notags"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO notags VALUES(3,3.5),(2,8)"));
    EXPECT_EQ(2, count("SELECT count(*) FROM notags"));
    EXPECT_EQ(0, count("SELECT tsfile_seal('main.notags',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO notags VALUES(3,3.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.notags',4)"));
    EXPECT_EQ(0, count("SELECT tsfile_seal('main.notags',5)"));
    EXPECT_EQ(4,
              count("SELECT watermark FROM tsfile_table_info('main.notags')"));
}

TEST_F(TsFileSqliteTest, SourceSchemaPersistsWhenFileBecomesUnavailable) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    const std::string dbpath = directory_ + "-source.db";
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(dbpath) + " AS persisted"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE persisted.source USING tsfile_hybrid(file=" +
             quote(path) + ",source_table='sensor',directory=" +
             quote(directory_ + "-source-hot") + ")"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO persisted.source VALUES(2,NULL,2)"));
    ASSERT_EQ(SQLITE_OK, exec("DETACH persisted"));
    ASSERT_EQ(0, rename(path.c_str(), (path + ".moved").c_str()));
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(dbpath) + " AS persisted"));
    EXPECT_EQ(
        3,
        count("SELECT count(*) FROM pragma_table_info('source','persisted')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('persisted.source') "
                       "WHERE status='MISSING'"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT * FROM persisted.source"));
    EXPECT_EQ(
        1, count("SELECT hot_rows FROM tsfile_table_info('persisted.source')"));
    ASSERT_EQ(0, rename((path + ".moved").c_str(), path.c_str()));
    EXPECT_EQ(2, count("SELECT count(*) FROM persisted.source"));
}

TEST_F(TsFileSqliteTest, QuotedSchemaAndRejectedEmptyPrecision) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE \"odd.table\" USING tsfile_hybrid(\"event "
             "time\" TIMESTAMP TIME,\"sensor value\" DOUBLE FIELD,directory=" +
             quote(directory_ + "-quoted") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO \"odd.table\" VALUES(1,3.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.\"odd.table\"',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.\"odd.table\"'," +
                       quote(directory_ + "-quoted-output") + ")"));
    const std::string quoted_file = find_tsfile(directory_ + "-quoted-output");
    ASSERT_FALSE(quoted_file.empty());
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE inferred USING tsfile_hybrid(file=" +
                   quote(quoted_file) + ",source_table='odd.table')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM inferred WHERE \"event time\"=1 "
                       "AND \"sensor value\"=3.5"));
    EXPECT_NE(
        SQLITE_OK,
        exec_raw("CREATE VIRTUAL TABLE invalid USING tsfile_hybrid(file=" +
                 quote(quoted_file) +
                 ",source_table='odd.table',timestamp_precision='')"));
}

TEST_F(TsFileSqliteTest, BusinessColumnsDoNotCarryManagementCommands) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE commands USING tsfile_hybrid(time TIMESTAMP "
             "TIME,_tsfile_command STRING FIELD,directory=" +
             quote(directory_ + "-commands") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO commands VALUES(1,'seal')"));
    EXPECT_EQ(
        1, count("SELECT count(*) FROM commands WHERE _tsfile_command='seal'"));
    EXPECT_EQ(2, count("SELECT count(*) FROM pragma_table_xinfo('commands')"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.commands',2)"));
}

TEST_F(TsFileSqliteTest, RowidNamedColumnsAndBooleanValues) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE aliases USING tsfile_hybrid(time TIMESTAMP "
             "TIME,rowid INT64 FIELD,oid INT64 FIELD,_rowid_ INT64 FIELD,flag "
             "BOOLEAN FIELD,directory=" +
             quote(directory_ + "-aliases") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO aliases "
                              "VALUES(1,-1,88,77,4294967296),(2,-1,55,44,0)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM aliases WHERE time=1 AND flag=1"));
    ASSERT_EQ(SQLITE_OK, exec("UPDATE aliases SET rowid=999 WHERE time=1"));
    EXPECT_EQ(1, count("SELECT count(*) FROM aliases WHERE time=1 AND "
                       "rowid=999 AND oid=88 AND _rowid_=77"));
    ASSERT_EQ(SQLITE_OK, exec("DELETE FROM aliases WHERE time=2"));
    EXPECT_EQ(1, count("SELECT count(*) FROM aliases"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.aliases',3)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM aliases WHERE time=1 AND "
                       "rowid=999 AND flag=1"));
}

TEST_F(TsFileSqliteTest, CopiedDatabaseCannotShareWritableDirectory) {
    const std::string original = directory_ + "-original.db",
                      copy = directory_ + "-copy.db";
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(original) + " AS owner"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE owner.data USING tsfile_hybrid(time "
             "TIMESTAMP TIME,value DOUBLE FIELD,directory=" +
             quote(directory_ + "-owned") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO owner.data VALUES(1,1.5)"));
    ASSERT_EQ(SQLITE_OK, exec("DETACH owner"));
    {
        std::ifstream in(original, std::ios::binary);
        std::ofstream out(copy, std::ios::binary);
        out << in.rdbuf();
    }
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(copy) + " AS cloned"));
    EXPECT_NE(SQLITE_OK, exec_raw("INSERT INTO cloned.data VALUES(2,2.5)"));
    ASSERT_EQ(SQLITE_OK, exec("DETACH cloned"));
    ASSERT_EQ(SQLITE_OK,
              exec("ATTACH " + quote(original) + " AS renamed_schema"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO renamed_schema.data VALUES(2,2.5)"));
    EXPECT_EQ(2, count("SELECT count(*) FROM renamed_schema.data"));
}

TEST_F(TsFileSqliteTest, RollbackPastTableCreationPreservesOuterTransaction) {
    ASSERT_EQ(SQLITE_OK, exec("CREATE TABLE keep(value)"));
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO keep VALUES(42)"));
    ASSERT_EQ(SQLITE_OK, exec("SAVEPOINT creation"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE transient USING tsfile_hybrid(time "
             "TIMESTAMP TIME,value DOUBLE FIELD,directory=" +
             quote(directory_ + "-transient") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO transient VALUES(1,1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.transient',2)"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK TO creation"));
    ASSERT_EQ(SQLITE_OK, exec("RELEASE creation"));
    ASSERT_EQ(SQLITE_OK, exec("COMMIT"));
    EXPECT_EQ(42, count("SELECT value FROM keep"));
    EXPECT_EQ(
        0, count("SELECT count(*) FROM sqlite_master WHERE name='transient'"));
    EXPECT_NE(0, access((directory_ + "-transient").c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, CommitNeverOverwritesExistingDirectoryEntries) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    std::string final;
    DIR* dir = opendir(directory_.c_str());
    ASSERT_NE(nullptr, dir);
    while (dirent* e = readdir(dir)) {
        std::string name = e->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".tmp")
            final =
                directory_ + "/" + name.substr(0, name.size() - 4) + ".tsfile";
    }
    closedir(dir);
    ASSERT_FALSE(final.empty());
    ASSERT_EQ(0, symlink((directory_ + "-missing").c_str(), final.c_str()));
    EXPECT_NE(SQLITE_OK, exec_raw("COMMIT"));
    struct stat st {};
    ASSERT_EQ(0, lstat(final.c_str(), &st));
    EXPECT_TRUE(S_ISLNK(st.st_mode));
    EXPECT_EQ(
        1,
        count("SELECT count(*) FROM sensor WHERE time=1 AND temperature=1.5"));
    EXPECT_EQ(1,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
}

TEST_F(TsFileSqliteTest, SealPreservesPreexistingTemporarySymlink) {
    const std::string temporary =
        directory_ + "/tsfilesensor-" + std::to_string(getpid()) + "-0.tmp";
    ASSERT_EQ(0, symlink((directory_ + "-missing").c_str(), temporary.c_str()));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    struct stat st {};
    ASSERT_EQ(0, lstat(temporary.c_str(), &st));
    EXPECT_TRUE(S_ISLNK(st.st_mode));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor"));
}

TEST_F(TsFileSqliteTest, ReconnectedFileTableRestoresDeviceBoundaries) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2000,'d1',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    const std::string dbpath = directory_ + "-file-reconnect.db";
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(dbpath) + " AS persisted"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE persisted.continued USING "
             "tsfile_hybrid(file=" +
             quote(path) +
             ",source_table='sensor',timestamp_precision='ms',directory=" +
             quote(directory_ + "-file-reconnect-hot") + ")"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO persisted.continued VALUES(2001,'d1',2)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO persisted.continued VALUES(1500,'d1',3)"));
    ASSERT_EQ(SQLITE_OK, exec("DETACH persisted"));
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(dbpath) + " AS persisted"));
    EXPECT_EQ(2, count("SELECT count(*) FROM persisted.continued"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO persisted.continued VALUES(1000,'d1',4)"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO persisted.continued VALUES(1500,'new',5)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM persisted.continued"));
}

TEST_F(TsFileSqliteTest, SealSplitsOutputBySizeLimit) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE big USING tsfile_hybrid(time TIMESTAMP "
             "TIME,device STRING TAG,payload BLOB FIELD,directory=" +
             quote(directory_ + "-big") + ",timestamp_precision='ms')"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("WITH RECURSIVE seq(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM "
             "seq WHERE x<12) INSERT INTO big SELECT x*1000,'d'||(x%2),"
             "randomblob(200000) FROM seq"));
    EXPECT_EQ(12, count("SELECT count(*) FROM big"));
    EXPECT_EQ(12, count("SELECT tsfile_seal('main.big',1)"));
    const auto files = list_tsfiles(directory_ + "-big");
    EXPECT_GE(files.size(), 2u);
    for (const std::string& path : files) {
        struct stat st {};
        ASSERT_EQ(0, stat(path.c_str(), &st));
        EXPECT_LE(st.st_size, 1024 * 1024);
    }
    EXPECT_EQ(12, count("SELECT count(*) FROM big"));
    EXPECT_EQ(static_cast<int>(files.size()),
              count("SELECT file_count FROM tsfile_table_info('main.big')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO big VALUES(13000,'d0',randomblob(1200000))"));
    EXPECT_EQ(SQLITE_CONSTRAINT, exec_raw("SELECT tsfile_seal('main.big',1)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM big WHERE time=13000"));
    EXPECT_EQ(static_cast<int>(files.size()),
              count("SELECT file_count FROM tsfile_table_info('main.big')"));
}

TEST_F(TsFileSqliteTest, CompactMergesSmallSealedFiles) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(3,'d0',3)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(3, count_tsfiles(directory_));
    EXPECT_EQ(1, count("SELECT tsfile_compact('main.sensor',10,20)"));
    EXPECT_EQ(1, count_tsfiles(directory_));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor"));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor WHERE device='d0'"));
    EXPECT_EQ(4,
              count("SELECT watermark FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM \"sensor_tsfile$segments\""));
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('main.sensor') "
                       "WHERE status='OK'"));
    EXPECT_EQ(0, count("SELECT tsfile_compact('main.sensor',10,20)"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_compact('main.sensor',20,10)"));
}

TEST_F(TsFileSqliteTest, CompactRollbackKeepsOriginalFiles) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(3,'d0',3)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(3, count_tsfiles(directory_));
    ASSERT_EQ(SQLITE_OK, exec("BEGIN"));
    EXPECT_EQ(1, count("SELECT tsfile_compact('main.sensor',10,20)"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK"));
    EXPECT_EQ(3, count_tsfiles(directory_));
    EXPECT_EQ(3, count("SELECT count(*) FROM \"sensor_tsfile$segments\""));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor"));
    EXPECT_EQ(1, count("SELECT tsfile_compact('main.sensor',10,20)"));
    EXPECT_EQ(1, count_tsfiles(directory_));
}

TEST_F(TsFileSqliteTest, DeviceInfoReportsPerDeviceWatermarks) {
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(1,'d0',1),(5,'d1',2)"));
    EXPECT_EQ(5, count("SELECT max_time FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d1'"));
    EXPECT_EQ(1, count("SELECT hot_rows FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d1'"));
    EXPECT_EQ(2, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(2,
              count("SELECT count(*) FROM tsfile_device_info('main.sensor')"));
    EXPECT_EQ(2,
              count("SELECT watermark FROM tsfile_device_info('main.sensor') "
                    "WHERE device_key='d0'"));
    EXPECT_EQ(6,
              count("SELECT watermark FROM tsfile_device_info('main.sensor') "
                    "WHERE device_key='d1'"));
    EXPECT_EQ(1, count("SELECT max_time FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d0'"));
    EXPECT_EQ(0, count("SELECT hot_rows FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d1'"));
    EXPECT_EQ(
        2, count("SELECT device_count FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(2,
              count("SELECT watermark FROM tsfile_table_info('main.sensor')"));
}

TEST_F(TsFileSqliteTest, DirectorySourceAndReadonlyExport) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE history_dir USING tsfile_hybrid(file=" +
             quote(directory_) + ",source_table='sensor')"));
    EXPECT_EQ(2, count("SELECT count(*) FROM history_dir"));
    EXPECT_EQ(
        "readonly",
        scalar_text("SELECT mode FROM tsfile_table_info('main.history_dir')"));
    EXPECT_EQ(2, count("SELECT file_count FROM "
                       "tsfile_table_info('main.history_dir')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM "
                       "tsfile_device_info('main.history_dir')"));
    EXPECT_EQ(1, count("SELECT watermark IS NULL FROM "
                       "tsfile_device_info('main.history_dir')"));
    EXPECT_EQ(0, count("SELECT hot_rows FROM "
                       "tsfile_device_info('main.history_dir')"));
    const std::string output = directory_ + "-directory-export";
    EXPECT_EQ(2, count("SELECT tsfile_export('main.history_dir'," +
                       quote(output) + ")"));
    EXPECT_EQ(2, count_tsfiles(output));
}

TEST_F(TsFileSqliteTest, FieldPredicatesFilterHotAndColdRows) {
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(1,'d0',1.5),(2,'d0',2.5),"
                   "(3,'d1',3.5),(4,'d1',NULL),(5,'d0',5.0)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor WHERE temperature>2.0"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor WHERE temperature>2.0"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor WHERE temperature=2.5"));
    EXPECT_EQ(2, count("SELECT count(*) FROM sensor WHERE temperature>=2.0 "
                       "AND temperature<5.0"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor WHERE temperature>=2.0 "
                       "AND device='d1'"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor WHERE time=4 AND "
                       "temperature IS NULL"));
    EXPECT_EQ(1,
              count("SELECT count(*) FROM sensor WHERE temperature IS NULL"));
    EXPECT_EQ(0, count("SELECT count(*) FROM sensor WHERE temperature>100"));
}

TEST_F(TsFileSqliteTest, FieldPredicatesSupportTypedFields) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE typed USING tsfile_hybrid("
             "time TIMESTAMP TIME,device STRING TAG,label TEXT FIELD,"
             "qty INT32 FIELD,flag BOOLEAN FIELD,directory=" +
             quote(directory_ + "-typed") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO typed VALUES(1,'d0','alpha',10,1),"
                              "(2,'d0','beta',20,0),(3,'d1',NULL,30,1)"));
    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.typed',2)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM typed WHERE label='alpha'"));
    EXPECT_EQ(0, count("SELECT count(*) FROM typed WHERE label='ALPHA'"));
    EXPECT_EQ(2, count("SELECT count(*) FROM typed WHERE qty>=20"));
    EXPECT_EQ(2, count("SELECT count(*) FROM typed WHERE flag=1"));
    EXPECT_EQ(1, count("SELECT count(*) FROM typed WHERE label='alpha' AND "
                       "qty=10 AND flag=1"));
    EXPECT_EQ(1, count("SELECT count(*) FROM typed WHERE label IS NULL"));
}

TEST_F(TsFileSqliteTest, SchemaFunctionListsTsfileTypes) {
    ASSERT_EQ(
        SQLITE_OK,
        exec(
            "CREATE VIRTUAL TABLE typed_schema USING tsfile_hybrid("
            "time TIMESTAMP TIME,group_id STRING TAG,device_id STRING TAG,"
            "d DOUBLE FIELD,i64 INT64 FIELD,i32 INT32 FIELD,flag BOOLEAN FIELD,"
            "label TEXT FIELD,payload BLOB FIELD,day DATE FIELD,directory=" +
            quote(directory_ + "-schema") + ",timestamp_precision='ms')"));
    EXPECT_EQ(10,
              count("SELECT count(*) FROM tsfile_schema('main.typed_schema')"));
    EXPECT_EQ(
        "TIMESTAMP",
        scalar_text("SELECT tsfile_type FROM "
                    "tsfile_schema('main.typed_schema') WHERE name='time'"));
    EXPECT_EQ(
        "TAG",
        scalar_text("SELECT category FROM tsfile_schema('main.typed_schema') "
                    "WHERE name='group_id'"));
    EXPECT_EQ(
        "FIELD",
        scalar_text("SELECT category FROM tsfile_schema('main.typed_schema') "
                    "WHERE name='label'"));
    EXPECT_EQ("REAL",
              scalar_text("SELECT sqlite_type FROM "
                          "tsfile_schema('main.typed_schema') WHERE name='d'"));
    EXPECT_EQ(
        "INTEGER",
        scalar_text("SELECT sqlite_type FROM "
                    "tsfile_schema('main.typed_schema') WHERE name='i32'"));
    EXPECT_EQ("BLOB", scalar_text("SELECT sqlite_type FROM tsfile_schema('main."
                                  "typed_schema') WHERE name='payload'"));
    EXPECT_EQ(1,
              count("SELECT not_null FROM tsfile_schema('main.typed_schema') "
                    "WHERE name='time'"));
    EXPECT_EQ(0,
              count("SELECT not_null FROM tsfile_schema('main.typed_schema') "
                    "WHERE name='label'"));
    EXPECT_EQ("time,group_id,device_id,d,i64,i32,flag,label,payload,day",
              scalar_text("SELECT group_concat(name, ',') FROM "
                          "tsfile_schema('main.typed_schema')"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT * FROM tsfile_schema('main.missing')"));
}

TEST_F(TsFileSqliteTest, SchemaFunctionWorksForInferredFileTables) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE history_schema USING tsfile_hybrid(file=" +
             quote(path) + ",source_table='sensor')"));
    EXPECT_EQ(3, count("SELECT count(*) FROM "
                       "tsfile_schema('main.history_schema')"));
    EXPECT_EQ("TIMESTAMP", scalar_text("SELECT tsfile_type FROM "
                                       "tsfile_schema('main.history_schema') "
                                       "WHERE name='time'"));
    EXPECT_EQ("TAG", scalar_text("SELECT category FROM "
                                 "tsfile_schema('main.history_schema') "
                                 "WHERE name='device'"));
    EXPECT_EQ("DOUBLE", scalar_text("SELECT tsfile_type FROM "
                                    "tsfile_schema('main.history_schema') "
                                    "WHERE name='temperature'"));
    EXPECT_EQ(0, count("SELECT not_null FROM "
                       "tsfile_schema('main.history_schema') "
                       "WHERE name='device'"));
}

TEST_F(TsFileSqliteTest, ManagementFunctionGuardsAndValidation) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(SQLITE_MISUSE,
              exec_raw("SELECT tsfile_seal('main.sensor','bad')"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_seal('main.missing',2)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("SELECT tsfile_seal('main.sensor',0)"));
    EXPECT_EQ(SQLITE_MISUSE,
              exec_raw("SELECT tsfile_compact('main.sensor','bad',20)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("SELECT tsfile_compact('main.sensor',0,20)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("SELECT tsfile_compact('main.sensor',20,10)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("SELECT tsfile_compact('main.sensor',"
                       "9223372036854775807,9223372036854775807)"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_compact('main.missing',1,2)"));
    EXPECT_EQ(SQLITE_MISUSE, exec_raw("SELECT tsfile_export('main.sensor',1)"));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.missing'," +
                                  quote(directory_ + "-guard-missing")));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-guard-parent-missing/child")));
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.sensor'," +
                                  quote(directory_ + "/inside") + ")"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_seal('main.sensor',2) FROM sensor"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_compact('main.sensor',1,2) FROM sensor"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-row-export") + ") FROM sensor"));

    ASSERT_EQ(SQLITE_OK, exec("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    ASSERT_FALSE(path.empty());
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE readonly USING tsfile_hybrid(file=" +
                   quote(path) + ",source_table='sensor')"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("SELECT tsfile_seal('main.readonly',2)"));
    EXPECT_EQ(SQLITE_READONLY,
              exec_raw("SELECT tsfile_compact('main.readonly',1,2)"));
    EXPECT_EQ(1, count("SELECT tsfile_export('main.readonly'," +
                       quote(directory_ + "-readonly-export") + ")"));
}

TEST_F(TsFileSqliteTest, VerifyReportsUnregisteredCorruptAndMismatch) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    ASSERT_FALSE(path.empty());

    const std::string unknown = directory_ + "/unknown.tsfile";
    {
        std::ofstream out(unknown);
        out << "not registered";
    }
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('main.sensor') "
                       "WHERE status='UNREGISTERED'"));
    ASSERT_EQ(0, unlink(unknown.c_str()));

    ASSERT_EQ(SQLITE_OK, exec("CREATE VIRTUAL TABLE other USING tsfile_hybrid("
                              "directory='" +
                              directory_ +
                              "-other',timestamp_precision='ms',"
                              "time TIMESTAMP TIME,device STRING TAG,"
                              "temperature DOUBLE FIELD)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO other VALUES(7,'x',9.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.other',2)"));
    const std::string other_path = find_tsfile(directory_ + "-other");
    ASSERT_FALSE(other_path.empty());
    {
        std::ifstream in(other_path, std::ios::binary);
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out << in.rdbuf();
    }
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('main.sensor') "
                       "WHERE status='MISMATCH'"));

    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out << "corrupt";
    }
    EXPECT_EQ(1, count("SELECT count(*) FROM tsfile_verify('main.sensor') "
                       "WHERE status='CORRUPT'"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT * FROM tsfile_verify('main.missing')"));
}

TEST_F(TsFileSqliteTest, DiagnosticsExposeSourceAndEmptyNoTagDevices) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(1, count("SELECT directory IS NOT NULL FROM "
                       "tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT source_file IS NULL FROM "
                       "tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT source_table IS NULL FROM "
                       "tsfile_table_info('main.sensor')"));

    const std::string path = source_file();
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE source_info USING tsfile_hybrid(file=" +
             quote(path) + ",source_table='sensor')"));
    EXPECT_EQ(path, scalar_text("SELECT source_file FROM "
                                "tsfile_table_info('main.source_info')"));
    EXPECT_EQ("sensor", scalar_text("SELECT source_table FROM "
                                    "tsfile_table_info('main.source_info')"));
    EXPECT_EQ(1, count("SELECT source_max_time=1 FROM "
                       "tsfile_table_info('main.source_info')"));
    EXPECT_EQ("readonly", scalar_text("SELECT mode FROM "
                                      "tsfile_table_info('main.source_info')"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT * FROM tsfile_table_info('main.missing')"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT * FROM tsfile_device_info('main.missing')"));

    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE empty_notags USING tsfile_hybrid("
                   "time TIMESTAMP TIME,value DOUBLE FIELD,directory=" +
                   quote(directory_ + "-empty-notags") +
                   ",timestamp_precision='ms')"));
    EXPECT_EQ(1, count("SELECT device_count FROM "
                       "tsfile_table_info('main.empty_notags')"));
    EXPECT_EQ(1, count("SELECT watermark=-9223372036854775808 FROM "
                       "tsfile_table_info('main.empty_notags')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM "
                       "tsfile_device_info('main.empty_notags')"));
    EXPECT_EQ(0, count("SELECT length(device_key) FROM "
                       "tsfile_device_info('main.empty_notags')"));
    EXPECT_EQ(1, count("SELECT watermark=-9223372036854775808 FROM "
                       "tsfile_device_info('main.empty_notags')"));
    EXPECT_EQ(1, count("SELECT max_time IS NULL FROM "
                       "tsfile_device_info('main.empty_notags')"));
    EXPECT_EQ(0, count("SELECT hot_rows FROM "
                       "tsfile_device_info('main.empty_notags')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO empty_notags VALUES(5,1.5)"));
    EXPECT_EQ(1, count("SELECT max_time=5 FROM "
                       "tsfile_device_info('main.empty_notags')"));
    EXPECT_EQ(1, count("SELECT hot_rows FROM "
                       "tsfile_device_info('main.empty_notags')"));
}

TEST_F(TsFileSqliteTest, CompactThresholdAndChangedFile) {
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE bigrow USING tsfile_hybrid("
                   "time TIMESTAMP TIME,device STRING TAG,payload BLOB FIELD,"
                   "directory=" +
                   quote(directory_ + "-compact-edge") +
                   ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO bigrow VALUES(1,'d0',X'00')"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.bigrow',64)"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO bigrow VALUES(2,'d0',randomblob(1200000))"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.bigrow',64)"));
    const std::string dir = directory_ + "-compact-edge";
    EXPECT_EQ(2, count_tsfiles(dir));
    EXPECT_EQ(0, count("SELECT tsfile_compact('main.bigrow',1,2)"));
    EXPECT_EQ(2, count_tsfiles(dir));
    EXPECT_EQ(2, count("SELECT count(*) FROM bigrow"));

    const auto files = list_tsfiles(dir);
    ASSERT_EQ(2u, files.size());
    {
        std::ofstream out(files[0], std::ios::binary | std::ios::trunc);
        out << "changed";
    }
    EXPECT_NE(SQLITE_OK,
              exec_raw("SELECT tsfile_compact('main.bigrow',10,10)"));
    EXPECT_EQ(2, count_tsfiles(dir));
    EXPECT_EQ(2, count("SELECT file_count FROM "
                       "tsfile_table_info('main.bigrow')"));
}

TEST_F(TsFileSqliteTest, SegmentTimeBoundsAreRecorded) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1000,'d0',1),"
                              "(5000,'d0',2),(2000,'d0',3)"));
    EXPECT_EQ(3, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ("1000,5000,3",
              scalar_text("SELECT min_time || ',' || max_time || ',' || "
                          "row_count FROM \"sensor_tsfile$segments\""));
}

TEST_F(TsFileSqliteTest, ExportRejectsChangedSourceAndDuplicateNames) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    ASSERT_FALSE(path.empty());
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE changed USING tsfile_hybrid(file=" +
                   quote(path) + ",source_table='sensor')"));
    {
        std::ofstream out(path, std::ios::binary | std::ios::trunc);
        out << "changed source";
    }
    const std::string changed_out = directory_ + "-changed-export";
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.changed'," +
                                  quote(changed_out) + ")"));
    EXPECT_NE(0, access(changed_out.c_str(), F_OK));

    const std::string dir_a = directory_ + "-dupA";
    const std::string dir_b = directory_ + "-dupB";
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE dupa USING tsfile_hybrid(directory='" +
                   dir_a +
                   "',timestamp_precision='ms',time TIMESTAMP TIME,"
                   "device STRING TAG,value DOUBLE FIELD)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO dupa VALUES(1,'d0',1.0)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.dupa',2)"));
    const std::string dupa_file = find_tsfile(dir_a);
    ASSERT_FALSE(dupa_file.empty());
    const std::string same_a = dir_a + "/same.tsfile";
    ASSERT_EQ(0, rename(dupa_file.c_str(), same_a.c_str()));
    ASSERT_EQ(0, mkdir(dir_b.c_str(), 0755));
    const std::string same_b = dir_b + "/same.tsfile";
    {
        std::ofstream out(same_b);
        out << "same basename";
    }
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE dupsrc USING tsfile_hybrid(file=" +
                   quote(dir_a) + ",source_table='dupa')"));
    struct stat st {};
    ASSERT_EQ(0, stat(same_b.c_str(), &st));
    ASSERT_EQ(
        SQLITE_OK,
        exec("INSERT INTO \"dupsrc_tsfile$segments\" VALUES(" + quote(same_b) +
             ",0,1,NULL,NULL,'dupa','x'," +
             std::to_string(static_cast<long long>(st.st_size)) + ",0,0)"));
    const std::string dup_out = directory_ + "-duplicate-export";
    EXPECT_NE(SQLITE_OK, exec_raw("SELECT tsfile_export('main.dupsrc'," +
                                  quote(dup_out) + ")"));
    EXPECT_NE(0, access(dup_out.c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, HybridModuleLifecycleAndTagUpdates) {
    EXPECT_NE(SQLITE_OK,
              exec_raw("ALTER TABLE sensor RENAME TO sensor_renamed"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sqlite_master "
                       "WHERE name='sensor'"));

    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(1,'d0',1),(2,'d0',2)"));
    ASSERT_EQ(SQLITE_OK, exec("UPDATE sensor SET device='d1' "
                              "WHERE time=2 AND device='d0'"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor WHERE device='d0'"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor "
                       "WHERE device='d1' AND time=2"));
    EXPECT_EQ(1, count("SELECT hot_rows FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d0'"));
    EXPECT_EQ(1, count("SELECT hot_rows FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d1'"));
    EXPECT_EQ(1,
              count("SELECT max_time=2 FROM tsfile_device_info('main.sensor') "
                    "WHERE device_key='d1'"));

    ASSERT_EQ(SQLITE_OK,
              exec("DELETE FROM sensor WHERE time=2 AND device='d1'"));
    EXPECT_EQ(0, count("SELECT hot_rows FROM tsfile_device_info('main.sensor') "
                       "WHERE device_key='d1'"));
    EXPECT_EQ(SQLITE_OK,
              exec_raw("INSERT OR IGNORE INTO sensor VALUES(1,'d0',9)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor "
                       "WHERE device='d0' AND time=1"));
    EXPECT_NE(SQLITE_OK,
              exec_raw("INSERT OR FAIL INTO sensor VALUES(1,'d0',9)"));

    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    ASSERT_FALSE(path.empty());
    ASSERT_EQ(SQLITE_OK, exec("DROP TABLE sensor"));
    EXPECT_EQ(0, count("SELECT count(*) FROM sqlite_master "
                       "WHERE name='sensor'"));
    EXPECT_EQ(0, count("SELECT count(*) FROM sqlite_master "
                       "WHERE name LIKE 'sensor_tsfile$%'"));
    EXPECT_EQ(0, access(path.c_str(), F_OK));
    EXPECT_EQ(0, access((directory_ + "/.tsfile-owner").c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, ExportCountsEverySealedFile) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string output = directory_ + "-multi-export";
    EXPECT_EQ(
        2, count("SELECT tsfile_export('main.sensor'," + quote(output) + ")"));
    EXPECT_EQ(2, count_tsfiles(output));
}

TEST_F(TsFileSqliteTest, AllFieldTypesRoundTripAtExtremes) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE alltypes USING tsfile_hybrid("
             "time TIMESTAMP TIME,device STRING TAG,"
             "b BOOLEAN FIELD,i32 INT32 FIELD,i64 INT64 FIELD,"
             "f FLOAT FIELD,d DOUBLE FIELD,txt TEXT FIELD,s STRING FIELD,"
             "payload BLOB FIELD,day DATE FIELD,stamp TIMESTAMP FIELD,"
             "directory=" +
             quote(directory_ + "-alltypes") + ",timestamp_precision='ms')"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("INSERT INTO alltypes VALUES("
             "1,'d0',1,2147483647,9223372036854775807,1.5,2.25,'text','s',"
             "X'00FF',2147483647,9223372036854775807),"
             "(2,'d0',0,-2147483648,-9223372036854775808,-1.5,-2.25,'','',"
             "X'',-2147483648,-9223372036854775808),"
             "(3,'d0',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"));
    EXPECT_EQ(3, count("SELECT tsfile_seal('main.alltypes',64)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM alltypes"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE b=1"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE i32=2147483647"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE i32=-2147483648"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes "
                       "WHERE i64=9223372036854775807"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes "
                       "WHERE i64=-9223372036854775808"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE f=1.5"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE d=-2.25"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE txt='text'"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE txt=''"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE s=''"));
    EXPECT_EQ(1,
              count("SELECT count(*) FROM alltypes WHERE hex(payload)='00FF'"));
    EXPECT_EQ(1,
              count("SELECT count(*) FROM alltypes WHERE length(payload)=0"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE day=2147483647"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes "
                       "WHERE stamp=-9223372036854775808"));
    EXPECT_EQ(1, count("SELECT count(*) FROM alltypes WHERE time=3 AND "
                       "b IS NULL AND i32 IS NULL AND i64 IS NULL AND "
                       "f IS NULL AND d IS NULL AND txt IS NULL AND "
                       "s IS NULL AND payload IS NULL AND day IS NULL AND "
                       "stamp IS NULL"));
    EXPECT_EQ(12, count("SELECT count(*) FROM tsfile_schema('main.alltypes')"));
}

TEST_F(TsFileSqliteTest, TimeBoundaryQueriesAndWatermarks) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES"
                              "(9223372036854775807,'max',1.0),"
                              "(-9223372036854775808,'min',2.0),"
                              "(-1,'neg',3.0),(0,'zero',4.0),(1,'one',5.0)"));
    EXPECT_EQ(5, count("SELECT tsfile_seal('main.sensor',64)"));
    EXPECT_EQ(5, count("SELECT count(*) FROM sensor"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor "
                       "WHERE time=9223372036854775807"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor "
                       "WHERE time=-9223372036854775808"));
    EXPECT_EQ(2,
              count("SELECT count(*) FROM sensor WHERE time>=0 AND time<=1"));
    EXPECT_EQ(0, count("SELECT count(*) FROM sensor WHERE time>0 AND time<1"));
    EXPECT_EQ(2, count("SELECT count(*) FROM sensor WHERE time<0"));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor WHERE time BETWEEN "
                       "-9223372036854775808 AND 0"));
    EXPECT_EQ(1, count("SELECT watermark IS NULL FROM "
                       "tsfile_device_info('main.sensor') "
                       "WHERE device_key='max'"));
    EXPECT_EQ(1, count("SELECT watermark=-9223372036854775807 FROM "
                       "tsfile_device_info('main.sensor') "
                       "WHERE device_key='min'"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO sensor VALUES(0,'max',9.0)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO sensor VALUES(-9223372036854775808,"
                       "'min',9.0)"));
    ASSERT_EQ(
        SQLITE_OK,
        exec("INSERT INTO sensor VALUES(-9223372036854775807,'min',9.0)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor "
                       "WHERE device='min' AND time=-9223372036854775807"));
}

TEST_F(TsFileSqliteTest, QueryOperatorAndResidualMatrix) {
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE ops USING tsfile_hybrid("
                   "time TIMESTAMP TIME,device STRING TAG,"
                   "temperature DOUBLE FIELD,label TEXT FIELD,directory=" +
                   quote(directory_ + "-ops") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO ops VALUES"
                              "(1,'d0',1.5,'alpha'),(2,'d0',2.5,'beta'),"
                              "(3,'d0',3.5,'gamma'),(4,'d1',4.5,'delta'),"
                              "(5,'d1',NULL,NULL),(6,'d1',6.5,'epsilon')"));
    EXPECT_EQ(6, count("SELECT tsfile_seal('main.ops',64)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM ops WHERE temperature=2.5"));
    EXPECT_EQ(4, count("SELECT count(*) FROM ops WHERE temperature<>2.5"));
    EXPECT_EQ(2, count("SELECT count(*) FROM ops WHERE temperature>3 AND "
                       "temperature<6"));
    EXPECT_EQ(2, count("SELECT count(*) FROM ops WHERE temperature BETWEEN 2 "
                       "AND 4"));
    EXPECT_EQ(2, count("SELECT count(*) FROM ops WHERE temperature IN "
                       "(1.5,6.5)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM ops WHERE temperature IS NULL"));
    EXPECT_EQ(5,
              count("SELECT count(*) FROM ops WHERE temperature IS NOT NULL"));
    EXPECT_EQ(2, count("SELECT count(*) FROM ops WHERE NOT (temperature>3)"));
    EXPECT_EQ(4, count("SELECT count(*) FROM ops WHERE device='d0' OR "
                       "temperature>5"));
    EXPECT_EQ(1, count("SELECT count(*) FROM ops WHERE label LIKE 'a%'"));
    EXPECT_EQ(2, count("SELECT count(*) FROM ops WHERE label LIKE '%ta'"));
    EXPECT_EQ(1, count("SELECT count(*) FROM ops WHERE label GLOB 'a*'"));
    EXPECT_EQ("6,5", scalar_text("SELECT group_concat(time,',') FROM "
                                 "(SELECT time FROM ops ORDER BY time DESC "
                                 "LIMIT 2)"));
    EXPECT_EQ("4,3", scalar_text("SELECT group_concat(time,',') FROM "
                                 "(SELECT time FROM ops ORDER BY time DESC "
                                 "LIMIT 2 OFFSET 2)"));
    EXPECT_EQ(2, count("SELECT count(*) FROM (SELECT device,count(*) AS cnt "
                       "FROM ops GROUP BY device HAVING cnt=3)"));
}

TEST_F(TsFileSqliteTest, MultiTagNullEmptyCombinations) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE multitag USING tsfile_hybrid("
             "time TIMESTAMP TIME,t1 STRING TAG,t2 STRING TAG,"
             "value DOUBLE FIELD,directory=" +
             quote(directory_ + "-multitag") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO multitag VALUES"
                   "(1,NULL,NULL,1.0),(1,'',NULL,2.0),(1,NULL,'',3.0),"
                   "(1,'','',4.0),(1,'a','b',5.0)"));
    EXPECT_EQ(5, count("SELECT tsfile_seal('main.multitag',64)"));
    EXPECT_EQ(5, count("SELECT count(*) FROM multitag"));
    EXPECT_EQ(1, count("SELECT count(*) FROM multitag "
                       "WHERE t1 IS NULL AND t2 IS NULL"));
    EXPECT_EQ(1, count("SELECT count(*) FROM multitag "
                       "WHERE t1='' AND t2 IS NULL"));
    EXPECT_EQ(1, count("SELECT count(*) FROM multitag "
                       "WHERE t1 IS NULL AND t2=''"));
    EXPECT_EQ(1, count("SELECT count(*) FROM multitag "
                       "WHERE t1='' AND t2=''"));
    EXPECT_EQ(1, count("SELECT count(*) FROM multitag "
                       "WHERE t1='a' AND t2='b'"));
    EXPECT_EQ(5, count("SELECT count(*) FROM "
                       "tsfile_device_info('main.multitag')"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("INSERT INTO multitag VALUES(1,NULL,NULL,9.0)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO multitag VALUES(2,NULL,NULL,9.0)"));
    EXPECT_EQ(1, count("SELECT hot_rows FROM "
                       "tsfile_device_info('main.multitag') "
                       "WHERE device_key='NULL,NULL'"));
}

TEST_F(TsFileSqliteTest, CompactCanSplitIntoMultipleOutputFiles) {
    ASSERT_EQ(
        SQLITE_OK,
        exec("CREATE VIRTUAL TABLE chunks USING tsfile_hybrid("
             "time TIMESTAMP TIME,device STRING TAG,payload BLOB FIELD,"
             "directory=" +
             quote(directory_ + "-chunks") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO chunks VALUES(1,'d0',randomblob(600000))"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.chunks',64)"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO chunks VALUES(2,'d0',randomblob(600000))"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.chunks',64)"));
    const std::string dir = directory_ + "-chunks";
    EXPECT_EQ(2, count_tsfiles(dir));
    EXPECT_EQ(2, count("SELECT tsfile_compact('main.chunks',1,1)"));
    EXPECT_EQ(2, count_tsfiles(dir));
    EXPECT_EQ(2, count("SELECT file_count FROM "
                       "tsfile_table_info('main.chunks')"));
    EXPECT_EQ(2, count("SELECT count(*) FROM chunks"));
    EXPECT_EQ(2, count("SELECT count(*) FROM tsfile_verify('main.chunks') "
                       "WHERE status='OK'"));
}

TEST_F(TsFileSqliteTest, TempTableAndQuotedReservedIdentifiers) {
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE temp.tempsensor USING tsfile_hybrid("
                   "time TIMESTAMP TIME,device STRING TAG,value DOUBLE FIELD,"
                   "directory=" +
                   quote(directory_ + "-temp") + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO temp.tempsensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('temp.tempsensor',2)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM temp.tempsensor"));
    EXPECT_EQ("writable", scalar_text("SELECT mode FROM "
                                      "tsfile_table_info('temp.tempsensor')"));
    ASSERT_EQ(SQLITE_OK, exec("DROP TABLE temp.tempsensor"));

    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE \"weird table\" USING tsfile_hybrid("
                   "\"select\" TIMESTAMP TIME,\"group\" STRING TAG,"
                   "\"order\" DOUBLE FIELD,directory=" +
                   quote(directory_ + "-quoted-reserved") +
                   ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO \"weird table\" VALUES(1,'g',2.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.\"weird table\"',2)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM \"weird table\" "
                       "WHERE \"select\"=1 AND \"group\"='g' AND "
                       "\"order\"=2.5"));
    EXPECT_EQ("select:0:INTEGER:TIME",
              scalar_text("SELECT name || ':' || cid || ':' || sqlite_type || "
                          "':' || category FROM "
                          "tsfile_schema('main.\"weird table\"') "
                          "WHERE name='select'"));
}

TEST_F(TsFileSqliteTest, TreeModelSourceIsRejectedClearly) {
    const std::string fixture = std::string(TSFILE_CPP_SOURCE_DIR) +
                                "/test/tools/fixtures/complex_tree.tsfile";
    const std::string sql =
        "CREATE VIRTUAL TABLE tree USING tsfile_hybrid(file=" + quote(fixture) +
        ",source_table='root',directory=" + quote(directory_ + "-tree") +
        ",timestamp_precision='ms')";
    char* error = nullptr;
    int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error);
    EXPECT_NE(SQLITE_OK, rc);
    std::string message = error == nullptr ? "" : error;
    sqlite3_free(error);
    EXPECT_NE(std::string::npos, message.find("tree model")) << message;
    EXPECT_NE(std::string::npos, message.find("not supported")) << message;
}

TEST_F(TsFileSqliteTest, CorruptSourceFileReportsTsfileErrorCode) {
    const std::string path = directory_ + "-corrupt.tsfile";
    {
        std::ofstream out(path);
        out << "not a tsfile";
    }
    const std::string sql =
        "CREATE VIRTUAL TABLE corrupt USING tsfile_hybrid(file=" + quote(path) +
        ",source_table='sensor',directory=" + quote(directory_ + "-corrupt") +
        ",timestamp_precision='ms')";
    char* error = nullptr;
    int rc = sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &error);
    EXPECT_NE(SQLITE_OK, rc);
    std::string message = error == nullptr ? "" : error;
    sqlite3_free(error);
    EXPECT_NE(std::string::npos, message.find("tsfile E_")) << message;
    EXPECT_NE(std::string::npos, message.find('=')) << message;
}

TEST_F(TsFileSqliteTest, QueryReportsDeletedSealedFile) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string path = source_file();
    ASSERT_FALSE(path.empty());
    ASSERT_EQ(0, unlink(path.c_str()));

    sqlite3_stmt* stmt = nullptr;
    ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db_, "SELECT count(*) FROM sensor",
                                            -1, &stmt, nullptr));
    int rc = sqlite3_step(stmt);
    std::string message = sqlite3_errmsg(db_);
    sqlite3_finalize(stmt);
    EXPECT_NE(SQLITE_ROW, rc);
    EXPECT_NE(std::string::npos, message.find("missing or changed file"))
        << message;
}

TEST_F(TsFileSqliteTest, OrphanTempFileIsKeptAfterSeal) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string stale = directory_ + "/tsfilesensor-99999999-0.tmp";
    {
        std::ofstream out(stale);
        out << "stale";
    }
    ASSERT_EQ(0, access(stale.c_str(), F_OK));

    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(0, access(stale.c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, OrphanTempFileIsKeptOnReconnect) {
    const std::string db_path = directory_ + "-temp-clean.db";
    const std::string table_dir = directory_ + "-temp-clean";
    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(db_path) + " AS persisted"));
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE persisted.clean USING tsfile_hybrid("
                   "time TIMESTAMP TIME,device STRING TAG,temperature DOUBLE "
                   "FIELD,directory=" +
                   quote(table_dir) + ",timestamp_precision='ms')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO persisted.clean VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('persisted.clean',2)"));
    ASSERT_EQ(SQLITE_OK, exec("DETACH persisted"));

    const std::string stale = table_dir + "/tsfileclean-99999999-0.tmp";
    {
        std::ofstream out(stale);
        out << "stale";
    }
    ASSERT_EQ(0, access(stale.c_str(), F_OK));

    ASSERT_EQ(SQLITE_OK, exec("ATTACH " + quote(db_path) + " AS persisted"));
    EXPECT_EQ(1, count("SELECT count(*) FROM persisted.clean"));
    EXPECT_EQ(0, access(stale.c_str(), F_OK));
    ASSERT_EQ(SQLITE_OK, exec("DETACH persisted"));
}

TEST_F(TsFileSqliteTest, OrphanTempFileIsCleanedAfterCompact) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(2,'d0',2.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string stale = directory_ + "/tsfilesensor-99999999-0.tmp";
    {
        std::ofstream out(stale);
        out << "stale";
    }
    ASSERT_EQ(0, access(stale.c_str(), F_OK));

    EXPECT_EQ(1, count("SELECT tsfile_compact('main.sensor',10,20)"));
    EXPECT_NE(0, access(stale.c_str(), F_OK));
}

TEST_F(TsFileSqliteTest, OrphanTempFileIsCleanedAfterExport) {
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO sensor VALUES(1,'d0',1.5)"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    const std::string stale = directory_ + "/tsfilesensor-99999999-0.tmp";
    {
        std::ofstream out(stale);
        out << "stale";
    }
    ASSERT_EQ(0, access(stale.c_str(), F_OK));

    EXPECT_EQ(1, count("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-export-clean") + ")"));
    EXPECT_NE(0, access(stale.c_str(), F_OK));
}

}  // namespace
