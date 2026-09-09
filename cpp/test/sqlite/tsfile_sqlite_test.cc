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
              exec_raw("INSERT INTO continuation VALUES(2,'different',9)"));
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
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',2)"));
    EXPECT_EQ(3, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(SQLITE_OK, exec("SAVEPOINT outer_point"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',3)"));
    ASSERT_EQ(SQLITE_OK, exec("SAVEPOINT inner_point"));
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.sensor',4)"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK TO outer_point"));
    EXPECT_EQ(2,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(2,
              count("SELECT watermark FROM tsfile_table_info('main.sensor')"));
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

TEST_F(TsFileSqliteTest, ExportAutomaticallySealsAndRoundTrips) {
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor VALUES(1,NULL,1.5),(2,'d0',2.5)"));
    const std::string output = directory_ + "-export";
    EXPECT_EQ(
        1, count("SELECT tsfile_export('main.sensor'," + quote(output) + ")"));
    EXPECT_EQ(0,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(3,
              count("SELECT watermark FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(2, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE exported USING tsfile_hybrid(file=" +
                   quote(output + "/part-000001.tsfile") +
                   ",source_table='sensor')"));
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
    EXPECT_EQ(1, count("SELECT tsfile_export('main.sensor'," +
                       quote(directory_ + "-max") + ")"));
    EXPECT_EQ(
        0,
        count("SELECT append_available FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(
        1,
        count(
            "SELECT watermark IS NULL FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor"));
    EXPECT_EQ(
        SQLITE_CONSTRAINT,
        exec_raw("INSERT INTO sensor VALUES(9223372036854775807,'new',2)"));
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
    EXPECT_EQ(1, count("SELECT tsfile_export('main.renamed'," +
                       quote(directory_ + "-selected-export") + ")"));
    storage::TsFileReader reader;
    ASSERT_EQ(common::E_OK,
              reader.open(directory_ + "-selected-export/part-000001.tsfile"));
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
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.empty_source',0)"));
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
    EXPECT_EQ(1, count("SELECT tsfile_seal('persisted.data',2)"));
    sqlite3* other = nullptr;
    ASSERT_EQ(SQLITE_OK, sqlite3_open(dbpath.c_str(), &other));
    sqlite3_enable_load_extension(other, 1);
    ASSERT_EQ(SQLITE_OK,
              sqlite3_load_extension(other, TSFILE_SQLITE_EXTENSION_PATH,
                                     nullptr, nullptr));
    ASSERT_EQ(SQLITE_OK, sqlite3_exec(other, "SELECT * FROM data", nullptr,
                                      nullptr, nullptr));
    EXPECT_EQ(1, count("SELECT tsfile_seal('persisted.data',3)"));
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
    EXPECT_EQ(0,
              count("SELECT hot_rows FROM tsfile_table_info('main.sensor')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM sensor"));
    EXPECT_NE(0, access(output.c_str(), F_OK));
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
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.notags',2)"));
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
    EXPECT_EQ(1, count("SELECT tsfile_seal('main.notags',4)"));
    EXPECT_EQ(0, count("SELECT tsfile_seal('main.notags',5)"));
    EXPECT_EQ(5,
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
    EXPECT_EQ(1, count("SELECT tsfile_export('main.\"odd.table\"'," +
                       quote(directory_ + "-quoted-output") + ")"));
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE inferred USING tsfile_hybrid(file=" +
                   quote(directory_ + "-quoted-output/part-000001.tsfile") +
                   ",source_table='odd.table')"));
    EXPECT_EQ(1, count("SELECT count(*) FROM inferred WHERE \"event time\"=1 "
                       "AND \"sensor value\"=3.5"));
    EXPECT_NE(
        SQLITE_OK,
        exec_raw("CREATE VIRTUAL TABLE invalid USING tsfile_hybrid(file=" +
                 quote(directory_ + "-quoted-output/part-000001.tsfile") +
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

}  // namespace
