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

#include <gtest/gtest.h>
#include <sqlite3.h>
#include <unistd.h>

#include <cstdio>
#include <string>

namespace {

class TsFileSqliteTest : public ::testing::Test {
   protected:
    void SetUp() override {
        directory_ = "/tmp/tsfile-sqlite-test-" +
                     std::to_string(static_cast<long long>(getpid()));
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
                       "column='time:TIMESTAMP:TIME',"
                       "column='device:STRING:TAG',"
                       "column='temperature:DOUBLE:FIELD')"));
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
        if (stmt != nullptr && sqlite3_step(stmt) == SQLITE_ROW) {
            result = sqlite3_column_int(stmt, 0);
        }
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
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor(_tsfile_command,_tsfile_cutoff) "
                   "VALUES('seal',2)"));
    ASSERT_EQ(SQLITE_OK, exec("ROLLBACK"));
    ASSERT_EQ(2, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(0, count("SELECT count(*) FROM sensor_segments"));

    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO sensor(_tsfile_command,_tsfile_cutoff) "
                   "VALUES('seal',2)"));
    ASSERT_EQ(2, count("SELECT count(*) FROM sensor"));
    ASSERT_EQ(1, count("SELECT count(*) FROM sensor_segments"));
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
                              "/ints',timestamp_precision='ms',"
                              "column='time:TIMESTAMP:TIME',"
                              "column='device:STRING:TAG',"
                              "column='reading:INT32:FIELD')"));
    EXPECT_EQ(SQLITE_MISMATCH,
              exec_raw("INSERT INTO ints VALUES(1,'d0',2147483648)"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO ints VALUES(1,'d0',42)"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO ints(_tsfile_command,_tsfile_cutoff) "
                   "VALUES('seal',2)"));
    EXPECT_EQ(SQLITE_CONSTRAINT,
              exec_raw("UPDATE ints SET reading=43 WHERE time=1"));
    EXPECT_EQ(SQLITE_CONSTRAINT, exec_raw("DELETE FROM ints WHERE time=1"));
    EXPECT_EQ(1, count("SELECT count(*) FROM ints "
                       "WHERE time=1 AND reading=42"));
}

TEST_F(TsFileSqliteTest, NullAndBlobSurviveSeal) {
    ASSERT_EQ(SQLITE_OK,
              exec("CREATE VIRTUAL TABLE payload USING tsfile_hybrid("
                   "directory='" +
                   directory_ +
                   "/payload',timestamp_precision='ms',"
                   "column='time:TIMESTAMP:TIME',"
                   "column='device:STRING:TAG',"
                   "column='payload:BLOB:FIELD',"
                   "column='note:TEXT:FIELD')"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO payload VALUES(1,'d0',X'000102','')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO payload VALUES(2,'d0',NULL,NULL)"));
    ASSERT_EQ(SQLITE_OK,
              exec("INSERT INTO payload(_tsfile_command,_tsfile_cutoff) "
                   "VALUES('seal',3)"));
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
                   "/attached',timestamp_precision='ms',"
                   "column='time:TIMESTAMP:TIME',"
                   "column='device:STRING:TAG',"
                   "column='reading:INT32:FIELD')"));
    ASSERT_EQ(SQLITE_OK, exec("INSERT INTO aux.attached VALUES(1,'d0',7)"));
    EXPECT_EQ(1, count("SELECT count(*) FROM aux.attached_data"));
    EXPECT_EQ(0, count("SELECT count(*) FROM main.sqlite_master "
                       "WHERE name='attached_data'"));
}

}  // namespace
