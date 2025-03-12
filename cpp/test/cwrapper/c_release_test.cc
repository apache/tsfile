/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <unistd.h>
#include <utils/db_utils.h>
extern "C" {
#include "cwrapper/errno_define_c.h"
#include "cwrapper/tsfile_cwrapper.h"
}

#include "common/tablet.h"
#include "utils/errno_define.h"
namespace CReleaseTest {
class CReleaseTest : public testing::Test {};

TEST_F(CReleaseTest, TestCreateFile) {
    ERRNO error_no = RET_OK;
    // Create File and Get RET_OK
    WriteFile file = write_file_new("create_file1.tsfile", &error_no);
    ASSERT_EQ(RET_OK, error_no);
    free_write_file(&file);

    // Already exists
    file = write_file_new("create_file1.tsfile", &error_no);
    ASSERT_EQ(RET_ALREADY_EXIST, error_no);
    ASSERT_EQ(nullptr, file);

    // Folder
    file = write_file_new("test/", &error_no);
    ASSERT_EQ(RET_FILRET_OPEN_ERR, error_no);

    remove("create_file1.tsfile");
}

TEST_F(CReleaseTest, TsFileWriterNew) {
    ERRNO error_code = RET_OK;

    remove("test.tsfile");
    TableSchema test_schema;
    test_schema.table_name = strdup("test_table");
    test_schema.column_num = 0;

    WriteFile file = write_file_new("test.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    // Invalid schema
    TsFileWriter writer = tsfile_writer_new(file, &test_schema, &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    ASSERT_EQ(nullptr, writer);

    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
    ASSERT_EQ(nullptr, file);
    remove("test.tsfile");
    file = write_file_new("test.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);
    // Invalid schema
    writer = tsfile_writer_new_with_memory_threshold(file, &test_schema, 100,
                                                     &error_code);
    ASSERT_EQ(RET_INVALID_SCHEMA, error_code);
    ASSERT_EQ(nullptr, writer);

    ASSERT_EQ(RET_OK, tsfile_writer_close(writer));
    free_write_file(&file);
    ASSERT_EQ(nullptr, file);
    remove("test.tsfile");
    file = write_file_new("test.tsfile", &error_code);
    ASSERT_EQ(RET_OK, error_code);

    TableSchema tableSchema;
    tableSchema.table_name = strdup("test_table");
    tableSchema.column_num = 2;
    tableSchema.column_schemas = static_cast<ColumnSchema *>(malloc(sizeof(ColumnSchema) * 2));
    tableSchema.column_schemas[0] = (ColumnSchema) {
        .column_name = strdup("col1"),
        .data_type = TS_DATATYPE_BOOLEAN,
        .column_category = TAG
    };


}

}  // namespace CReleaseTest