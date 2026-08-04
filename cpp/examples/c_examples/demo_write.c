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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "c_examples.h"

static char* duplicate_string(const char* src) {
    size_t len = strlen(src) + 1;
    char* dst = (char*)malloc(len);
    if (dst != NULL) {
        memcpy(dst, src, len);
    }
    return dst;
}

// This example shows you how to write tsfile.
ERRNO write_tsfile() {
    ERRNO code = 0;
    WriteFile file = NULL;
    TsFileWriter writer = NULL;
    Tablet tablet = NULL;
    TableSchema table_schema = {0};

    code = set_global_compression(TS_COMPRESSION_LZ4);
    if (code != RET_OK) {
        goto cleanup;
    }
    code = set_datatype_encoding(TS_DATATYPE_INT32, TS_ENCODING_TS_2DIFF);
    if (code != RET_OK) {
        goto cleanup;
    }
    char* table_name = "table1";

    // Create table schema to describe a table in a tsfile.
    table_schema.table_name = duplicate_string(table_name);
    table_schema.column_schemas =
        (ColumnSchema*)calloc(3, sizeof(ColumnSchema));
    if (table_schema.table_name == NULL ||
        table_schema.column_schemas == NULL) {
        code = RET_OOM;
        goto cleanup;
    }
    table_schema.column_num = 3;
    table_schema.column_schemas[0] =
        (ColumnSchema){.column_name = duplicate_string("id1"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    table_schema.column_schemas[1] =
        (ColumnSchema){.column_name = duplicate_string("id2"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    table_schema.column_schemas[2] =
        (ColumnSchema){.column_name = duplicate_string("s1"),
                       .data_type = TS_DATATYPE_INT32,
                       .column_category = FIELD};
    if (table_schema.column_schemas[0].column_name == NULL ||
        table_schema.column_schemas[1].column_name == NULL ||
        table_schema.column_schemas[2].column_name == NULL) {
        code = RET_OOM;
        goto cleanup;
    }

    remove("test_c.tsfile");
    // Create a file with specify path to write tsfile.
    file = write_file_new("test_c.tsfile", &code);
    if (code != RET_OK) {
        goto cleanup;
    }

    // Create tsfile writer with specify table schema.
    writer = tsfile_writer_new(file, &table_schema, &code);
    if (code != RET_OK) {
        goto cleanup;
    }

    // Create tablet to insert data.
    tablet = tablet_new((char*[]){"id1", "id2", "s1"},
                        (TSDataType[]){TS_DATATYPE_STRING, TS_DATATYPE_STRING,
                                       TS_DATATYPE_INT32},
                        3, 5);

    for (int row = 0; row < 5; row++) {
        Timestamp timestamp = row;
        tablet_add_timestamp(tablet, row, timestamp);
        tablet_add_value_by_name_string_with_len(
            tablet, row, "id1", "id_field_1", strlen("id_field_1"));
        tablet_add_value_by_name_string_with_len(
            tablet, row, "id2", "id_field_2", strlen("id_field_2"));
        tablet_add_value_by_name_int32_t(tablet, row, "s1", row);
    }

    // Write tablet data.
    code = tsfile_writer_write(writer, tablet);

cleanup:
    if (tablet != NULL) {
        free_tablet(&tablet);
    }

    if (table_schema.table_name != NULL ||
        table_schema.column_schemas != NULL) {
        free_table_schema(table_schema);
    }

    if (writer != NULL) {
        ERRNO close_code = tsfile_writer_close(writer);
        if (code == RET_OK) {
            code = close_code;
        }
    }

    if (file != NULL) {
        free_write_file(&file);
    }

    if (code != RET_OK) {
        printf("get err no: %d", code);
    }
    return code;
}
