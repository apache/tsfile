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

#include <malloc.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "c_examples.h"

// This example shows you how to write tsfile.
ERRNO write_tsfile() {
    ERRNO code = 0;
    char* table_name = "table1";

    // Create table schema to describe a table in a tsfile.
    TableSchema table_schema;
    table_schema.table_name = strdup(table_name);
    table_schema.column_num = 3;
    table_schema.column_schemas =
        (ColumnSchema*)malloc(sizeof(ColumnSchema) * 3);
    table_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("id1"),
                     .data_type = TS_DATATYPE_STRING,
                     .compression = TS_COMPRESSION_UNCOMPRESSED,
                     .encoding = TS_ENCODING_PLAIN,
                     .column_category = TAG};
    table_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("id2"),
                     .data_type = TS_DATATYPE_STRING,
                     .compression = TS_COMPRESSION_UNCOMPRESSED,
                     .encoding = TS_ENCODING_PLAIN,
                     .column_category = TAG};
    table_schema.column_schemas[2] =
        (ColumnSchema){.column_name = strdup("s1"),
                     .data_type = TS_DATATYPE_INT32,
                     .compression = TS_COMPRESSION_UNCOMPRESSED,
                     .encoding = TS_ENCODING_PLAIN,
                     .column_category = FIELD};

    // Create a file with specify path to write tsfile.
    WriteFile file = write_file_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);

    // Create tsfile writer with specify table schema.
    TsFileWriter writer = tsfile_writer_new(file, &table_schema, &code);
    HANDLE_ERROR(code);

    // Create tablet to insert data.
    Tablet tablet =
        tablet_new((char*[]){"id1", "id2", "s1"},
                   (TSDataType[]){TS_DATATYPE_STRING, TS_DATATYPE_STRING,
                                  TS_DATATYPE_INT32},
                   3, 5);

    for (int row = 0; row < 5; row++) {
        Timestamp timestamp = row;
        tablet_add_timestamp(tablet, row, timestamp);
        tablet_add_value_by_name_string(tablet, row, "id1", "id_field_1");
        tablet_add_value_by_name_string(tablet, row, "id2", "id_field_2");
        tablet_add_value_by_name_int32_t(tablet, row, "s1", row);
    }

    // Write tablet data.
    HANDLE_ERROR(tsfile_writer_write(writer, tablet));

    // Free tablet.
    free_tablet(&tablet);

    // Free table schema we used before.
    free_table_schema(table_schema);

    // Close writer.
    HANDLE_ERROR(tsfile_writer_close(writer));

    // Close write file after closing writer.
    free_write_file(&file);

    return 0;
}