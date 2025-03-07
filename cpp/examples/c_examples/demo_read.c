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

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#include "c_examples.h"

// This example shows you how to read tsfile.
ERRNO read_tsfile() {

    ERRNO code = 0;
    char* table_name = "table1";

    // Create tsfile reader with specify tsfile's path
    TsFileReader reader = tsfile_reader_new("test_c.tsfile", &code);
    HANDLE_ERROR(code);

    ResultSet ret = tsfile_query_table(
        reader, table_name, (char*[]){"id1", "id2", "s1"}, 3, 0, 10, &code);
    HANDLE_ERROR(code);

    if (ret == NULL) {
        HANDLE_ERROR(RET_INVALID_QUERY);
    }

    // Get query result metadata: column name and datatype
    ResultSetMetaData metadata = tsfile_result_set_get_metadata(ret);
    int column_num = tsfile_result_set_metadata_get_column_num(metadata);

    for (int i = 1; i <= column_num; i++) {
        printf("column:%s, datatype:%d\n", tsfile_result_set_metadata_get_column_name(metadata, i),
               tsfile_result_set_metadata_get_data_type(metadata, i));
    }

    // Get data by column name or index.
    while (tsfile_result_set_next(ret, &code) && code == RET_OK) {
        // Timestamp at column 1 and column index begin from 1.
        Timestamp timestamp =
            tsfile_result_set_get_value_by_index_int64_t(ret, 1);
        printf("%ld\n", timestamp);
        for (int i = 1; i <= column_num; i++) {
            if (tsfile_result_set_is_null_by_index(ret, i)) {
                printf(" null ");
            } else {
                switch (tsfile_result_set_metadata_get_data_type(metadata, i)) {
                    case TS_DATATYPE_BOOLEAN:
                        printf("%d\n", tsfile_result_set_get_value_by_index_bool(
                                         ret, i));
                        break;
                    case TS_DATATYPE_INT32:
                        printf("%d\n",
                               tsfile_result_set_get_value_by_index_int32_t(ret,
                                                                            i));
                        break;
                    case TS_DATATYPE_INT64:
                        printf("%ld\n",
                               tsfile_result_set_get_value_by_index_int64_t(ret,
                                                                            i));
                        break;
                    case TS_DATATYPE_FLOAT:
                        printf("%f\n", tsfile_result_set_get_value_by_index_float(
                                         ret, i));
                        break;
                    case TS_DATATYPE_DOUBLE:
                        printf("%lf\n",
                               tsfile_result_set_get_value_by_index_double(ret,
                                                                           i));
                        break;
                    case TS_DATATYPE_STRING:
                        printf("%s\n",
                               tsfile_result_set_get_value_by_index_string(ret,
                                                                           i));
                        break;
                    default:
                        printf("unknown_type");
                        break;
                }
            }
        }
    }

    // Free query meta data
    free_result_set_meta_data(metadata);

    // Free query handler.
    free_tsfile_result_set(&ret);

    // Close tsfile reader.
    tsfile_reader_close(reader);

    return 0;
}
