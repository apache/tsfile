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

#include <utils/errno_define.h>

#include <chrono>
#include <iostream>
#include <string>
#include <fstream>

#include <string.h>

#include "bench_conf.h"
#include "bench_mark_c_cpp.h"
#include "bench_mark_utils.h"
#include "cwrapper/tsfile_cwrapper.h"
#define HANDLE_ERROR(err_no)                  \
    do {                                      \
        if (err_no != 0) {                    \
            printf("get err no: %d", err_no); \
            return err_no;                    \
        }                                     \
    } while (0)

char** column_list;
TSDataType* data_types_c;
int column_num_c = 0;

int bench_mark_c_write() {
    ERRNO code = 0;
    char* table_name = "TestTable";
    print_config(false);
    TableSchema table_schema;


    table_schema.table_name = strdup(table_name);
    int column = 0;
    for (auto data_type : bench::field_type_vector) {
        column += data_type;
    }
    column_list = new char*[column + 2];
    data_types_c = new TSDataType[column + 2];
    column_num_c = column;

    std::ofstream csv_file("memory_usage_c.csv");
    if (!csv_file.is_open()) {
        std::cout << "csv create failed!" << std::endl;
        return 0;
    }
    csv_file << "iter_num,memory_usage(kb)\n";
    int iter_num = 0;
    csv_file << iter_num++ <<","<< get_memory_usage() << "\n";

    table_schema.column_schemas =
        (ColumnSchema*)malloc(sizeof(ColumnSchema) * (2 + column));
    table_schema.column_num = column + 2;
    table_schema.column_schemas[0] =
        (ColumnSchema){.column_name = strdup("TAG1"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    column_list[0] = strdup("TAG1");
    data_types_c[0] = TS_DATATYPE_STRING;
    table_schema.column_schemas[1] =
        (ColumnSchema){.column_name = strdup("TAG2"),
                       .data_type = TS_DATATYPE_STRING,
                       .column_category = TAG};
    column_list[1] = strdup("TAG2");
    data_types_c[1] = TS_DATATYPE_STRING;

    int col = 2;
    for (int i = 0; i < bench::field_type_vector.size(); i++) {
        int column_num = bench::field_type_vector[i];
        for (int j = 0; j < column_num; j++) {
            column_list[col] =
                strdup(std::string("FIELD" + std::to_string(i)).c_str());
            data_types_c[col] = static_cast<TSDataType>(data_type[i]);
            table_schema.column_schemas[col++] = (ColumnSchema){
                .column_name =
                    strdup(std::string("FIELD" + std::to_string(i)).c_str()),
                .data_type = static_cast<TSDataType>(data_type[i]),
                .column_category = FIELD};
        }
    }

    remove("bench_mark_c.tsfile");
    WriteFile file = write_file_new("bench_mark_c.tsfile", &code);
    HANDLE_ERROR(code);
    TsFileWriter writer = tsfile_writer_new(file, &table_schema, &code);
    HANDLE_ERROR(code);
    free_table_schema(table_schema);
    int64_t prepare_time = 0;
    int64_t writing_time = 0;
    int64_t timestamp = 0;
    int64_t row_num =
        bench::tag1_num * bench::tag2_num * bench::timestamp_per_tag;
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < bench::tablet_num; i++) {
        csv_file << iter_num++ <<","<< get_memory_usage() << "\n";
        int cur_row = 0;
        print_progress_bar(i, bench::tablet_num);
        auto prepare_start = std::chrono::high_resolution_clock::now();
        auto tablet =
            tablet_new(column_list, data_types_c, column + 2, row_num);
        for (int tag1 = 0; tag1 < bench::tag1_num; tag1++) {
            for (int tag2 = 0; tag2 < bench::tag2_num; tag2++) {
                for (int row = 0; row < bench::timestamp_per_tag; row++) {
                    tablet_add_timestamp(tablet, cur_row, timestamp + row);
                    tablet_add_value_by_index_string(
                        tablet, cur_row, 0,
                        std::string("TAG1_" + std::to_string(tag1)).c_str());
                    tablet_add_value_by_index_string(
                        tablet, cur_row, 1,
                        std::string("TAG2_" + std::to_string(tag2)).c_str());
                    for (int col = 2; col < column + 2; col++) {
                        switch (data_types_c[col]) {
                            case TS_DATATYPE_INT32:
                                tablet_add_value_by_index_int32_t(
                                    tablet, cur_row, col,
                                    static_cast<int32_t>(timestamp));
                                break;
                            case TS_DATATYPE_INT64:
                                tablet_add_value_by_index_int64_t(
                                    tablet, cur_row, col,
                                    static_cast<int64_t>(timestamp));
                                break;
                            case TS_DATATYPE_FLOAT:
                                tablet_add_value_by_index_float(
                                    tablet, cur_row, col,
                                    static_cast<float>(timestamp));
                                break;
                            case TS_DATATYPE_DOUBLE:
                                tablet_add_value_by_index_double(
                                    tablet, cur_row, col,
                                    static_cast<double>(timestamp));
                                break;
                            case TS_DATATYPE_BOOLEAN:
                                tablet_add_value_by_index_bool(
                                    tablet, cur_row, col,
                                    static_cast<bool>(timestamp % 2));
                                break;
                            default:
                                ;
                        }
                    }
                    cur_row++;
                }
            }
        }
        csv_file << iter_num++ <<","<< get_memory_usage() << "\n";
        auto prepare_end = std::chrono::high_resolution_clock::now();
        prepare_time += std::chrono::duration_cast<std::chrono::microseconds>(
                            prepare_end - prepare_start)
                            .count();

        auto writing_start = std::chrono::high_resolution_clock::now();
        tsfile_writer_write(writer, tablet);
        auto writing_end = std::chrono::high_resolution_clock::now();
        writing_time += std::chrono::duration_cast<std::chrono::microseconds>(
                            writing_end - writing_start)
                            .count();
        free_tablet(&tablet);
        timestamp += bench::timestamp_per_tag;
        csv_file << iter_num++ <<","<< get_memory_usage() << "\n";
    }

    csv_file << iter_num++ <<","<< get_memory_usage() << "\n";
    auto close_start = std::chrono::high_resolution_clock::now();
    tsfile_writer_close(writer);
    auto close_end = std::chrono::high_resolution_clock::now();

    writing_time += std::chrono::duration_cast<std::chrono::microseconds>(
                        close_end - close_start)
                        .count();
    free_write_file(&file);
    auto end = std::chrono::high_resolution_clock::now();

    csv_file << iter_num++ <<","<< get_memory_usage() << "\n";
    FILE* file_to_size = fopen("bench_mark_c.tsfile", "rb");
    if (!file_to_size) {
        std::cout << "unable to open file" << std::endl;
        return -1;
    }
    fseeko(file_to_size, 0, SEEK_END);
    off_t size = ftello(file_to_size);
    fclose(file_to_size);

    std::cout << "=======" << std::endl;
    std::cout << "Finish writing for C" << std::endl;
    std::cout << "Tsfile size is " << size << " bytes " << " ~ " << size / 1024
              << "KB" << std::endl;

    double pre_time = prepare_time / 1000.0 / 1000.0;
    double write_time = writing_time / 1000.0 / 1000.0;
    std::cout << "Preparing time is " << pre_time << " s" << std::endl;
    std::cout << "Writing  time is " << write_time << " s" << std::endl;
    std::cout << "writing speed is "
              << static_cast<long long>(
                     bench::tablet_num * bench::tag1_num * bench::tag2_num *
                     bench::timestamp_per_tag * (column_num_c + 2) /
                     (pre_time + write_time))
              << " points/s" << std::endl;
    std::cout << "total time is "
              << std::chrono::duration_cast<std::chrono::microseconds>(end -
                                                                       start)
                         .count() /
                     1000.0 / 1000.0
              << " s" << std::endl;
    std::cout << "========" << std::endl;
    return 0;
}

int bench_mark_c_read() {
    std::cout << "Bench mark c read" << std::endl;
    int code = common::E_OK;
    TsFileReader reader = tsfile_reader_new("bench_mark_c.tsfile", &code);
    ResultSet result =
        tsfile_query_table(reader, "TestTable", column_list, column_num_c,
                           INT64_MIN, INT64_MAX, &code);
    int64_t row = 0;
    auto read_start = std::chrono::high_resolution_clock::now();
    while (tsfile_result_set_next(result, &code) && code == common::E_OK) {
        row++;
    }
    auto read_end = std::chrono::high_resolution_clock::now();
    int64_t total_points = row * column_num_c;
    double reading_time;
    reading_time = std::chrono::duration_cast<std::chrono::microseconds>(
                       read_end - read_start)
                       .count() /
                   1000.0 / 1000.0;

    std::cout << "total points is " << total_points << std::endl;
    std::cout << "reading time is " << reading_time << " s" << std::endl;
    std::cout << "read speed:"
              << static_cast<int64_t>(total_points / reading_time)
              << " points/s" << std::endl;
    std::cout << "====================" << std::endl;
    free_tsfile_result_set(&result);
    tsfile_reader_close(reader);
    return 0;
}