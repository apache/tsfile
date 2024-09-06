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

#include <time.h>

#include <iostream>
#include <random>
#include <string>

#include "cpp_examples.h"

using namespace storage;

long getNowTime() { return time(nullptr); }

static std::string generate_random_string(int length) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 61);

    const std::string chars =
        "0123456789"
        "abcdefghijklmnopqrstuvwxyz"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    std::string random_string;

    for (int i = 0; i < length; ++i) {
        random_string += chars[dis(gen)];
    }

    return random_string;
}

int demo_write() {
    TsFileWriter* tsfile_writer_ = new TsFileWriter();
    libtsfile_init();
    std::string file_name_ = std::string("tsfile_writer_test_") +
                             generate_random_string(10) +
                             std::string(".tsfile");
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    mode_t mode = 0666;
    tsfile_writer_->open(file_name_, flags, mode);
    remove(file_name_.c_str());
    const int device_num = 50;
    const int measurement_num = 50;
    std::vector<MeasurementSchema> schema_vec[50];
    for (int i = 0; i < device_num; i++) {
        std::string device_name = "test_device" + std::to_string(i);
        for (int j = 0; j < measurement_num; j++) {
            std::string measure_name = "measurement" + std::to_string(j);
            schema_vec[i].push_back(
                MeasurementSchema(measure_name, common::TSDataType::INT32,
                                  common::TSEncoding::PLAIN,
                                  common::CompressionType::UNCOMPRESSED));
            tsfile_writer_->register_timeseries(
                device_name, measure_name, common::TSDataType::INT32,
                common::TSEncoding::PLAIN,
                common::CompressionType::UNCOMPRESSED);
        }
    }

    std::cout << "input tablet size" << std::endl;
    int tablet_size;
    std::cin >> tablet_size;

    int max_rows = 100000;
    int cur_row = 0;
    long start = getNowTime();
    for (; cur_row < max_rows;) {
        if (cur_row + tablet_size > max_rows) {
            tablet_size = max_rows - cur_row;
        }
        for (int i = 0; i < device_num; i++) {
            std::string device_name = "test_device" + std::to_string(i);
            Tablet tablet(device_name, &schema_vec[i], tablet_size);
            tablet.init();
            for (int row = 0; row < tablet_size; row++) {
                tablet.set_timestamp(row, 16225600 + cur_row + row);
            }
            for (int j = 0; j < measurement_num; j++) {
                for (int row = 0; row < tablet_size; row++) {
                    tablet.set_value(row, j, row + cur_row);
                }
            }
            tsfile_writer_->write_tablet(tablet);
            tsfile_writer_->flush();
        }
        cur_row += tablet_size;
        std::cout << "finish writing " << cur_row << " rows" << std::endl;
    }

    tsfile_writer_->close();
    long end = getNowTime();
    printf("interval waitForResults is %ld \n", end - start);
    return 0;
}
