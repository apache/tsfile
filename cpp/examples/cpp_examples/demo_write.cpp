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
#include <string>

#include "cpp_examples.h"

int demo_write() {
    storage::TsFileWriter tsfile_writer;
    std::string device_name = "root.db001.dev001";
    std::string measurement_name = "m001";
    storage::libtsfile_init();
    int ret = tsfile_writer.open("cpp_rw.tsfile", O_CREAT | O_RDWR, 0644);
    ASSERT(ret == 0);
    ret = tsfile_writer.register_timeseries(device_name, measurement_name,
                                            common::INT32, common::PLAIN,
                                            common::UNCOMPRESSED);
    ASSERT(ret == 0);
    std::cout << "get open ret: " << ret << std::endl;

    int row_count = 100;
    for (int i = 1; i < row_count; ++i) {
        storage::DataPoint point(measurement_name, 10000 + i);
        storage::TsRecord record(i, device_name, 1);
        record.points_.push_back(point);
        ret = tsfile_writer.write_record(record);
        ASSERT(ret == 0);
    }

    tsfile_writer.flush();
    std::cout << "finish flush" << std::endl;
    tsfile_writer.close();
    std::cout << "tsfile closed." << std::endl;
    storage::libtsfile_destroy();
    std::cout << "tsfile to destory." << std::endl;
    std::cout << "finish writing" << std::endl;
    std::cout << "will close our files" << std::endl;
    return 0;
}
