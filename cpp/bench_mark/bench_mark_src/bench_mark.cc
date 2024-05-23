#include <chrono>
#include <cmath>
#include <iostream>
#include <numeric>
#include <string>

#include "bench_conf.h"
#include "common/db_common.h"
#include "common/global.h"
#include "common/path.h"
#include "writer/tsfile_writer.h"

std::vector<int> register_timeseries(storage::TsFileWriter& writer,
                                     int timeseries_num,
                                     std::vector<int> type_list) {
    auto start = std::chrono::high_resolution_clock::now();
    int sum = std::accumulate(type_list.begin(), type_list.end(), 0);
    std::vector<float> ratio_list;
    for (int i = 0; i < type_list.size(); i++) {
        ratio_list.push_back((float)type_list[i] / sum);
    }
    std::vector<int> type_num;
    for (int i = 0; i < common::TSDataType::TEXT - 1; i++) {
        type_num.push_back((int)std::ceil(timeseries_num * ratio_list[i]));
    }
    type_num.push_back(timeseries_num -
                       std::accumulate(type_num.begin(), type_num.end(), 0));
    writer.open("/tmp/tsfile_test.tsfile", O_CREAT | O_RDWR, 0644);
    int ind = 0;
    int ret = 0;
    int type = 0;
    for (auto num : type_num) {
        for (int i = 0; i < num; i++) {
            std::string device_name = "root.db001.dev" + std::to_string(ind);
            std::string measurement_name = "m" + std::to_string(ind);
            ret = writer.register_timeseries(
                device_name, measurement_name, (common::TSDataType)type,
                common::TSEncoding::PLAIN,
                common::CompressionType::UNCOMPRESSED);
            ind++;
        }
        std::cout << "register finished for TsDataType"
                  << common::s_data_type_names[type]
                  << " timeseries num: " << num << std::endl;
        type++;
    }
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    std::cout << "register " << timeseries_num << "timeseries in file"
              << "./test_data/tsfile_test.tsfile" << std::endl;
    std::cout << "register timeseries cost time: " << elapsed.count() << "s"
              << std::endl;
    return type_num;
}

void test_writer_benchmark(storage::TsFileWriter& writer, int loop_num,
                           std::vector<int> type_num) {
    std::cout << "start writing data" << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    int type = 0;
    for (int i = 0; i < loop_num; i++) {
        int ind = 0;
        for (auto num : type_num) {
            for (int j = 0; j < num; j++) {
                std::string device_name =
                    "root.db001.dev" + std::to_string(ind);
                std::string measurement_name = "m" + std::to_string(ind);
                long long currentTimeStamp = i;
                storage::TsRecord record(currentTimeStamp, device_name, 1);
                switch (type) {
                    case common::INT32: {
                        storage::DataPoint point(measurement_name, 10000 + i);
                        record.points_.push_back(point);
                        break;
                    }
                    case common::INT64: {
                        storage::DataPoint point(measurement_name,
                                                 int64_t(10000 + i));
                        record.points_.push_back(point);
                        break;
                    }
                    case common::BOOLEAN: {
                        storage::DataPoint point(measurement_name, i / 2 == 0);
                        record.points_.push_back(point);
                        break;
                    }
                    case common::FLOAT: {
                        storage::DataPoint point(measurement_name, (float)i);
                        record.points_.push_back(point);
                        break;
                    }
                    case common::DOUBLE: {
                        storage::DataPoint point(measurement_name, (double)i);
                        record.points_.push_back(point);
                        break;
                    }
                }
                int ret = writer.write_record(record);
                ASSERT(ret == 0);
                ind++;
            }
            type++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = end - start;
    int timeseries_num = std::accumulate(type_num.begin(), type_num.end(), 0);
    std::cout << "writer loop: " << loop_num
              << " timeseries num: " << timeseries_num << " records in file"
              << "./test_data/tsfile_test.tsfile" << std::endl;
    std::cout << "total num of points: " << loop_num * timeseries_num
              << std::endl;
    std::cout << "writer data cost time: " << elapsed.count() << "s"
              << std::endl;
    std::cout << "writer data speed:"
              << loop_num * timeseries_num / elapsed.count() << " points/s"
              << std::endl;
    writer.flush();
    writer.close();
    auto end_flush = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_flush = end_flush - end;
    std::cout << "flush data cost time: " << elapsed_flush.count() << "s"
              << std::endl;
}

int main() {
    std::cout << "LibTsFile benchmark" << std::endl;
    std::cout << "LOOP_NUM:" << bench::LOOP_NUM << std::endl;
    std::cout << "THREAD_NUM:" << bench::THREAD_NUM << std::endl;
    std::cout << "TIMESERIES_NUM:" << bench::TIMESERIES_NUM << std::endl;
    std::cout << "TYPE_LIST: " << bench::TYPE_LIST[0] << ":"
              << bench::TYPE_LIST[1] << ":" << bench::TYPE_LIST[2] << ":"
              << bench::TYPE_LIST[3] << ":" << bench::TYPE_LIST[4] << ":"
              << bench::TYPE_LIST[5] << std::endl;
    std::cout << "init tsfile config value" << std::endl;
    common::init_config_value();
    storage::TsFileWriter writer;
    auto type_num =
        register_timeseries(writer, bench::TIMESERIES_NUM, bench::TYPE_LIST);
    test_writer_benchmark(writer, bench::LOOP_NUM, type_num);
    return 0;
}
