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
#ifndef READER_DEVICE_ORDERED_TSBLOCK_READER_H
#define READER_DEVICE_ORDERED_TSBLOCK_READER_H
#include "reader/block/single_device_tsblock_reader.h"
#include "reader/block/tsblock_reader.h"
#include "reader/meta_data_querier.h"
#include "reader/task/device_task_iterator.h"
namespace storage {

class DeviceTaskIterator;
class SingleDeviceTsBlockReader;

class DeviceOrderedTsBlockReader : public TsBlockReader {
   public:
    explicit DeviceOrderedTsBlockReader(
        std::unique_ptr<DeviceTaskIterator> device_task_iterator,
        IMetadataQuerier *metadata_querier, int32_t block_size,
        TsFileIOReader *tsfile_io_reader, Filter *time_filter,
        Filter *field_filter)
        : device_task_iterator_(std::move(device_task_iterator)),
          metadata_querier_(metadata_querier),
          block_size_(block_size),
          tsfile_io_reader_(tsfile_io_reader),
          time_filter_(time_filter),
          field_filter_(field_filter) {}
    ~DeviceOrderedTsBlockReader() override { close(); }

    int has_next(bool &has_next) override;
    int next(common::TsBlock *&ret_block) override;
    void close() override;

   private:
    std::unique_ptr<DeviceTaskIterator> device_task_iterator_;
    IMetadataQuerier *metadata_querier_;
    int32_t block_size_;
    SingleDeviceTsBlockReader *current_reader_ = nullptr;
    TsFileIOReader *tsfile_io_reader_;
    Filter *time_filter_ = nullptr;
    Filter *field_filter_ = nullptr;
};
}  // namespace storage

#endif  // READER_DEVICE_ORDERED_TSBLOCK_READER_H