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

#include "reader/block/device_ordered_tsblock_reader.h"

namespace storage {

int DeviceOrderedTsBlockReader::has_next(bool &has_next) {
    int ret = common::E_OK;
    if (current_reader_ != nullptr && IS_SUCC(current_reader_->has_next(has_next)) && has_next) {
        return common::E_OK;
    }
    if (current_reader_ != nullptr) {
        delete current_reader_;
        current_reader_ = nullptr;
    }
    while (device_task_iterator_->has_next()) {
        DeviceQueryTask *task = nullptr;
        if (IS_FAIL(device_task_iterator_->next(task))) {
            has_next = false;
        }
        if (current_reader_) {
            delete current_reader_;
            current_reader_ = nullptr;
        }
        current_reader_ = new SingleDeviceTsBlockReader(
            task, block_size_, metadata_querier_, tsfile_io_reader_, time_filter_,
            field_filter_);
        if (current_reader_ == nullptr) {
            has_next = false;
            return common::E_OOM;
        }
        if (RET_FAIL(current_reader_->has_next(has_next))) {
            return ret;
        } else {
            return common::E_OK;
        }
    }
    return ret;
}

int DeviceOrderedTsBlockReader::next(common::TsBlock *&ret_block) {
    int ret = common::E_OK;
    bool next = false;
    if (RET_FAIL(has_next(next)) || !next) {
    } else if (RET_FAIL(current_reader_->next(ret_block))) {
    }
    return ret;
}

void DeviceOrderedTsBlockReader::close() {
    if (current_reader_) {
        delete current_reader_;
        current_reader_ = nullptr;
    }
    if (time_filter_ != nullptr) {
        delete time_filter_;
        time_filter_ = nullptr;
    }
    if (field_filter_ != nullptr) {
        delete field_filter_;
        field_filter_ = nullptr;
    }
}

}  // namespace storage
