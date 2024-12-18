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
#include "tsfile_reader.h"

#include "common/schema.h"
#include "filter/time_operator.h"
#include "tsfile_executor.h"

using namespace common;
using namespace storage;

namespace storage {

TsFileReader::TsFileReader() : read_file_(nullptr), tsfile_executor_(nullptr) {}

TsFileReader::~TsFileReader() { close(); }

int TsFileReader::open(const std::string &file_path) {
    int ret = E_OK;
    read_file_ = new storage::ReadFile;
    tsfile_executor_ = new storage::TsFileExecutor();
    if (RET_FAIL(read_file_->open(file_path))) {
        std::cout << "filed to open file " << ret << std::endl;
    } else if (RET_FAIL(tsfile_executor_->init(read_file_))) {
        std::cout << "filed to init " << ret << std::endl;
    }
    return ret;
}

int TsFileReader::close() {
    int ret = E_OK;
    if (tsfile_executor_ != nullptr) {
        delete tsfile_executor_;
        tsfile_executor_ = nullptr;
    }
    if (read_file_ != nullptr) {
        read_file_->close();
        delete read_file_;
        read_file_ = nullptr;
    }
    return ret;
}

int TsFileReader::query(QueryExpression *qe, ResultSet *&ret_qds) {
    return tsfile_executor_->execute(qe, ret_qds);
}

int TsFileReader::query(std::vector<std::string> &path_list, int64_t start_time,
                        int64_t end_time, ResultSet *&result_set) {
    int ret = E_OK;
    Filter *time_filter = new TimeBetween(start_time, end_time, false);
    Expression *exp =
        new storage::Expression(storage::GLOBALTIME_EXPR, time_filter);
    std::vector<Path> path_list_vec;
    for (const auto &path : path_list) {
        path_list_vec.emplace_back(Path(path, true));
    }
    QueryExpression *query_expression =
        QueryExpression::create(path_list_vec, exp);
    ret = tsfile_executor_->execute(query_expression, result_set);
    return ret;
}

void TsFileReader::destroy_query_data_set(storage::ResultSet *qds) {
    tsfile_executor_->destroy_query_data_set(qds);
}

std::vector<std::string> TsFileReader::get_all_devices() {
    TsFileMeta *tsfile_meta = tsfile_executor_->get_tsfile_meta();
    std::vector<std::string> device_ids;
    if (tsfile_meta != nullptr) {
        PageArena pa;
        pa.init(512, MOD_TSFILE_READER);
        get_all_devices(device_ids, tsfile_meta->index_node_, pa);  
    }
    return device_ids;
}

int TsFileReader::get_all_devices(std::vector<std::string> &device_ids, MetaIndexNode *index_node, PageArena &pa) {
    int ret = E_OK;
    if (index_node != nullptr) {
        if (index_node->node_type_ == LEAF_DEVICE) {
            for (const auto &meta_index_entry : index_node->children_) {
                device_ids.push_back(meta_index_entry->name_.to_std_string());
            }
        } else {
            for (size_t idx = 0; idx < index_node->children_.size(); idx++) {
                MetaIndexEntry* meta_index_entry = index_node->children_[idx];
                int start_offset = meta_index_entry->offset_;
                int end_offset = index_node->end_offset_;
                if (idx + 1 < index_node->children_.size()) {
                    end_offset = index_node->children_[idx + 1]->offset_;
                }
                ASSERT(end_offset - start_offset > 0);
                const int32_t read_size = (int32_t)end_offset - start_offset;
                int32_t ret_read_len = 0;
                char *data_buf = (char *)pa.alloc(read_size);
                void *m_idx_node_buf = pa.alloc(sizeof(MetaIndexNode));
                if (IS_NULL(data_buf) || IS_NULL(m_idx_node_buf)) {
                    return E_OOM;
                }
                MetaIndexNode *top_node = new (m_idx_node_buf) MetaIndexNode(&pa);
                if (RET_FAIL(read_file_->read(start_offset, data_buf, read_size,
                                ret_read_len))) {
                } else if (RET_FAIL(top_node->deserialize_from(data_buf, read_size))) {
                } else {
                    ret = get_all_devices(device_ids, top_node, pa);
                }
                top_node->destroy();
            }
        }
    }
    return ret;
}

int TsFileReader::get_timeseries_schema(
    const std::string &device_id, std::vector<MeasurementSchema> &result) {
    int ret = E_OK;
    std::vector<ITimeseriesIndex *> timeseries_indexs;
    PageArena pa;
    pa.init(512, MOD_TSFILE_READER);
    if (RET_FAIL(tsfile_executor_->get_tsfile_io_reader()
                     ->get_device_timeseries_meta_without_chunk_meta(
                         device_id, timeseries_indexs, pa))) {
    } else {
        for (auto timeseries_index : timeseries_indexs) {
            MeasurementSchema ms(
                timeseries_index->get_measurement_name().to_std_string(),
                timeseries_index->get_data_type());
            result.push_back(ms);
        }
    }
    return E_OK;
}

ResultSet *TsFileReader::read_timeseries(
    const std::string &device_name, std::vector<std::string> measurement_name) {
    return nullptr;
}

}  // namespace storage
