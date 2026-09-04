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

#include "tsfile_executor.h"

#include "expression.h"
#include "qds_with_timegenerator.h"
#include "qds_without_timegenerator.h"
#include "reader/block/prepared_series_tsblock_reader.h"
#include "reader/filter/time_operator.h"
#include "reader/prepared_series.h"
#include "reader/table_result_set.h"

using namespace common;

namespace storage {

TsFileExecutor::TsFileExecutor()
    : io_reader_(),
      query_exprs_(nullptr),
      data_scan_iter_(),
      tsblocks_(),
      time_iters_(),
      value_iters_(),
      is_inited_(false) {}

TsFileExecutor::~TsFileExecutor() {}

int TsFileExecutor::init(RandomAccessFile* read_file) {
    int ret = E_OK;
    io_reader_.reset();
    if (RET_FAIL(io_reader_.init(read_file))) {
    } else {
        is_inited_ = true;
    }
    return ret;
}

int TsFileExecutor::init(const std::string& file_path) {
    int ret = E_OK;
    io_reader_.reset();
    if (RET_FAIL(io_reader_.init(file_path))) {
    } else {
        is_inited_ = true;
    }
    return ret;
}

int TsFileExecutor::execute(QueryExpression* query_expr, ResultSet*& ret_qds) {
    ASSERT(is_inited_);
    query_exprs_ = query_expr;
    std::vector<Path> paths = query_exprs_->selected_series_;
    Expression* origin_expr = query_exprs_->expression_;
    Expression* regular_expr = nullptr;
    if (query_exprs_->has_filter_) {
        regular_expr = query_exprs_->optimize(origin_expr, paths);
        if (regular_expr == nullptr) {
            return E_INVALID_ARG;
        }
        query_exprs_->set_expression(regular_expr);
    }

    if (regular_expr == nullptr || regular_expr->type_ == GLOBALTIME_EXPR) {
#if DEBUG_SE
        std::cout << "got into 1 path" << std::endl;
#endif
        return execute_may_with_global_timefilter(query_exprs_, ret_qds);
    } else {
#if DEBUG_SE
        std::cout << "got into 2 path" << std::endl;
#endif
        // no filter or just global time filter
        return execute_with_timegenerator(query_exprs_, ret_qds);
    }
}

int TsFileExecutor::execute(QueryExpression* query_expr, ResultSet*& ret_qds,
                            int offset, int limit) {
    ASSERT(is_inited_);
    query_exprs_ = query_expr;

    int ret = E_OK;
    QDSWithoutTimeGenerator* qds = new QDSWithoutTimeGenerator;
    ret = qds->init(&io_reader_, query_expr, offset, limit);
    if (ret != E_OK) {
        delete qds;
        qds = nullptr;
    }
    ret_qds = qds;
    return ret;
}

int TsFileExecutor::prepare_series(const FileGeneration& generation,
                                   const PreparedLocator& locator,
                                   std::shared_ptr<PreparedSeries>& prepared) {
    ASSERT(is_inited_);
    return io_reader_.prepare_series(generation, locator, prepared);
}

int TsFileExecutor::prepare_series(
    const FileGeneration& generation, const PreparedLocator& locator,
    const std::shared_ptr<PreparedSeries>& aligned_time_owner,
    std::shared_ptr<PreparedSeries>& prepared) {
    ASSERT(is_inited_);
    return io_reader_.prepare_series(generation, locator, aligned_time_owner,
                                     prepared);
}

int TsFileExecutor::execute_prepared(
    const std::shared_ptr<PreparedSeries>& prepared, int64_t start_time,
    int64_t end_time, int offset, int limit, const std::string& column_name,
    ResultSet*& ret_qds) {
    ASSERT(is_inited_);
    ret_qds = nullptr;
    if (prepared == nullptr || start_time > end_time || offset < 0) {
        return E_INVALID_ARG;
    }
    auto tsblock_reader = std::unique_ptr<PreparedSeriesTsBlockReader>(
        new PreparedSeriesTsBlockReader());
    int ret = tsblock_reader->init(&io_reader_, prepared,
                                   new TimeBetween(start_time, end_time, false),
                                   offset, limit);
    if (ret != E_OK) {
        return ret;
    }
    std::vector<std::string> column_names(1, column_name);
    std::vector<common::TSDataType> data_types(
        1, tsblock_reader->value_data_type());
    ret_qds = new TableResultSet(std::move(tsblock_reader), column_names,
                                 data_types, RETURN_BATCH);
    return E_OK;
}

int TsFileExecutor::execute_prepared_multi(
    const std::vector<std::shared_ptr<PreparedSeries>>& prepared,
    int64_t start_time, int64_t end_time, int offset, int limit,
    ResultSet*& ret_qds) {
    ASSERT(is_inited_);
    ret_qds = nullptr;
    if (prepared.empty() || start_time > end_time || offset < 0) {
        return E_INVALID_ARG;
    }

    auto tsblock_reader = std::unique_ptr<PreparedSeriesTsBlockReader>(
        new PreparedSeriesTsBlockReader());
    int ret = tsblock_reader->init_multi(
        &io_reader_, prepared, new TimeBetween(start_time, end_time, false),
        offset, limit);
    if (ret != E_OK) {
        return ret;
    }

    std::vector<std::string> column_names;
    column_names.reserve(prepared.size());
    for (const auto& entry : prepared) {
        column_names.push_back(
            entry->index()->get_measurement_name().to_std_string());
    }
    std::vector<common::TSDataType> data_types =
        tsblock_reader->value_data_types();
    ret_qds =
        new TableResultSet(std::move(tsblock_reader), std::move(column_names),
                           std::move(data_types), RETURN_BATCH);
    return E_OK;
}

int TsFileExecutor::execute_may_with_global_timefilter(QueryExpression* qe,
                                                       ResultSet*& ret_qds) {
    int ret = E_OK;
    QDSWithoutTimeGenerator* qds = new QDSWithoutTimeGenerator;
    ret = qds->init(&io_reader_, qe);
    if (ret != E_OK) {
        delete qds;
        qds = nullptr;
    }
    ret_qds = qds;
    return ret;
}

int TsFileExecutor::execute_with_timegenerator(QueryExpression* qe,
                                               ResultSet*& ret_qds) {
    int ret = E_OK;
    QDSWithTimeGenerator* qds = new QDSWithTimeGenerator;
    ret = qds->init(&io_reader_, qe);
    if (ret != E_OK) {
        delete qds;
        qds = nullptr;
    }
    ret_qds = qds;
    return ret;
}

void TsFileExecutor::destroy_query_data_set(ResultSet* qds) {
    delete qds;
    qds = nullptr;
}

}  // namespace storage
