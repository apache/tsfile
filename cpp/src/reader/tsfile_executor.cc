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

int TsFileExecutor::init(ReadFile *read_file) {
    int ret = E_OK;
    io_reader_.reset();
    if (RET_FAIL(io_reader_.init(read_file))) {
    } else {
        is_inited_ = true;
    }
    return ret;
}

int TsFileExecutor::init(const std::string &file_path) {
    int ret = E_OK;
    io_reader_.reset();
    if (RET_FAIL(io_reader_.init(file_path))) {
    } else {
        is_inited_ = true;
    }
    return ret;
}

int TsFileExecutor::execute(QueryExpression *query_expr,
                            QueryDataSet *&ret_qds) {
    ASSERT(is_inited_);
    query_exprs_ = query_expr;
    std::vector<Path> paths = query_expr->selected_series_;
    Expression *origin_expr = query_exprs_->expression_;
    Expression *regular_expr = nullptr;
    if (query_exprs_->has_filter_) {
        regular_expr = query_exprs_->optimize(origin_expr, paths);
        if (regular_expr == nullptr) {
            return E_SDK_QUERY_OPTIMIZE_ERR;
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

int TsFileExecutor::execute_may_with_global_timefilter(QueryExpression *qe,
                                                       QueryDataSet *&ret_qds) {
    int ret = E_OK;
    QDSWithoutTimeGenerator *qds = new QDSWithoutTimeGenerator;
    ret = qds->init(&io_reader_, qe);
    if (ret != E_OK) {
        delete qds;
        qds = nullptr;
    }
    ret_qds = qds;
    return ret;
}

int TsFileExecutor::execute_with_timegenerator(QueryExpression *qe,
                                               QueryDataSet *&ret_qds) {
    int ret = E_OK;
    QDSWithTimeGenerator *qds = new QDSWithTimeGenerator;
    ret = qds->init(&io_reader_, qe);
    if (ret != E_OK) {
        delete qds;
        qds = nullptr;
    }
    ret_qds = qds;
    return ret;
}

void TsFileExecutor::destroy_query_data_set(QueryDataSet *qds) { delete qds; }

}  // namespace storage
