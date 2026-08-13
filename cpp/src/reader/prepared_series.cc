/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include "reader/prepared_series.h"

#include <algorithm>

namespace storage {

bool PagePositionIndex::append_complete(const PageLocator& locator) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (locator.row_begin != covered_rows_ ||
        locator.row_end <= locator.row_begin) {
        return false;
    }
    pages_.push_back(locator);
    covered_rows_ = locator.row_end;
    return true;
}

bool PagePositionIndex::find(uint64_t row, PageLocator& result) const {
    std::lock_guard<std::mutex> guard(mutex_);
    if (row >= covered_rows_) {
        return false;
    }
    std::vector<PageLocator>::const_iterator it =
        std::upper_bound(pages_.begin(), pages_.end(), row,
                         [](uint64_t value, const PageLocator& locator) {
                             return value < locator.row_begin;
                         });
    if (it == pages_.begin()) {
        return false;
    }
    --it;
    if (row < it->row_begin || row >= it->row_end) {
        return false;
    }
    result = *it;
    return true;
}

uint64_t PagePositionIndex::covered_rows() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return covered_rows_;
}

size_t PagePositionIndex::size() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return pages_.size();
}

PreparedSeries::PreparedSeries(const FileGeneration& generation,
                               const PreparedLocator& locator)
    : generation_(generation), locator_(locator), arena_(), index_(nullptr) {
    arena_.init(512, common::MOD_TSFILE_READER);
}

PreparedSeries::~PreparedSeries() {
    index_ = nullptr;
    arena_.destroy();
}

}  // namespace storage
