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

#include "tag_filter.h"

#include <algorithm>
#include <utility>

namespace storage {

// TagFilter base class implementation
TagFilter::TagFilter(int col_idx, std::string tag_value)
    : value_(std::move(tag_value)), value2_(""), col_idx_(col_idx) {}

TagFilter::~TagFilter() = default;

bool TagFilter::satisfyRow(int time, std::vector<std::string*> segments) const {
    return satisfyRow(segments);
}

bool TagFilter::satisfyRow(std::vector<std::string*> segments) const {
    ASSERT(false);
    return false;
}

// TagEq implementation
TagEq::TagEq(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {}

bool TagEq::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    return *segments[col_idx_] == value_;
}

// TagNeq implementation
TagNeq::TagNeq(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {}

bool TagNeq::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    return *segments[col_idx_] != value_;
}

// TagLt implementation
TagLt::TagLt(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {}

bool TagLt::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    return *segments[col_idx_] < value_;
}

// TagLteq implementation
TagLteq::TagLteq(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {}

bool TagLteq::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    return *segments[col_idx_] <= value_;
}

// TagGt implementation
TagGt::TagGt(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {}

bool TagGt::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    return *segments[col_idx_] > value_;
}

// TagGteq implementation
TagGteq::TagGteq(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {}

bool TagGteq::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    return *segments[col_idx_] >= value_;
}

// TagRegExp implementation
TagRegExp::TagRegExp(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {
    try {
        pattern_ = std::regex(value_);
        is_valid_pattern_ = true;
    } catch (const std::regex_error& e) {
        is_valid_pattern_ = false;
    }
}

bool TagRegExp::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size() || !is_valid_pattern_) return false;
    try {
        return std::regex_search(*segments[col_idx_], pattern_);
    } catch (const std::regex_error&) {
        return false;
    }
}

// TagNotRegExp implementation
TagNotRegExp::TagNotRegExp(int col_idx, std::string tag_value)
    : TagFilter(col_idx, std::move(tag_value)) {
    try {
        pattern_ = std::regex(value_);
        is_valid_pattern_ = true;
    } catch (const std::regex_error& e) {
        is_valid_pattern_ = false;
    }
}

bool TagNotRegExp::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size() || !is_valid_pattern_) return false;
    try {
        return !std::regex_search(*segments[col_idx_], pattern_);
    } catch (const std::regex_error&) {
        return true;
    }
}

// TagBetween implementation
TagBetween::TagBetween(int col_idx, std::string lower_value,
                       std::string upper_value)
    : TagFilter(col_idx, std::move(lower_value)) {
    value2_ = std::move(upper_value);
}

bool TagBetween::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    const std::string& segment_value = *segments[col_idx_];
    return segment_value >= value_ && segment_value <= value2_;
}

// TagNotBetween implementation
TagNotBetween::TagNotBetween(int col_idx, std::string lower_value,
                             std::string upper_value)
    : TagFilter(col_idx, std::move(lower_value)) {
    value2_ = std::move(upper_value);
}

bool TagNotBetween::satisfyRow(std::vector<std::string*> segments) const {
    if (col_idx_ >= segments.size()) return false;
    const std::string& segment_value = *segments[col_idx_];
    return segment_value < value_ || segment_value > value2_;
}

// TagAnd implementation
TagAnd::TagAnd(TagFilter* left, TagFilter* right)
    : TagFilter(-1, ""), left_(left), right_(right) {}

TagAnd::~TagAnd() {
    delete left_;
    delete right_;
}

bool TagAnd::satisfyRow(std::vector<std::string*> segments) const {
    return left_->satisfyRow(segments) && right_->satisfyRow(segments);
}

// TagOr implementation
TagOr::TagOr(TagFilter* left, TagFilter* right)
    : TagFilter(-1, ""), left_(left), right_(right) {}

TagOr::~TagOr() {
    delete left_;
    delete right_;
}

bool TagOr::satisfyRow(std::vector<std::string*> segments) const {
    return left_->satisfyRow(segments) || right_->satisfyRow(segments);
}

// TagNot implementation
TagNot::TagNot(TagFilter* filter) : TagFilter(-1, ""), filter_(filter) {}

TagNot::~TagNot() { delete filter_; }

bool TagNot::satisfyRow(std::vector<std::string*> segments) const {
    return !filter_->satisfyRow(segments);
}

// TagFilterBuilder implementation
TagFilterBuilder::TagFilterBuilder(TableSchema* schema)
    : table_schema_(schema) {}

Filter* TagFilterBuilder::eq(const std::string& columnName,
                             const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagEq(idx, value);
}

Filter* TagFilterBuilder::neq(const std::string& columnName,
                              const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagNeq(idx, value);
}

Filter* TagFilterBuilder::lt(const std::string& columnName,
                             const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagLt(idx, value);
}

Filter* TagFilterBuilder::lteq(const std::string& columnName,
                               const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagLteq(idx, value);
}

Filter* TagFilterBuilder::gt(const std::string& columnName,
                             const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagGt(idx, value);
}

Filter* TagFilterBuilder::gteq(const std::string& columnName,
                               const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagGteq(idx, value);
}

Filter* TagFilterBuilder::reg_exp(const std::string& columnName,
                                  const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagRegExp(idx, value);
}

Filter* TagFilterBuilder::not_reg_exp(const std::string& columnName,
                                      const std::string& value) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagNotRegExp(idx, value);
}

Filter* TagFilterBuilder::between_and(const std::string& columnName,
                                      const std::string& lower,
                                      const std::string& upper) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagBetween(idx, lower, upper);
}

Filter* TagFilterBuilder::not_between_and(const std::string& columnName,
                                          const std::string& lower,
                                          const std::string& upper) {
    auto idx = get_id_column_index(columnName);
    if (idx < 0) return nullptr;
    return new TagNotBetween(idx, lower, upper);
}

Filter* TagFilterBuilder::and_filter(Filter* left, Filter* right) {
    return new TagAnd(dynamic_cast<TagFilter*>(left),
                      dynamic_cast<TagFilter*>(right));
}

Filter* TagFilterBuilder::or_filter(Filter* left, Filter* right) {
    return new TagOr(dynamic_cast<TagFilter*>(left),
                     dynamic_cast<TagFilter*>(right));
}

Filter* TagFilterBuilder::not_filter(Filter* filter) {
    return new TagNot(dynamic_cast<TagFilter*>(filter));
}

int TagFilterBuilder::get_id_column_index(const std::string& columnName) {
    int idColumnOrder = table_schema_->find_id_column_order(columnName);
    if (idColumnOrder == -1) {
        return -1;
    }
    return idColumnOrder + 1;
}

}  // namespace storage