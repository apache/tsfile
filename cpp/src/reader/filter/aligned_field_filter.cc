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

#include "reader/filter/aligned_field_filter.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <utility>

#include "common/statistic.h"

namespace storage {
namespace {

template <typename T>
bool compare_values(T left, T right, AlignedFieldFilter::CompareOp op) {
    switch (op) {
        case AlignedFieldFilter::CompareOp::EQ:
            return left == right;
        case AlignedFieldFilter::CompareOp::NE:
            return left != right;
        case AlignedFieldFilter::CompareOp::LT:
            return left < right;
        case AlignedFieldFilter::CompareOp::LE:
            return left <= right;
        case AlignedFieldFilter::CompareOp::GT:
            return left > right;
        case AlignedFieldFilter::CompareOp::GE:
            return left >= right;
    }
    return false;
}

int compare_string(const char* data, uint32_t len, const std::string& literal,
                   AlignedFieldFilter::CompareOp op) {
    const std::string value(data, len);
    switch (op) {
        case AlignedFieldFilter::CompareOp::EQ:
            return value == literal;
        case AlignedFieldFilter::CompareOp::NE:
            return value != literal;
        case AlignedFieldFilter::CompareOp::LT:
            return value < literal;
        case AlignedFieldFilter::CompareOp::LE:
            return value <= literal;
        case AlignedFieldFilter::CompareOp::GT:
            return value > literal;
        case AlignedFieldFilter::CompareOp::GE:
            return value >= literal;
    }
    return false;
}

bool is_integer_type(common::TSDataType type) {
    return type == common::INT32 || type == common::DATE ||
           type == common::INT64 || type == common::TIMESTAMP;
}

bool is_string_type(common::TSDataType type) {
    return type == common::STRING || type == common::TEXT ||
           type == common::BLOB;
}

std::string normalize_column_name(const std::string& column) {
    std::string normalized(column);
    for (char& ch : normalized) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    return normalized;
}

}  // namespace

AlignedFieldFilter::Literal AlignedFieldFilter::Literal::boolean(bool value) {
    Literal literal;
    literal.type = common::BOOLEAN;
    literal.bool_value = value;
    return literal;
}

AlignedFieldFilter::Literal AlignedFieldFilter::Literal::int32(int32_t value) {
    Literal literal;
    literal.type = common::INT32;
    literal.int32_value = value;
    return literal;
}

AlignedFieldFilter::Literal AlignedFieldFilter::Literal::int64(int64_t value) {
    Literal literal;
    literal.type = common::INT64;
    literal.int64_value = value;
    return literal;
}

AlignedFieldFilter::Literal AlignedFieldFilter::Literal::float32(float value) {
    Literal literal;
    literal.type = common::FLOAT;
    literal.float_value = value;
    return literal;
}

AlignedFieldFilter::Literal AlignedFieldFilter::Literal::float64(double value) {
    Literal literal;
    literal.type = common::DOUBLE;
    literal.double_value = value;
    return literal;
}

AlignedFieldFilter::Literal AlignedFieldFilter::Literal::string(
    std::string value) {
    Literal literal;
    literal.type = common::STRING;
    literal.string_value = std::move(value);
    return literal;
}

AlignedFieldFilter::AlignedFieldFilter(Kind kind, std::string column,
                                       CompareOp op, Literal literal)
    : kind_(kind),
      column_(std::move(column)),
      compare_op_(op),
      literal_(std::move(literal)) {}

AlignedFieldFilter::AlignedFieldFilter(Kind kind, Ptr left, Ptr right)
    : kind_(kind), left_(std::move(left)), right_(std::move(right)) {}

AlignedFieldFilter::Ptr AlignedFieldFilter::compare(std::string column,
                                                    CompareOp op,
                                                    Literal literal) {
    return Ptr(new AlignedFieldFilter(Kind::COMPARISON,
                                      normalize_column_name(column), op,
                                      std::move(literal)));
}

AlignedFieldFilter::Ptr AlignedFieldFilter::is_null(std::string column) {
    return Ptr(new AlignedFieldFilter(Kind::IS_NULL,
                                      normalize_column_name(column),
                                      CompareOp::EQ, Literal()));
}

AlignedFieldFilter::Ptr AlignedFieldFilter::is_not_null(std::string column) {
    return Ptr(new AlignedFieldFilter(Kind::IS_NOT_NULL,
                                      normalize_column_name(column),
                                      CompareOp::EQ, Literal()));
}

AlignedFieldFilter::Ptr AlignedFieldFilter::and_filter(Ptr left, Ptr right) {
    return Ptr(
        new AlignedFieldFilter(Kind::AND, std::move(left), std::move(right)));
}

AlignedFieldFilter::Ptr AlignedFieldFilter::or_filter(Ptr left, Ptr right) {
    return Ptr(
        new AlignedFieldFilter(Kind::OR, std::move(left), std::move(right)));
}

AlignedFieldFilter::Ptr AlignedFieldFilter::not_filter(Ptr child) {
    return negate(std::move(child));
}

AlignedFieldFilter::Ptr AlignedFieldFilter::negate(Ptr node) {
    if (node == nullptr) return nullptr;
    switch (node->kind_) {
        case Kind::COMPARISON: {
            CompareOp neg_op = CompareOp::EQ;
            switch (node->compare_op_) {
                case CompareOp::EQ:
                    neg_op = CompareOp::NE;
                    break;
                case CompareOp::NE:
                    neg_op = CompareOp::EQ;
                    break;
                case CompareOp::LT:
                    neg_op = CompareOp::GE;
                    break;
                case CompareOp::LE:
                    neg_op = CompareOp::GT;
                    break;
                case CompareOp::GT:
                    neg_op = CompareOp::LE;
                    break;
                case CompareOp::GE:
                    neg_op = CompareOp::LT;
                    break;
            }
            return Ptr(new AlignedFieldFilter(Kind::COMPARISON, node->column_,
                                              neg_op, node->literal_));
        }
        case Kind::IS_NULL:
            return is_not_null(node->column_);
        case Kind::IS_NOT_NULL:
            return is_null(node->column_);
        case Kind::NOT:
            return std::move(node->left_);
        case Kind::AND:
            return or_filter(negate(std::move(node->left_)),
                             negate(std::move(node->right_)));
        case Kind::OR:
            return and_filter(negate(std::move(node->left_)),
                              negate(std::move(node->right_)));
    }
    return nullptr;
}

std::vector<std::string> AlignedFieldFilter::referenced_columns() const {
    std::vector<std::string> columns;
    std::set<std::string> seen_columns;
    std::set<const AlignedFieldFilter*> seen_nodes;
    std::vector<const AlignedFieldFilter*> stack{this};
    while (!stack.empty()) {
        const AlignedFieldFilter* node = stack.back();
        stack.pop_back();
        if (node == nullptr || !seen_nodes.insert(node).second) continue;
        if (node->kind_ == Kind::COMPARISON || node->kind_ == Kind::IS_NULL ||
            node->kind_ == Kind::IS_NOT_NULL) {
            if (seen_columns.insert(node->column_).second) {
                columns.push_back(node->column_);
            }
            continue;
        }
        if (node->right_) stack.push_back(node->right_.get());
        if (node->left_) stack.push_back(node->left_.get());
    }
    return columns;
}

bool AlignedFieldFilter::may_match(
    const std::map<std::string, Statistic*>& statistics,
    int64_t row_count) const {
    if (kind_ == Kind::AND) {
        return !left_ || !right_ ||
               (left_->may_match(statistics, row_count) &&
                right_->may_match(statistics, row_count));
    }
    if (kind_ == Kind::OR) {
        return !left_ || !right_ ||
               (left_->may_match(statistics, row_count) ||
                right_->may_match(statistics, row_count));
    }
    if (kind_ == Kind::NOT) {
        // NOT can be pushed down to leaves in a future planner. Until then,
        // never use page statistics to prune a negated predicate.
        return true;
    }

    const auto found = statistics.find(column_);
    if (found == statistics.end() || found->second == nullptr) return true;
    Statistic* statistic = found->second;
    if (kind_ == Kind::IS_NULL) {
        return row_count <= 0 || statistic->count_ < row_count;
    }
    if (kind_ == Kind::IS_NOT_NULL) {
        return statistic->count_ > 0;
    }
    if (statistic->count_ <= 0) return false;

    if (auto* typed = dynamic_cast<Int32Statistic*>(statistic)) {
        if (literal_.type != common::INT32) return true;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ <= literal_.int32_value &&
                       literal_.int32_value <= typed->max_value_;
            case CompareOp::NE:
                return !(typed->min_value_ == typed->max_value_ &&
                         typed->min_value_ == literal_.int32_value &&
                         typed->count_ == row_count);
            case CompareOp::LT:
                return typed->min_value_ < literal_.int32_value;
            case CompareOp::LE:
                return typed->min_value_ <= literal_.int32_value;
            case CompareOp::GT:
                return typed->max_value_ > literal_.int32_value;
            case CompareOp::GE:
                return typed->max_value_ >= literal_.int32_value;
        }
    }
    if (auto* typed = dynamic_cast<Int64Statistic*>(statistic)) {
        if (literal_.type != common::INT64) return true;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ <= literal_.int64_value &&
                       literal_.int64_value <= typed->max_value_;
            case CompareOp::NE:
                return !(typed->min_value_ == typed->max_value_ &&
                         typed->min_value_ == literal_.int64_value &&
                         typed->count_ == row_count);
            case CompareOp::LT:
                return typed->min_value_ < literal_.int64_value;
            case CompareOp::LE:
                return typed->min_value_ <= literal_.int64_value;
            case CompareOp::GT:
                return typed->max_value_ > literal_.int64_value;
            case CompareOp::GE:
                return typed->max_value_ >= literal_.int64_value;
        }
    }
    if (auto* typed = dynamic_cast<FloatStatistic*>(statistic)) {
        if (literal_.type != common::FLOAT) return true;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ <= literal_.float_value &&
                       literal_.float_value <= typed->max_value_;
            case CompareOp::NE:
                return !(typed->min_value_ == typed->max_value_ &&
                         typed->min_value_ == literal_.float_value &&
                         typed->count_ == row_count);
            case CompareOp::LT:
                return typed->min_value_ < literal_.float_value;
            case CompareOp::LE:
                return typed->min_value_ <= literal_.float_value;
            case CompareOp::GT:
                return typed->max_value_ > literal_.float_value;
            case CompareOp::GE:
                return typed->max_value_ >= literal_.float_value;
        }
    }
    if (auto* typed = dynamic_cast<DoubleStatistic*>(statistic)) {
        if (literal_.type != common::DOUBLE) return true;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ <= literal_.double_value &&
                       literal_.double_value <= typed->max_value_;
            case CompareOp::NE:
                return !(typed->min_value_ == typed->max_value_ &&
                         typed->min_value_ == literal_.double_value &&
                         typed->count_ == row_count);
            case CompareOp::LT:
                return typed->min_value_ < literal_.double_value;
            case CompareOp::LE:
                return typed->min_value_ <= literal_.double_value;
            case CompareOp::GT:
                return typed->max_value_ > literal_.double_value;
            case CompareOp::GE:
                return typed->max_value_ >= literal_.double_value;
        }
    }
    return true;
}

bool AlignedFieldFilter::all_match(
    const std::map<std::string, Statistic*>& statistics,
    int64_t row_count) const {
    if (row_count <= 0) return false;
    if (kind_ == Kind::AND) {
        return left_ && right_ && left_->all_match(statistics, row_count) &&
               right_->all_match(statistics, row_count);
    }
    if (kind_ == Kind::OR) {
        return (left_ && left_->all_match(statistics, row_count)) ||
               (right_ && right_->all_match(statistics, row_count));
    }
    if (kind_ == Kind::NOT) {
        // Physical plans may keep a NOT node for now. Do not infer a
        // page-wide match from its child statistics.
        return false;
    }

    const auto found = statistics.find(column_);
    if (found == statistics.end() || found->second == nullptr) return false;
    Statistic* statistic = found->second;
    if (kind_ == Kind::IS_NULL) {
        return statistic->count_ == 0;
    }
    if (kind_ == Kind::IS_NOT_NULL) {
        return statistic->count_ == row_count;
    }
    // Ordinary comparisons do not match NULL. A page-wide match requires that
    // every row has a non-NULL value and that the whole min/max range passes.
    if (statistic->count_ != row_count) return false;

    if (auto* typed = dynamic_cast<Int32Statistic*>(statistic)) {
        if (literal_.type != common::INT32) return false;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ == literal_.int32_value &&
                       typed->max_value_ == literal_.int32_value;
            case CompareOp::NE:
                return literal_.int32_value < typed->min_value_ ||
                       literal_.int32_value > typed->max_value_;
            case CompareOp::LT:
                return typed->max_value_ < literal_.int32_value;
            case CompareOp::LE:
                return typed->max_value_ <= literal_.int32_value;
            case CompareOp::GT:
                return typed->min_value_ > literal_.int32_value;
            case CompareOp::GE:
                return typed->min_value_ >= literal_.int32_value;
        }
    }
    if (auto* typed = dynamic_cast<Int64Statistic*>(statistic)) {
        if (literal_.type != common::INT64) return false;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ == literal_.int64_value &&
                       typed->max_value_ == literal_.int64_value;
            case CompareOp::NE:
                return literal_.int64_value < typed->min_value_ ||
                       literal_.int64_value > typed->max_value_;
            case CompareOp::LT:
                return typed->max_value_ < literal_.int64_value;
            case CompareOp::LE:
                return typed->max_value_ <= literal_.int64_value;
            case CompareOp::GT:
                return typed->min_value_ > literal_.int64_value;
            case CompareOp::GE:
                return typed->min_value_ >= literal_.int64_value;
        }
    }
    if (auto* typed = dynamic_cast<FloatStatistic*>(statistic)) {
        if (literal_.type != common::FLOAT) return false;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ == literal_.float_value &&
                       typed->max_value_ == literal_.float_value;
            case CompareOp::NE:
                return literal_.float_value < typed->min_value_ ||
                       literal_.float_value > typed->max_value_;
            case CompareOp::LT:
                return typed->max_value_ < literal_.float_value;
            case CompareOp::LE:
                return typed->max_value_ <= literal_.float_value;
            case CompareOp::GT:
                return typed->min_value_ > literal_.float_value;
            case CompareOp::GE:
                return typed->min_value_ >= literal_.float_value;
        }
    }
    if (auto* typed = dynamic_cast<DoubleStatistic*>(statistic)) {
        if (literal_.type != common::DOUBLE) return false;
        switch (compare_op_) {
            case CompareOp::EQ:
                return typed->min_value_ == literal_.double_value &&
                       typed->max_value_ == literal_.double_value;
            case CompareOp::NE:
                return literal_.double_value < typed->min_value_ ||
                       literal_.double_value > typed->max_value_;
            case CompareOp::LT:
                return typed->max_value_ < literal_.double_value;
            case CompareOp::LE:
                return typed->max_value_ <= literal_.double_value;
            case CompareOp::GT:
                return typed->min_value_ > literal_.double_value;
            case CompareOp::GE:
                return typed->min_value_ >= literal_.double_value;
        }
    }
    return false;
}

int AlignedFieldFilter::evaluate(common::TsBlock& block,
                                 const std::vector<std::string>& column_names,
                                 SelectionMask& mask) const {
    std::map<std::string, uint32_t> column_index;
    for (uint32_t i = 0; i < column_names.size(); ++i) {
        column_index[column_names[i]] = i + 1;
    }

    struct Frame {
        const AlignedFieldFilter* node;
        uint8_t state;
    };
    std::vector<Frame> traversal;
    std::vector<const AlignedFieldFilter*> postorder;
    traversal.push_back({this, 0});
    while (!traversal.empty()) {
        Frame& frame = traversal.back();
        const AlignedFieldFilter* node = frame.node;
        if (node->kind_ == Kind::COMPARISON || node->kind_ == Kind::IS_NULL ||
            node->kind_ == Kind::IS_NOT_NULL) {
            postorder.push_back(node);
            traversal.pop_back();
        } else if (node->kind_ == Kind::NOT) {
            if (!node->left_) return common::E_INVALID_ARG;
            if (frame.state == 0) {
                frame.state = 1;
                traversal.push_back({node->left_.get(), 0});
            } else {
                postorder.push_back(node);
                traversal.pop_back();
            }
        } else {
            if (!node->left_ || !node->right_) return common::E_INVALID_ARG;
            if (frame.state == 0) {
                frame.state = 1;
                traversal.push_back({node->left_.get(), 0});
            } else if (frame.state == 1) {
                frame.state = 2;
                traversal.push_back({node->right_.get(), 0});
            } else {
                postorder.push_back(node);
                traversal.pop_back();
            }
        }
    }

    std::vector<SelectionMask> masks;
    masks.reserve(postorder.size());
    for (const auto* node : postorder) {
        if (node->kind_ == Kind::COMPARISON || node->kind_ == Kind::IS_NULL ||
            node->kind_ == Kind::IS_NOT_NULL) {
            SelectionMask leaf_mask(block.get_row_count());
            int ret = node->evaluate_leaf(block, column_index, leaf_mask);
            if (ret != common::E_OK) return ret;
            masks.push_back(std::move(leaf_mask));
        } else if (node->kind_ == Kind::NOT) {
            if (masks.empty()) return common::E_INVALID_ARG;
            masks.back().invert();
        } else {
            if (masks.size() < 2) return common::E_INVALID_ARG;
            SelectionMask right = std::move(masks.back());
            masks.pop_back();
            if (node->kind_ == Kind::AND) {
                masks.back().and_with(right);
            } else {
                masks.back().or_with(right);
            }
        }
    }
    if (masks.size() != 1) return common::E_INVALID_ARG;
    mask = std::move(masks.back());
    return common::E_OK;
}

int AlignedFieldFilter::evaluate_columns(const std::vector<ColumnView>& columns,
                                         uint32_t row_count,
                                         SelectionMask& mask) const {
    std::map<std::string, uint32_t> column_index;
    for (uint32_t i = 0; i < columns.size(); ++i) {
        column_index[columns[i].name] = i;
    }

    struct Frame {
        const AlignedFieldFilter* node;
        uint8_t state;
    };
    std::vector<Frame> traversal{{this, 0}};
    std::vector<const AlignedFieldFilter*> postorder;
    while (!traversal.empty()) {
        Frame& frame = traversal.back();
        const AlignedFieldFilter* node = frame.node;
        if (node->kind_ == Kind::COMPARISON || node->kind_ == Kind::IS_NULL ||
            node->kind_ == Kind::IS_NOT_NULL) {
            postorder.push_back(node);
            traversal.pop_back();
        } else if (node->kind_ == Kind::NOT) {
            if (!node->left_) return common::E_INVALID_ARG;
            if (frame.state == 0) {
                frame.state = 1;
                traversal.push_back({node->left_.get(), 0});
            } else {
                postorder.push_back(node);
                traversal.pop_back();
            }
        } else {
            if (!node->left_ || !node->right_) return common::E_INVALID_ARG;
            if (frame.state == 0) {
                frame.state = 1;
                traversal.push_back({node->left_.get(), 0});
            } else if (frame.state == 1) {
                frame.state = 2;
                traversal.push_back({node->right_.get(), 0});
            } else {
                postorder.push_back(node);
                traversal.pop_back();
            }
        }
    }

    std::vector<SelectionMask> masks;
    masks.reserve(postorder.size());
    for (const auto* node : postorder) {
        if (node->kind_ == Kind::COMPARISON || node->kind_ == Kind::IS_NULL ||
            node->kind_ == Kind::IS_NOT_NULL) {
            const auto found = column_index.find(node->column_);
            if (found == column_index.end()) return common::E_COLUMN_NOT_EXIST;
            SelectionMask leaf_mask(row_count);
            int ret = node->evaluate_leaf(columns[found->second], row_count,
                                          leaf_mask);
            if (ret != common::E_OK) return ret;
            masks.push_back(std::move(leaf_mask));
        } else if (node->kind_ == Kind::NOT) {
            if (masks.empty()) return common::E_INVALID_ARG;
            masks.back().invert();
        } else {
            if (masks.size() < 2) return common::E_INVALID_ARG;
            SelectionMask right = std::move(masks.back());
            masks.pop_back();
            if (node->kind_ == Kind::AND) {
                masks.back().and_with(right);
            } else {
                masks.back().or_with(right);
            }
        }
    }
    if (masks.size() != 1) return common::E_INVALID_ARG;
    mask = std::move(masks.back());
    return common::E_OK;
}

int AlignedFieldFilter::evaluate_leaf(const ColumnView& column,
                                      uint32_t row_count,
                                      SelectionMask& mask) const {
    if (column.type == common::INVALID_DATATYPE) return common::E_INVALID_ARG;
    uint32_t nonnull_index = column.nonnull_start;
    for (uint32_t row = 0; row < row_count; ++row) {
        const uint32_t source_row = column.row_offset + row;
        bool is_null = true;
        if (column.notnull_bitmap != nullptr &&
            source_row < column.notnull_bitmap_size * 8U) {
            is_null = (column.notnull_bitmap[source_row / 8U] &
                       (0x80U >> (source_row % 8U))) == 0;
        }

        const char* data = nullptr;
        uint32_t len = 0;
        if (!is_null) {
            if (column.type == common::STRING || column.type == common::TEXT ||
                column.type == common::BLOB) {
                if (column.string_values == nullptr) {
                    return common::E_INVALID_ARG;
                }
                data = column.string_values[nonnull_index].buf_;
                len = column.string_values[nonnull_index].len_;
            } else {
                if (column.fixed_values == nullptr) {
                    return common::E_INVALID_ARG;
                }
                const uint32_t elem_size =
                    common::get_data_type_size(column.type);
                data = column.fixed_values +
                       static_cast<size_t>(nonnull_index) * elem_size;
                len = elem_size;
            }
            ++nonnull_index;
        }

        bool satisfied = false;
        if (kind_ == Kind::IS_NULL) {
            satisfied = is_null;
        } else if (kind_ == Kind::IS_NOT_NULL) {
            satisfied = !is_null;
        } else if (!is_null) {
            if (column.type == common::BOOLEAN &&
                literal_.type == common::BOOLEAN) {
                bool value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.bool_value, compare_op_);
            } else if ((column.type == common::INT32 ||
                        column.type == common::DATE) &&
                       literal_.type == common::INT32) {
                int32_t value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.int32_value, compare_op_);
            } else if ((column.type == common::INT64 ||
                        column.type == common::TIMESTAMP) &&
                       literal_.type == common::INT64) {
                int64_t value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.int64_value, compare_op_);
            } else if (column.type == common::FLOAT &&
                       literal_.type == common::FLOAT) {
                float value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.float_value, compare_op_);
            } else if (column.type == common::DOUBLE &&
                       literal_.type == common::DOUBLE) {
                double value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.double_value, compare_op_);
            } else if (is_string_type(column.type) &&
                       is_string_type(literal_.type)) {
                satisfied = compare_string(data, len, literal_.string_value,
                                           compare_op_);
            } else {
                return common::E_NOT_SUPPORT;
            }
        }
        if (satisfied) mask.set_one(row);
    }
    return common::E_OK;
}

int AlignedFieldFilter::evaluate_leaf(
    common::TsBlock& block, const std::map<std::string, uint32_t>& column_index,
    SelectionMask& mask) const {
    const auto found = column_index.find(column_);
    if (found == column_index.end()) return common::E_COLUMN_NOT_EXIST;

    const uint32_t vector_index = found->second;
    if (vector_index >= block.get_column_count()) return common::E_INVALID_ARG;
    common::Vector* vector = block.get_vector(vector_index);
    if (vector == nullptr) return common::E_INVALID_ARG;

    common::ColIterator iter(vector_index, &block);
    const common::TSDataType column_type = vector->get_vector_type();
    for (uint32_t row = 0; row < block.get_row_count(); ++row) {
        uint32_t len = 0;
        bool is_null = false;
        char* data = iter.read(&len, &is_null);

        bool satisfied = false;
        if (kind_ == Kind::IS_NULL) {
            satisfied = is_null;
        } else if (kind_ == Kind::IS_NOT_NULL) {
            satisfied = !is_null;
        } else if (!is_null) {
            if (column_type == common::BOOLEAN &&
                literal_.type == common::BOOLEAN) {
                bool value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.bool_value, compare_op_);
            } else if ((column_type == common::INT32 ||
                        column_type == common::DATE) &&
                       literal_.type == common::INT32) {
                int32_t value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.int32_value, compare_op_);
            } else if ((column_type == common::INT64 ||
                        column_type == common::TIMESTAMP) &&
                       literal_.type == common::INT64) {
                int64_t value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.int64_value, compare_op_);
            } else if (column_type == common::FLOAT &&
                       literal_.type == common::FLOAT) {
                float value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.float_value, compare_op_);
            } else if (column_type == common::DOUBLE &&
                       literal_.type == common::DOUBLE) {
                double value;
                std::memcpy(&value, data, sizeof(value));
                satisfied =
                    compare_values(value, literal_.double_value, compare_op_);
            } else if (is_string_type(column_type) &&
                       is_string_type(literal_.type)) {
                satisfied = compare_string(data, len, literal_.string_value,
                                           compare_op_);
            } else {
                return common::E_NOT_SUPPORT;
            }
        }
        if (satisfied) mask.set_one(row);
        iter.next();
    }
    return common::E_OK;
}

}  // namespace storage
