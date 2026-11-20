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

#include "expression.h"

#include "reader/filter/and_filter.h"
#include "reader/filter/or_filter.h"

namespace storage {

namespace {

// Takes ownership of both filters and returns the combined filter, or
// nullptr after releasing them when either side is missing.
Filter* make_binary_filter(Filter* left, Filter* right, ExpressionType type) {
    if (left == nullptr || right == nullptr) {
        delete left;
        delete right;
        return nullptr;
    }
    if (type == AND_EXPR) {
        return new AndFilter(left, right);
    }
    return new OrFilter(left, right);
}

}  // namespace

bool QueryExpression::add_time_filter_to_series_filter(
    Filter* time_filter, Expression* single_series_expr) {
    if (time_filter == nullptr) {
        return false;
    }
    // Each leaf owns its own copy: a single Filter instance must not be
    // attached to several expressions, or destruction would free it twice.
    Filter* copy = time_filter->clone();
    if (copy == nullptr) {
        return false;
    }
    if (single_series_expr->filter_ == nullptr) {
        single_series_expr->filter_ = copy;
    } else {
        single_series_expr->filter_ =
            new AndFilter(single_series_expr->filter_, copy);
    }
    return true;
}

bool QueryExpression::add_time_filter_to_query_filter(Filter* time_filter,
                                                      Expression* expression) {
    if (expression->type_ == SERIES_EXPR) {
        return add_time_filter_to_series_filter(time_filter, expression);
    } else if ((expression->type_ == AND_EXPR) ||
               (expression->type_ == OR_EXPR)) {
        return add_time_filter_to_query_filter(time_filter,
                                               expression->left_) &&
               add_time_filter_to_query_filter(time_filter, expression->right_);
    }
    std::cout << "Expression should contains only SingleSeriesExpression "
                 "but other type is found"
              << std::endl;
    return false;
}

Expression* QueryExpression::combine_two_global_time_filter(
    Expression* parent, ExpressionType type) {
    Expression* left = parent->left_;
    Expression* right = parent->right_;
    parent->left_ = nullptr;
    parent->right_ = nullptr;

    Filter* combined = nullptr;
    if (left != nullptr && right != nullptr) {
        Filter* left_filter = left->filter_;
        Filter* right_filter = right->filter_;
        left->filter_ = nullptr;
        right->filter_ = nullptr;
        delete left;
        delete right;
        combined = make_binary_filter(left_filter, right_filter, type);
    }
    if (combined == nullptr) {
        std::cout << "unrecognized QueryFilterOperatorType :" << type
                  << std::endl;
        delete parent;
        return nullptr;
    }
    parent->type_ = GLOBALTIME_EXPR;
    parent->filter_ = combined;
    return parent;
}

bool QueryExpression::update_filter_with_or(Expression* expression,
                                            Filter* filter, const Path& path) {
    if (expression->type_ == SERIES_EXPR && expression->series_path_ == path) {
        if (expression->filter_ == nullptr) {
            expression->filter_ = filter;
        } else {
            expression->filter_ = new OrFilter(expression->filter_, filter);
        }
        return true;
    } else if (expression->type_ == OR_EXPR) {
        return update_filter_with_or(expression->left_, filter, path) ||
               update_filter_with_or(expression->right_, filter, path);
    }
    return false;
}

Expression* QueryExpression::merge_second_tree_to_first_tree(
    Expression* left_expression, Expression* right_expression) {
    if (right_expression == nullptr) {
        return left_expression;
    }
    if (left_expression == nullptr) {
        delete right_expression;
        return nullptr;
    }
    if (right_expression->type_ == SERIES_EXPR) {
        Filter* filter = right_expression->filter_;
        right_expression->filter_ = nullptr;
        const Path path = right_expression->series_path_;
        delete right_expression;
        if (!update_filter_with_or(left_expression, filter, path)) {
            delete filter;
        }
        return left_expression;
    } else if (right_expression->type_ == OR_EXPR) {
        Expression* right_left = right_expression->left_;
        Expression* right_right = right_expression->right_;
        right_expression->left_ = nullptr;
        right_expression->right_ = nullptr;
        delete right_expression;
        left_expression =
            merge_second_tree_to_first_tree(left_expression, right_left);
        return merge_second_tree_to_first_tree(left_expression, right_right);
    }
    return new Expression(OR_EXPR, left_expression, right_expression);
}

Expression* QueryExpression::push_global_time_filter_to_all_series(
    Filter* time_filter, std::vector<Path>& selected_series) {
    if (time_filter == nullptr || selected_series.empty()) {
        std::cout << "size of selectSeries could not be 0" << std::endl;
        return nullptr;
    }

    Filter* first_copy = time_filter->clone();
    if (first_copy == nullptr) {
        return nullptr;
    }
    Expression* expression =
        new Expression(SERIES_EXPR, selected_series.at(0), first_copy);
    for (size_t i = 1; i < selected_series.size(); i++) {
        Filter* copy = time_filter->clone();
        if (copy == nullptr) {
            delete expression;
            return nullptr;
        }
        Expression* right =
            new Expression(SERIES_EXPR, selected_series.at(i), copy);
        expression = new Expression(OR_EXPR, expression, right);
    }
    return expression;
}

Expression* QueryExpression::handle_one_global_time_filter(
    Expression* parent, Expression* global, Expression* rest,
    std::vector<Path>& selected_series, ExpressionType type) {
    // Detach the parent's children: they are consumed below and deleting the
    // parent at the end must not free them again.
    parent->left_ = nullptr;
    parent->right_ = nullptr;

    Filter* time_filter = global->filter_;
    global->filter_ = nullptr;
    delete global;

    Expression* optimized_rest = optimize(rest, selected_series);
    if (optimized_rest == nullptr) {
        delete time_filter;
        delete parent;
        return nullptr;
    }

    if (optimized_rest->type_ == GLOBALTIME_EXPR) {
        Filter* other = optimized_rest->filter_;
        optimized_rest->filter_ = nullptr;
        delete optimized_rest;
        Filter* combined = make_binary_filter(time_filter, other, type);
        if (combined == nullptr) {
            delete parent;
            return nullptr;
        }
        parent->type_ = GLOBALTIME_EXPR;
        parent->filter_ = combined;
        return parent;
    }

    if (type == AND_EXPR) {
        const bool applied =
            add_time_filter_to_query_filter(time_filter, optimized_rest);
        delete time_filter;
        delete parent;
        if (!applied) {
            delete optimized_rest;
            return nullptr;
        }
        return optimized_rest;
    } else if (type == OR_EXPR) {
        Expression* after_transform =
            push_global_time_filter_to_all_series(time_filter, selected_series);
        delete time_filter;
        delete parent;
        if (after_transform == nullptr) {
            delete optimized_rest;
            return nullptr;
        }
        return merge_second_tree_to_first_tree(after_transform, optimized_rest);
    }
    std::cout << "unknown relation in Expression:" << type << std::endl;
    delete time_filter;
    delete optimized_rest;
    delete parent;
    return nullptr;
}

// Contract: consumes @expression. The returned tree is owned by the caller;
// on failure the input tree has been released and nullptr is returned.
Expression* QueryExpression::optimize(Expression* expression,
                                      std::vector<Path>& series_paths) {
    if (expression == nullptr) {
        return nullptr;
    }
    const ExpressionType type = expression->type_;
    if (type == GLOBALTIME_EXPR || type == SERIES_EXPR) {
        return expression;
    }
    if (type != AND_EXPR && type != OR_EXPR) {
        std::cout << "unknown Expression type:" << type << std::endl;
        delete expression;
        return nullptr;
    }

    Expression* left = expression->left_;
    Expression* right = expression->right_;
    const bool left_global = left != nullptr && left->type_ == GLOBALTIME_EXPR;
    const bool right_global =
        right != nullptr && right->type_ == GLOBALTIME_EXPR;

    if (left_global && right_global) {
        return combine_two_global_time_filter(expression, type);
    } else if (left_global || right_global) {
        Expression* global = left_global ? left : right;
        Expression* rest = left_global ? right : left;
        return handle_one_global_time_filter(expression, global, rest,
                                             series_paths, type);
    }

    // Reuse this node: optimize the children and keep exactly one owner for
    // the result tree.
    expression->left_ = nullptr;
    expression->right_ = nullptr;
    Expression* regular_left = optimize(left, series_paths);
    if (regular_left == nullptr) {
        delete right;
        delete expression;
        return nullptr;
    }
    Expression* regular_right = optimize(right, series_paths);
    if (regular_right == nullptr) {
        delete regular_left;
        delete expression;
        return nullptr;
    }
    expression->left_ = regular_left;
    expression->right_ = regular_right;

    if (regular_left->type_ == GLOBALTIME_EXPR ||
        regular_right->type_ == GLOBALTIME_EXPR) {
        return optimize(expression, series_paths);
    }
    return expression;
}

void QueryExpression::destory() {
    // Kept for compatibility: optimization no longer registers detached
    // nodes/filters here, so these lists stay empty.
    for (size_t i = 0; i < my_exprs_.size(); i++) {
        delete my_exprs_[i];
        my_exprs_[i] = nullptr;
    }
    my_exprs_.clear();
    for (size_t i = 0; i < my_filters_.size(); i++) {
        delete my_filters_[i];
        my_filters_[i] = nullptr;
    }
    my_filters_.clear();
    if (expression_ != nullptr) {
        delete expression_;
        expression_ = nullptr;
    }
}

}  // namespace storage
