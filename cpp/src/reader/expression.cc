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

#include "expression.h"

#include "reader/filter/and_filter.h"
#include "reader/filter/or_filter.h"

namespace storage {

void QueryExpression::add_time_filter_to_series_filter(
    Filter *time_filter, Expression *single_series_expr) {
    Filter *filter = new AndFilter(single_series_expr->filter_, time_filter);
    single_series_expr->filter_ = filter;
    my_filters_.push_back(filter);
}

void QueryExpression::add_time_filter_to_query_filter(Filter *time_filter,
                                                      Expression *expression) {
    if (expression->type_ == SERIES_EXPR) {
        add_time_filter_to_series_filter(time_filter, expression);
    } else if ((expression->type_ == AND_EXPR) ||
               (expression->type_ == OR_EXPR)) {
        add_time_filter_to_query_filter(time_filter, expression->left_);
        add_time_filter_to_query_filter(time_filter, expression->right_);
    } else {
        std::cout << "Expression should contains only SingleSeriesExpression "
                     "but other type is found"
                  << std::endl;
    }
}

Expression *QueryExpression::combine_two_global_time_filter(
    Expression *left, Expression *right, ExpressionType type) {
    if (type == AND_EXPR) {
        Filter *filter = new AndFilter(left->filter_, right->filter_);
        Expression *expr = new Expression(GLOBALTIME_EXPR, filter);
        my_filters_.push_back(filter);
        my_exprs_.push_back(expr);
        return expr;
    } else if (type == OR_EXPR) {
        Filter *filter = new OrFilter(left->filter_, right->filter_);
        Expression *expr = new Expression(GLOBALTIME_EXPR, filter);
        my_filters_.push_back(filter);
        my_exprs_.push_back(expr);
        return expr;
    }
    std::cout << "unrecognized QueryFilterOperatorType :" << type << std::endl;
    return nullptr;
}

bool QueryExpression::update_filter_with_or(Expression *expression,
                                            Filter *filter, Path &path) {
    if (expression->type_ == SERIES_EXPR && expression->series_path_ == path) {
        Filter *node_filter = expression->filter_;
        node_filter = new OrFilter(node_filter, filter);
        my_filters_.push_back(node_filter);
        expression->filter_ = node_filter;
        return true;
    } else if (expression->type_ == OR_EXPR) {
        Expression *left = expression->left_;
        Expression *right = expression->right_;
        return update_filter_with_or(left, filter, path) ||
               update_filter_with_or(right, filter, path);
    } else {
        return false;
    }
}

Expression *QueryExpression::merge_second_tree_to_first_tree(
    Expression *left_expression, Expression *right_expression) {
    if (right_expression->type_ == SERIES_EXPR) {
        Expression *leaf = right_expression;
        update_filter_with_or(left_expression, leaf->filter_,
                              leaf->series_path_);
        return left_expression;
    } else if (right_expression->type_ == OR_EXPR) {
        Expression *left_child = right_expression->left_;
        Expression *right_child = right_expression->right_;
        left_expression =
            merge_second_tree_to_first_tree(left_expression, left_child);
        left_expression =
            merge_second_tree_to_first_tree(left_expression, right_child);
        return left_expression;
    } else {
        Expression *expr =
            new Expression(OR_EXPR, left_expression, right_expression);
        my_exprs_.push_back(expr);
        return expr;
    }
}

Expression *QueryExpression::push_global_time_filter_to_all_series(
    Expression *time_filter, std::vector<Path> &selected_series) {
    if (selected_series.size() == 0) {
        std::cout << "size of selectSeries could not be 0" << std::endl;
    }

    Expression *expression = new Expression(SERIES_EXPR, selected_series.at(0),
                                            time_filter->filter_);
    my_exprs_.push_back(expression);
    for (uint32_t i = 1; i < selected_series.size(); i++) {
        Expression *r = new Expression(SERIES_EXPR, selected_series.at(i),
                                       time_filter->filter_);
        expression = new Expression(OR_EXPR, expression, r);
        my_exprs_.push_back(r);
        my_exprs_.push_back(expression);
    }
    return expression;
}

Expression *QueryExpression::handle_one_global_time_filter(
    Expression *left, Expression *expression,
    std::vector<Path> &selected_series, ExpressionType type) {
    Expression *expr = optimize(expression, selected_series);

    if (expr->type_ == GLOBALTIME_EXPR) {
        return combine_two_global_time_filter(left, expr, type);
    }

    if (type == AND_EXPR) {
        add_time_filter_to_query_filter(left->filter_, expr);
        return expr;
    } else if (type == OR_EXPR) {
        Expression *after_transform =
            push_global_time_filter_to_all_series(left, selected_series);
        return merge_second_tree_to_first_tree(after_transform, expr);
    }
    std::cout << "unknown relation in Expression:" << type << std::endl;
    return nullptr;
}

Expression *QueryExpression::optimize(Expression *expression,
                                      std::vector<Path> &series_paths) {
    ExpressionType type = expression->type_;
    if (type == GLOBALTIME_EXPR || type == SERIES_EXPR) {
        return expression;
    } else if (type == AND_EXPR || type == OR_EXPR) {
        Expression *left = expression->left_;
        Expression *right = expression->right_;
        if (left->type_ == GLOBALTIME_EXPR && right->type_ == GLOBALTIME_EXPR) {
            return combine_two_global_time_filter(left, right, type);
        } else if (left->type_ == GLOBALTIME_EXPR &&
                   right->type_ != GLOBALTIME_EXPR) {
            return handle_one_global_time_filter(left, right, series_paths,
                                                 type);
        } else if (left->type_ != GLOBALTIME_EXPR &&
                   right->type_ == GLOBALTIME_EXPR) {
            return handle_one_global_time_filter(right, left, series_paths,
                                                 type);
        } else if (left->type_ != GLOBALTIME_EXPR &&
                   right->type_ != GLOBALTIME_EXPR) {
            Expression *regular_left = optimize(left, series_paths);
            Expression *regular_right = optimize(right, series_paths);
            Expression *mid_ret = nullptr;
            if (type == AND_EXPR) {
                mid_ret = new Expression(AND_EXPR, regular_left, regular_right);
                my_exprs_.push_back(mid_ret);
            } else if (type == OR_EXPR) {
                mid_ret = new Expression(OR_EXPR, regular_left, regular_right);
                my_exprs_.push_back(mid_ret);
            } else {
                std::cout << "unsupported Expression type:" << type
                          << std::endl;
            }

            if (mid_ret->left_->type_ == GLOBALTIME_EXPR ||
                mid_ret->right_->type_ == GLOBALTIME_EXPR) {
                return optimize(mid_ret, series_paths);
            } else {
                return mid_ret;
            }
        }
    } else {
        std::cout << "unknown Expression type:" << type << std::endl;
    }
    return nullptr;
}

void QueryExpression::destory() {
    for (size_t i = 0; i < my_exprs_.size(); i++) {
        delete my_exprs_[i];
    }
    my_exprs_.clear();
    for (size_t i = 0; i < my_filters_.size(); i++) {
        delete my_filters_[i];
    }
    my_filters_.clear();
}

}  // namespace storage
