#ifndef STORAGE_TSFILE_READ_EXPRESSION_QUERY_EXPRESSION_H
#define STORAGE_TSFILE_READ_EXPRESSION_QUERY_EXPRESSION_H

#include <string>
#include <vector>
#include "common/db_common.h"
#include "path.h"

namespace timecho
{
namespace storage
{

class Filter;

enum ExpressionType
{
  AND_EXPR,
  OR_EXPR,
  SERIES_EXPR,
  GLOBALTIME_EXPR,
  INVALID_EXPR
};

struct Expression
{
  ExpressionType type_;
  Expression *left_;
  Expression *right_;
  Filter *filter_;
  Path series_path_;

  Expression(ExpressionType t)
    : type_(t),
      left_(nullptr),
      right_(nullptr),
      filter_(nullptr),
      series_path_() {}
  Expression(ExpressionType t,
             Expression *l,
             Expression *r)
    : type_(t),
      left_(l),
      right_(r),
      filter_(nullptr),
      series_path_() {}
  Expression(ExpressionType t, Filter *f)
    : type_(t),
      left_(nullptr),
      right_(nullptr),
      filter_(f),
      series_path_() {}
  Expression(ExpressionType t, const Path &path, Filter *f)
    : type_(t),
      left_(nullptr),
      right_(nullptr),
      filter_(f),
      series_path_(path) {}
};

class QueryExpression
{
public:
  QueryExpression() : has_filter_(false), expression_(nullptr) {}
  ~QueryExpression() { destory(); }

  static QueryExpression* create(const std::vector<Path> &selected_series,
                                 Expression *expression)
  {
    QueryExpression *ret = new QueryExpression();
    ret->selected_series_ = selected_series;
    ret->expression_ = expression;
    ret->has_filter_ = (expression != nullptr);
    return ret;   
  }
  static void destory(QueryExpression *expr)
  {
    if (expr) {
      delete expr;
    }
  }
  FORCE_INLINE void set_select_series(const std::vector<Path> &selected_series)
  {
    selected_series_ = selected_series;
  }
  FORCE_INLINE void add_select_series(const Path &path)
  {
    selected_series_.push_back(path);
  }
  FORCE_INLINE void set_expression(Expression *expression)
  {
    if (expression) {
      expression_ = expression;
      has_filter_ = true;
    }
  }
  Expression* optimize(Expression *expression, std::vector<Path> &series_paths);
  void destory();

private:
  Expression* combine_two_global_time_filter(Expression *left,
                                             Expression *right,
                                             ExpressionType type);
  Expression* handle_one_global_time_filter(Expression *left,
                                            Expression *expression,
                                            std::vector<Path> &selected_series,
                                            ExpressionType type);
  void add_time_filter_to_query_filter(Filter *time_filter,
                                       Expression *expression);
  void add_time_filter_to_series_filter(Filter *time_filter,
                                        Expression *single_series_expr);
  Expression* push_global_time_filter_to_all_series(Expression *time_filter,
                                                    std::vector<Path> &selected_series);
  Expression* merge_second_tree_to_first_tree(Expression *left_expression,
                                               Expression *right_expression);
  bool update_filter_with_or(Expression *expression,
                             Filter *filter,
                             Path &path);

public:
  bool has_filter_;
  std::vector<Path> selected_series_;
  std::vector<common::TSDataType> data_types_;
  Expression *expression_;
  std::vector<Expression*> my_exprs_;
  std::vector<Filter*> my_filters_;
};

}  // storage
}  // timecho

#endif  // STORAGE_TSFILE_READ_EXPRESSION_QUERY_EXPRESSION_H
