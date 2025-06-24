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
//
// #ifndef CWRAPPER_TSFILE_CWRAPPER_H
// #define CWRAPPER_TSFILE_CWRAPPER_H
//
// #include <fcntl.h>
// #include <stdbool.h>
// #include <stddef.h>
// #include <stdint.h>
// #ifdef _WIN32
// #include <sys/stat.h>
// #endif
//
// #include "tsfile_cwrapper.h"
//
// typedef void* TimeFilterExpression;
//
// #define MAX_COLUMN_FILTER_NUM 10
// typedef enum operator_type {
//    LT,
//    LE,
//    EQ,
//    GT,
//    GE,
//    NOTEQ,
//} OperatorType;
//
// typedef enum expression_type {
//    OR,
//    AND,
//    GLOBALTIME,
//} ExpressionType;
//
// typedef struct constant {
//    int64_t value_condition;
//    int type;
//} Constant;
//
// typedef struct expression {
//    const char* column_name;
//    Constant const_condition;
//    ExpressionType expression_type;
//    OperatorType operate_type;
//    struct expression* children[MAX_COLUMN_FILTER_NUM];
//    int children_length;
//} Expression;
//
// typedef void* QueryDataRetINTERNAL;
// typedef struct query_data_ret {
//    char** column_names;
//    int column_num;
//    QueryDataRetINTERNAL data;
//}* QueryDataRet;
//
// #ifdef __cplusplus
// extern "C" {
// #endif
//
// TimeFilterExpression* create_query_and_time_filter();
//
// TimeFilterExpression* create_time_filter(const char* table_name,
//                                         const char* column_name,
//                                         OperatorType oper,
//                                         timestamp timestamp);
//
// TimeFilterExpression* add_time_filter_to_and_query(
//    TimeFilterExpression* exp_and, TimeFilterExpression* exp);
//
// void destroy_time_filter_query(TimeFilterExpression* expression);
//
// Expression* create_time_expression(const char* column_name, OperatorType
// oper,
//                                   timestamp timestamp);
//
// Expression* add_and_filter_to_and_query(Expression* exp_and, Expression*
// exp);
//
// QueryDataRet ts_reader_query(TsFileReader reader, const char* table_name,
//                             const char** columns, int colum_num,
//                             TimeFilterExpression* expression);
//
//
// #ifdef __cplusplus
//}
// #endif
// #endif  // CWRAPPER_TSFILE_CWRAPPER_H
