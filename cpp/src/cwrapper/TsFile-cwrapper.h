//
// Created by 18367 on 2024/4/25.
// Author: Colin lee
//
#ifndef CWRAPPER_TSFILE_CWRAPPER_H
#define CWRAPPER_TSFILE_CWRAPPER_H

#include <fcntl.h>
#include <stddef.h>
#include <stdint.h>

typedef long long SchemaInfo;
typedef int ErrCode;
typedef long long timestamp;
typedef void* CTsFileReader;
typedef void* CTsFileWriter;
typedef void* TsFileRowData;
typedef int ErrorCode;
typedef void* TimeFilterExpression;

// DATA TYPE
#define TS_TYPE_INT32 1 << 8
#define TS_TYPE_BOOLEAN 1 << 9
#define TS_TYPE_FLOAT 1 << 10
#define TS_TYPE_DOUBLE 1 << 11
#define TS_TYPE_INT64 1 << 12
#define TS_TYPE_TEXT 1 << 13

// ENCODING TYPE
#define TS_ENCODING_PLAIN 1 << 16
#define TS_ENCODING_TS_DIFF 1 << 17
#define TS_ENCODING_DICTIONARY 1 << 18
#define TS_ENCODING_RLE 1 << 19
#define TS_ENCODING_BITMAP 1 << 20
#define TS_ENCODING_GORILLA_V1 1 << 21
#define TS_ENCODING_REGULAR 1 << 22
#define TS_ENCODING_GORILLA 1 << 23
#define TS_ENCODING_ZIGZAG 1 << 24
#define TS_ENCODING_FREQ 1 << 25

// COMPRESS TYPE
#define TS_COMPRESS_UNCOMPRESS 1LL << 32
#define TS_COMPRESS_SNAPPY 1LL << 33
#define TS_COMPRESS_GZIP 1LL << 34
#define TS_COMPRESS_LZO 1LL << 35
#define TS_COMPRESS_SDT 1LL << 36
#define TS_COMPRESS_PAA 1LL << 37
#define TS_COMPRESS_PLA 1LL << 38
#define TS_COMPRESS_LZ4 1LL << 39

#define MAX_COLUMN_FILTER_NUM 10

typedef struct column_schema {
    char* name;
    SchemaInfo column_def;
} ColumnSchema;

typedef struct table_shcema {
    char* table_name;
    ColumnSchema** column_schema;
    int column_num;
} TableSchema;

typedef enum operator_type {
    LT,
    LE,
    EQ,
    GT,
    GE,
    NOTEQ,
} OperatorType;

typedef enum expression_type {
    OR,
    AND,
    GLOBALTIME,
} ExpressionType;

typedef struct constant {
    int64_t value_condition;
    int type;
} Constant;

typedef struct expression {
    const char* column_name;
    Constant const_condition;
    ExpressionType expression_type;
    OperatorType operatype;
    struct expression* children[MAX_COLUMN_FILTER_NUM];
    int children_length;
} Expression;

typedef struct tablet {
    char* table_name;
    ColumnSchema** column_schema;
    int column_num;
    timestamp* times;
    bool** bitmap;
    void** value;
    int cur_num;
    int max_capacity;
} Tablet;

typedef struct tsfile_conf {
    int mem_threshold_kb;
} TsFileConf;

typedef Tablet DataResult;

typedef void* QueryDataRetINTERNAL;
typedef struct query_data_ret {
    char** column_names;
    QueryDataRetINTERNAL data;
} * QueryDataRet;

#ifdef __cplusplus
extern "C" {
#endif

CTsFileReader ts_reader_open(const char* pathname, ErrCode* err_code);
CTsFileWriter ts_writer_open(const char* pathname, ErrCode* err_code);
CTsFileWriter ts_writer_open_flag(const char* pathname, mode_t flag,
                                  ErrCode* err_code);
CTsFileWriter ts_writer_open_conf(const char* pathname, mode_t flag,
                                  ErrCode* err_code, TsFileConf* conf);

ErrorCode ts_writer_close(CTsFileWriter writer);
ErrorCode ts_reader_colse(CTsFileReader reader);

ErrorCode tsfile_register_table_column(CTsFileWriter writer,
                                       const char* table_name,
                                       ColumnSchema* schema);
ErrorCode tsfile_register_table(CTsFileWriter writer,
                                TableSchema* table_shcema);

TsFileRowData create_tsfile_row(const char* tablename, int64_t timestamp,
                                int column_length);

ErrorCode insert_data_into_tsfile_row_int32(TsFileRowData data, char* columname,
                                            int32_t value);
ErrorCode insert_data_into_tsfile_row_boolean(TsFileRowData data,
                                              char* columname, bool value);
ErrorCode insert_data_into_tsfile_row_int64(TsFileRowData data, char* columname,
                                            int64_t value);
ErrorCode insert_data_into_tsfile_row_float(TsFileRowData data, char* columname,
                                            float value);
ErrorCode insert_data_into_tsfile_row_double(TsFileRowData data,
                                             char* columname, double value);

ErrorCode tsfile_write_row_data(CTsFileWriter writer, TsFileRowData data);
ErrorCode destory_tsfile_row(TsFileRowData data);

Tablet* create_tablet(const char* table_name, int max_capacity);
Tablet* add_column_to_tablet(Tablet* tablet, char* column_name,
                             SchemaInfo column_def);
Tablet add_data_to_tablet(Tablet tablet, int line_id, int64_t timestamp,
                          const char* column_name, int64_t value);

ErrorCode destory_tablet(Tablet* tablet);

ErrorCode tsfile_flush_data(CTsFileWriter writer);

Expression create_column_filter_I32(const char* column_name, OperatorType oper,
                                    int32_t int32_value);
Expression create_column_filter_I64(const char* column_name, OperatorType oper,
                                    int64_t int64_value);
Expression create_column_filter_bval(const char* column_name, OperatorType oper,
                                     bool bool_value);
Expression create_column_filter_fval(const char* column_name, OperatorType oper,
                                     float float_value);
Expression create_column_filter_dval(const char* column_name, OperatorType oper,
                                     double double_value);
Expression create_column_filter_cval(const char* column_name, OperatorType oper,
                                     const char* char_value);

TimeFilterExpression* create_andquery_timefilter();

TimeFilterExpression* create_time_filter(const char* table_name,
                                         const char* column_name,
                                         OperatorType oper, int64_t timestamp);

TimeFilterExpression* add_time_filter_to_and_query(
    TimeFilterExpression* exp_and, TimeFilterExpression* exp);

void destory_time_filter_query(TimeFilterExpression* expression);

Expression* create_time_expression(const char* column_name, OperatorType oper,
                                   int64_t timestamp);

Expression* add_and_filter_to_and_query(Expression* exp_and, Expression* exp);

QueryDataRet ts_reader_query(CTsFileReader reader, const char* table_name,
                             const char** columns, int colum_num,
                             TimeFilterExpression* expression);
ErrorCode destory_query_dataret(QueryDataRet query_data_set);

DataResult* ts_next(QueryDataRet data, int expect_line_count);

void print_data_result(DataResult* result);

void clean_data_record(DataResult data_result);
void clean_query_ret(QueryDataRet query_data_set);
void clean_query_tree(Expression* expression);

#ifdef __cplusplus
}
#endif
#endif  // CWRAPPER_TSFILE_CWRAPPER_H
