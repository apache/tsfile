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

#include "cwrapper/TsFile-cwrapper.h"

#include <iomanip>

#include "common/global.h"
#include "reader/expression.h"
#include "reader/filter/and_filter.h"
#include "reader/filter/filter.h"
#include "reader/filter/time_filter.h"
#include "reader/filter/time_operator.h"
#include "reader/query_data_set.h"
#include "reader/tsfile_reader.h"
#include "utils/errno_define.h"
#include "writer/tsfile_writer.h"

static bool is_init = false;

#define INSERT_DATA_INTO_RECORD(record, column, value)               \
    do {                                                             \
        DataPoint point(column, value);                              \
        if (record->points_.size() + 1 > record->points_.capacity()) \
            return E_BUF_NOT_ENOUGH;                                 \
        record->points_.push_back(point);                            \
        return E_OK;                                                 \
    } while (0)

#define CONSTRUCT_EXP_INTERNAL(exp, column_name) \
    do {                                         \
        exp.column_name = column_name;           \
        exp.operatype = oper;                    \
        exp.children_length = 0;                 \
    } while (0)

#define INSERT_DATA_TABLET_STEP                                             \
    do {                                                                    \
        for (int i = 0; i < tablet->column_num; i++) {                      \
            if (strcmp(tablet->column_schema[i]->name, column_name) == 0) { \
                column_id = i;                                              \
                break;                                                      \
            }                                                               \
        }                                                                   \
        if (column_id == -1) {                                              \
            return tablet;                                                  \
        }                                                                   \
        if (tablet->cur_num + 1 > tablet->max_capacity) {                   \
            return tablet;                                                  \
        }                                                                   \
        tablet->times[line_id] = timestamp;                                 \
    } while (0)
#define TSDataType common::TSDataType
#define TSEncoding common::TSEncoding
#define CompressionType common::CompressionType
#define TsFileReader storage::TsFileReader
#define TsFileWriter storage::TsFileWriter
#define E_OK common::E_OK
#define TsRecord storage::TsRecord
#define DataPoint storage::DataPoint
#define E_BUF_NOT_ENOUGH common::E_BUF_NOT_ENOUGH

TSDataType get_datatype(SchemaInfo schema_info) {
    if (schema_info & TS_TYPE_BOOLEAN) {
        return TSDataType::BOOLEAN;
    } else if (schema_info & TS_TYPE_DOUBLE) {
        return TSDataType::DOUBLE;
    } else if (schema_info & TS_TYPE_FLOAT) {
        return TSDataType::FLOAT;
    } else if (schema_info & TS_TYPE_INT32) {
        return TSDataType::INT32;
    } else if (schema_info & TS_TYPE_INT64) {
        return TSDataType::INT64;
    } else if (schema_info & TS_TYPE_TEXT) {
        return TSDataType::TEXT;
    }
    return TSDataType::INVALID_DATATYPE;
}

TSEncoding get_data_encoding(SchemaInfo schema_info) {
    if (schema_info & TS_ENCODING_PLAIN) {
        return TSEncoding::PLAIN;
    } else if (schema_info & TS_ENCODING_TS_DIFF) {
        return TSEncoding::DIFF;
    } else if (schema_info & TS_ENCODING_BITMAP) {
        return TSEncoding::BITMAP;
    } else if (schema_info & TS_ENCODING_GORILLA) {
        return TSEncoding::GORILLA;
    }
    return TSEncoding::PLAIN;
}

CompressionType get_data_compression(SchemaInfo schema_info) {
    if (schema_info & TS_COMPRESS_UNCOMPRESS) {
        return CompressionType::UNCOMPRESSED;
    } else if (schema_info & TS_COMPRESS_LZ4) {
        return CompressionType::LZ4;
    }
    return CompressionType::UNCOMPRESSED;
}

SchemaInfo get_schema_info(TSDataType type) {
    switch (type) {
        case TSDataType::BOOLEAN:
            return TS_TYPE_BOOLEAN;
        case TSDataType::DOUBLE:
            return TS_TYPE_DOUBLE;
        case TSDataType::FLOAT:
            return TS_TYPE_FLOAT;
        case TSDataType::INT32:
            return TS_TYPE_INT32;
        case TSDataType::INT64:
            return TS_TYPE_INT64;
        case TSDataType::TEXT:
            return TS_TYPE_TEXT;
        default:
            return 0;
    }
}

void init_tsfile_config() {
    if (!is_init) {
        common::init_config_value();
        is_init = true;
    }
}

CTsFileReader ts_reader_open(const char* pathname, ErrorCode* err_code) {
    init_tsfile_config();
    TsFileReader* reader = new TsFileReader();
    int ret = reader->open(pathname);
    if (ret != E_OK) {
        std::cout << "open file failed" << std::endl;
        *err_code = ret;
        delete reader;
        return nullptr;
    }
    return reader;
}

CTsFileWriter ts_writer_open(const char* pathname, ErrorCode* err_code) {
    init_tsfile_config();
    TsFileWriter* writer = new TsFileWriter();
    int flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef _WIN32
    flags |= O_BINARY;
#endif
    int ret = writer->open(pathname, flags, 0644);
    if (ret != E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

CTsFileWriter ts_writer_open_flag(const char* pathname, mode_t flag,
                                  ErrorCode* err_code) {
    init_tsfile_config();
    TsFileWriter* writer = new TsFileWriter();
    int ret = writer->open(pathname, O_CREAT | O_RDWR, flag);
    if (ret != E_OK) {
        delete writer;
        *err_code = ret;
        return nullptr;
    }
    return writer;
}

CTsFileWriter ts_writer_open_conf(const char* pathname, int flag,
                                  ErrorCode* err_code, TsFileConf* conf) {
    *err_code = common::E_INVALID_ARG;
    return nullptr;
}

ErrorCode ts_writer_close(CTsFileWriter writer) {
    TsFileWriter* w = (TsFileWriter*)writer;
    int ret = w->close();
    delete w;
    return ret;
}

ErrorCode ts_reader_close(CTsFileReader reader) {
    TsFileReader* ts_reader = (TsFileReader*)reader;
    delete ts_reader;
    return E_OK;
}

ErrorCode tsfile_register_table_column(CTsFileWriter writer,
                                       const char* table_name,
                                       ColumnSchema* schema) {
    TsFileWriter* w = (TsFileWriter*)writer;
    int ret = w->register_timeseries(table_name, schema->name,
                                     get_datatype(schema->column_def),
                                     get_data_encoding(schema->column_def),
                                     get_data_compression(schema->column_def));
    return ret;
}

ErrorCode tsfile_register_table(CTsFileWriter writer,
                                TableSchema* table_schema) {
    TsFileWriter* w = (TsFileWriter*)writer;
    for (int column_id = 0; column_id < table_schema->column_num; column_id++) {
        ColumnSchema* schema = table_schema->column_schema[column_id];
        ErrorCode ret =
            w->register_timeseries(table_schema->table_name, schema->name,
                                   get_datatype(schema->column_def),
                                   get_data_encoding(schema->column_def),
                                   get_data_compression(schema->column_def));
        if (ret != E_OK) {
            return ret;
        }
    }
    return E_OK;
}

TsFileRowData create_tsfile_row(const char* table_name, int64_t timestamp,
                                int column_length) {
    TsRecord* record = new TsRecord(timestamp, table_name, column_length);
    return record;
}

Tablet* create_tablet(const char* table_name, int max_capacity) {
    Tablet* tablet = new Tablet();
    tablet->table_name = strdup(table_name);
    tablet->max_capacity = max_capacity;
    tablet->times = (timestamp*)malloc(max_capacity * sizeof(int64_t));
    return tablet;
}

int get_size_from_schema_info(SchemaInfo schema_info) {
    if (schema_info & TS_TYPE_BOOLEAN) {
        return sizeof(bool);
    } else if (schema_info & TS_TYPE_DOUBLE) {
        return sizeof(double);
    } else if (schema_info & TS_TYPE_FLOAT) {
        return sizeof(float);
    } else if (schema_info & TS_TYPE_INT32) {
        return sizeof(int32_t);
    } else if (schema_info & TS_TYPE_INT64) {
        return sizeof(int64_t);
    } else if (schema_info & TS_TYPE_TEXT) {
        return sizeof(char*);
    }
    return 0;
}

Tablet* add_column_to_tablet(Tablet* tablet, char* column_name,
                             SchemaInfo column_def) {
    tablet->column_num++;
    tablet->column_schema = (ColumnSchema**)realloc(
        tablet->column_schema, tablet->column_num * sizeof(ColumnSchema*));
    tablet->bitmap =
        (bool**)realloc(tablet->bitmap, tablet->column_num * sizeof(bool*));
    tablet->bitmap[tablet->column_num - 1] =
        (bool*)malloc(tablet->max_capacity * sizeof(bool));
    std::memset(tablet->bitmap[tablet->column_num - 1], 0,
                tablet->max_capacity * sizeof(bool));
    ColumnSchema* schema = new ColumnSchema();
    schema->name = column_name;
    schema->column_def = column_def;
    tablet->column_schema[tablet->column_num - 1] = schema;
    tablet->value =
        (void**)realloc(tablet->value, tablet->column_num * sizeof(void*));
    tablet->value[tablet->column_num - 1] =
        (void*)malloc(tablet->max_capacity * sizeof(int64_t));
    return tablet;
}

Tablet* add_data_to_tablet_i64(Tablet* tablet, int line_id, int64_t timestamp,
                               const char* column_name, int64_t value) {
    int column_id = -1;
    INSERT_DATA_TABLET_STEP;
    memcpy((int64_t*)tablet->value[column_id] + line_id, &value,
           sizeof(int64_t));
    tablet->bitmap[column_id][line_id] = true;
    line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
    return tablet;
}

Tablet* add_data_to_tablet_i32(Tablet* tablet, int line_id, int64_t timestamp,
                               const char* column_name, int32_t value) {
    int column_id = -1;
    INSERT_DATA_TABLET_STEP;
    memcpy((int32_t*)tablet->value[column_id] + line_id, &value,
           sizeof(int32_t));
    tablet->bitmap[column_id][line_id] = true;
    line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
    return tablet;
}

Tablet* add_data_to_tablet_float(Tablet* tablet, int line_id, int64_t timestamp,
                                 const char* column_name, float value) {
    int column_id = -1;
    INSERT_DATA_TABLET_STEP;
    memcpy((float*)tablet->value[column_id] + line_id, &value, sizeof(float));
    tablet->bitmap[column_id][line_id] = true;
    line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
    return tablet;
}

Tablet* add_data_to_tablet_double(Tablet* tablet, int line_id,
                                  int64_t timestamp, const char* column_name,
                                  double value) {
    int column_id = -1;
    INSERT_DATA_TABLET_STEP;
    memcpy((double*)tablet->value[column_id] + line_id, &value, sizeof(double));
    tablet->bitmap[column_id][line_id] = true;
    line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
    return tablet;
}

Tablet* add_data_to_tablet_bool(Tablet* tablet, int line_id, int64_t timestamp,
                                const char* column_name, bool value) {
    int column_id = -1;
    INSERT_DATA_TABLET_STEP;
    memcpy((bool*)tablet->value[column_id] + line_id, &value, sizeof(bool));
    tablet->bitmap[column_id][line_id] = true;
    line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
    return tablet;
}

Tablet* add_data_to_tablet_char(Tablet* tablet, int line_id, int64_t timestamp,
                                const char* column_name, char* value) {
    int column_id = -1;
    INSERT_DATA_TABLET_STEP;
    memcpy((char*)tablet->value[column_id] + line_id, &value, sizeof(char*));
    tablet->bitmap[column_id][line_id] = true;
    line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
    return tablet;
}

// Tablet* add_null_to_tablet(Tablet* tablet, int line_id, int64_t timestamp,
// const char* column_num) {
//     int column_id = -1;
//     for (int i = 0; i < tablet->column_num; i++) {
//         if (strcmp(tablet->column_schema[i]->name, column_num) == 0) {
//             column_id = i;
//             break;
//         }
//     }
//     if (column_id == -1) {
//         return tablet;
//     }

//     if (tablet->cur_num + 1 > tablet->max_capacity) {
//         return tablet;
//     }
//     tablet->times[line_id] = timestamp;
//     memcpy((int64_t*)tablet->value[column_id] + line_id, 0, sizeof(int64_t));
//     line_id > tablet->cur_num ? tablet->cur_num = line_id : 0;
//     return tablet;
// }

ErrorCode destory_tablet(Tablet* tablet) {
    free(tablet->table_name);
    tablet->table_name = nullptr;
    free(tablet->times);
    tablet->times = nullptr;
    for (int i = 0; i < tablet->column_num; i++) {
        free(tablet->column_schema[i]);
        free(tablet->value[i]);
        free(tablet->bitmap[i]);
    }
    free(tablet->bitmap);
    free(tablet->column_schema);
    free(tablet->value);
    delete tablet;
    return E_OK;
}

ErrorCode insert_data_into_tsfile_row_int32(TsFileRowData data, char* columname,
                                            int32_t value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ErrorCode insert_data_into_tsfile_row_boolean(TsFileRowData data,
                                              char* columname, bool value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ErrorCode insert_data_into_tsfile_row_int64(TsFileRowData data, char* columname,
                                            int64_t value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ErrorCode insert_data_into_tsfile_row_float(TsFileRowData data, char* columname,
                                            float value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ErrorCode insert_data_into_tsfile_row_double(TsFileRowData data,
                                             char* columname, double value) {
    TsRecord* record = (TsRecord*)data;
    INSERT_DATA_INTO_RECORD(record, columname, value);
}

ErrorCode tsfile_write_row_data(CTsFileWriter writer, TsFileRowData data) {
    TsFileWriter* w = (TsFileWriter*)writer;
    TsRecord* record = (TsRecord*)data;
    int ret = w->write_record(*record);
    if (ret == E_OK) {
        delete record;
    }
    return ret;
}

ErrorCode destory_tsfile_row(TsFileRowData data) {
    TsRecord* record = (TsRecord*)data;
    if (record != nullptr) {
        delete record;
        record = nullptr;
    }
    return E_OK;
}

ErrorCode tsfile_flush_data(CTsFileWriter writer) {
    TsFileWriter* w = (TsFileWriter*)writer;
    int ret = w->flush();
    return ret;
}

Expression create_column_filter(const char* column_name, OperatorType oper,
                                int32_t int32_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = int32_value;
    exp.const_condition.type = TS_TYPE_INT32;
    return exp;
}

Expression create_column_filter(const char* column_name, OperatorType oper,
                                int64_t int64_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = int64_value;
    exp.const_condition.type = TS_TYPE_INT64;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                bool bool_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = bool_value ? 1 : 0;
    exp.const_condition.type = TS_TYPE_BOOLEAN;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                float float_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    memcpy(&exp.const_condition.value_condition, &float_value, sizeof(float));
    exp.const_condition.type = TS_TYPE_FLOAT;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                double double_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = double_value;
    exp.const_condition.type = TS_TYPE_DOUBLE;
    return exp;
}
Expression create_column_filter(const char* column_name, OperatorType oper,
                                const char* char_value) {
    Expression exp;
    CONSTRUCT_EXP_INTERNAL(exp, column_name);
    exp.const_condition.value_condition = reinterpret_cast<int64_t>(char_value);
    exp.const_condition.type = TS_TYPE_TEXT;
    return exp;
}

TimeFilterExpression* create_andquery_timefilter() {
    storage::Expression* exp = new storage::Expression(storage::AND_EXPR);
    return (TimeFilterExpression*)exp;
}

TimeFilterExpression* create_time_filter(const char* table_name,
                                         const char* column_name,
                                         OperatorType oper, int64_t timestamp) {
    std::string table_name_str(table_name);
    std::string column_name_str(column_name);
    storage::Path path(table_name_str, column_name_str);
    storage::Filter* filter;
    switch (oper) {
        case GT:
            filter = storage::TimeFilter::gt(timestamp);
            break;
        case LT:
            filter = storage::TimeFilter::lt(timestamp);
            break;
        case EQ:
            filter = storage::TimeFilter::eq(timestamp);
            break;
        case NOTEQ:
            filter = storage::TimeFilter::not_eqt(timestamp);
            break;
        case GE:
            filter = storage::TimeFilter::gt_eq(timestamp);
            break;
        case LE:
            filter = storage::TimeFilter::lt_eq(timestamp);
            break;
        default:
            filter = nullptr;
            break;
    }
    storage::Expression* exp =
        new storage::Expression(storage::SERIES_EXPR, path, filter);
    return (TimeFilterExpression*)exp;
}

TimeFilterExpression* add_time_filter_to_and_query(
    TimeFilterExpression* exp_and, TimeFilterExpression* exp) {
    storage::Expression* and_exp = (storage::Expression*)exp_and;
    storage::Expression* time_exp = (storage::Expression*)exp;
    if (and_exp->left_ == nullptr) {
        and_exp->left_ = time_exp;
    } else if (and_exp->right_ == nullptr) {
        and_exp->right_ = time_exp;
    } else {
        storage::Expression* new_exp =
            new storage::Expression(storage::AND_EXPR);
        new_exp->left_ = and_exp->right_;
        and_exp->right_ = new_exp;
        add_time_filter_to_and_query((TimeFilterExpression*)new_exp, exp);
    }
    return exp_and;
}

void destory_time_filter_query(TimeFilterExpression* expression) {
    if (expression == nullptr) {
        return;
    }

    destory_time_filter_query(
        (TimeFilterExpression*)((storage::Expression*)expression)->left_);
    destory_time_filter_query(
        (TimeFilterExpression*)((storage::Expression*)expression)->right_);
    storage::Expression* exp = (storage::Expression*)expression;
    if (exp->type_ == storage::ExpressionType::SERIES_EXPR) {
        delete exp->filter_;
    } else {
        delete exp;
    }
}

Expression create_global_time_expression(OperatorType oper, int64_t timestamp) {
    Expression exp;
    exp.operatype = oper;
    exp.expression_type = GLOBALTIME;
    exp.const_condition.value_condition = timestamp;
    exp.const_condition.type = TS_TYPE_INT64;
    return exp;
}

Expression* and_filter_to_and_query(Expression* exp_and, Expression* exp) {
    if (exp_and->children_length >= MAX_COLUMN_FILTER_NUM - 1) {
        return nullptr;
    }
    exp_and->children[exp_and->children_length++] = exp;
    return exp_and;
}

QueryDataRet ts_reader_query(CTsFileReader reader, const char* table_name,
                             const char** columns_name, int column_num,
                             TimeFilterExpression* expression) {
    TsFileReader* r = (TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }

    storage::QueryDataSet* qds = nullptr;
    storage::QueryExpression* query_expression =
        storage::QueryExpression::create(selected_paths,
                                         (storage::Expression*)expression);
    r->query(query_expression, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    ret->column_num = column_num;
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    storage::QueryExpression::destory(query_expression);
    return ret;
}

QueryDataRet ts_reader_begin_end(CTsFileReader reader, const char* table_name,
                                 char** columns_name, int column_num,
                                 timestamp begin, timestamp end) {
    TsFileReader* r = (TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }

    storage::QueryDataSet* qds = nullptr;
    storage::Filter* filter_low = nullptr;
    storage::Filter* filter_high = nullptr;
    storage::Expression* exp = nullptr;
    storage::Filter* and_filter = nullptr;
    if (begin != -1) {
        filter_low = storage::TimeFilter::gt_eq(begin);
    }
    if (end != -1) {
        filter_high = storage::TimeFilter::lt_eq(end);
    }
    if (filter_low != nullptr && filter_high != nullptr) {
        and_filter = new storage::AndFilter(filter_low, filter_high);
        exp = new storage::Expression(storage::GLOBALTIME_EXPR, and_filter);
    } else if (filter_low != nullptr && filter_high == nullptr) {
        exp = new storage::Expression(storage::GLOBALTIME_EXPR, filter_low);
    } else if (filter_high != nullptr && filter_low == nullptr) {
        exp = new storage::Expression(storage::GLOBALTIME_EXPR, filter_high);
    }
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(selected_paths, exp);
    r->query(query_expr, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_num = column_num;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    storage::QueryExpression::destory(query_expr);
    return ret;
}

QueryDataRet ts_reader_read(CTsFileReader reader, const char* table_name,
                            char** columns_name, int column_num) {
    TsFileReader* r = (TsFileReader*)reader;
    std::string table_name_str(table_name);
    std::vector<storage::Path> selected_paths;
    for (int i = 0; i < column_num; i++) {
        std::string column_name(columns_name[i]);
        selected_paths.push_back(storage::Path(table_name_str, column_name));
    }
    storage::QueryDataSet* qds = nullptr;
    storage::QueryExpression* query_expr =
        storage::QueryExpression::create(selected_paths, nullptr);
    r->query(query_expr, qds);
    QueryDataRet ret = (QueryDataRet)malloc(sizeof(struct query_data_ret));
    ret->data = qds;
    ret->column_names = (char**)malloc(column_num * sizeof(char*));
    ret->column_num = column_num;
    for (int i = 0; i < column_num; i++) {
        ret->column_names[i] = strdup(columns_name[i]);
    }
    storage::QueryExpression::destory(query_expr);
    return ret;
}

ErrorCode destory_query_dataret(QueryDataRet data) {
    storage::QueryDataSet* qds = (storage::QueryDataSet*)data->data;
    delete qds;
    for (int i = 0; i < data->column_num; i++) {
        free(data->column_names[i]);
    }
    free(data->column_names);
    free(data);
    return E_OK;
}

DataResult* ts_next(QueryDataRet data, int expect_line_count) {
    storage::QueryDataSet* qds = (storage::QueryDataSet*)data->data;
    DataResult* result = create_tablet("result", expect_line_count);
    storage::RowRecord* record;
    bool init_tablet = false;
    for (int i = 0; i < expect_line_count; i++) {
        record = qds->get_next();
        if (record == nullptr) {
            break;
            std::cout << "record null now"
                      << "i = " << i << std::endl;
        }
        int column_num = record->get_fields()->size();
        if (!init_tablet) {
            for (int col = 0; col < column_num; col++) {
                storage::Field* field = record->get_field(col);
                result = add_column_to_tablet(result, data->column_names[col],
                                              get_schema_info(field->type_));
            }
            init_tablet = true;
        }
        for (int col = 0; col < column_num; col++) {
            storage::Field* field = record->get_field(col);
            switch (field->type_) {
                // all data will stored as 8 bytes
                case TSDataType::BOOLEAN:
                    result = add_data_to_tablet_bool(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.bval_);
                    break;
                case TSDataType::INT32:
                    result = add_data_to_tablet_i32(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.ival_);
                    break;
                case TSDataType::INT64:
                    result = add_data_to_tablet_i64(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.lval_);
                    break;
                case TSDataType::FLOAT:
                    result = add_data_to_tablet_float(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.fval_);
                    break;
                case TSDataType::DOUBLE:
                    result = add_data_to_tablet_double(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.dval_);
                    break;
                case TSDataType::TEXT:
                    result = add_data_to_tablet_char(
                        result, i, record->get_timestamp(),
                        data->column_names[col], field->value_.sval_);
                    break;
                case TSDataType::NULL_TYPE:
                    // result = add_data_to_tablet(result, i ,
                    // record->get_timestamp(),
                    //                             data->column_names[col], 0);
                    // skip null data
                    break;
                default:
                    std::cout << field->type_ << std::endl;
                    std::cout << "error here" << std::endl;
                    return nullptr;
            }
        }
    }
    return result;
}

void print_data_result(DataResult* result) {
    std::cout << std::left << std::setw(15) << "timestamp";
    for (int i = 0; i < result->column_num; i++) {
        std::cout << std::left << std::setw(15)
                  << result->column_schema[i]->name;
    }
    std::cout << std::endl;
    for (int i = 0; i < result->cur_num; i++) {
        std::cout << std::left << std::setw(15);
        std::cout << result->times[i];
        for (int j = 0; j < result->column_num; j++) {
            ColumnSchema* schema = result->column_schema[j];
            double dval;
            float fval;
            std::cout << std::left << std::setw(15);
            switch (get_datatype(schema->column_def)) {
                case TSDataType::BOOLEAN:
                    std::cout
                        << ((*((int64_t*)result->value[j] + i)) > 0 ? "true"
                                                                    : "false");
                    break;
                case TSDataType::INT32:
                    std::cout << *((int64_t*)result->value[j] + i);
                    break;
                case TSDataType::INT64:
                    std::cout << *((int64_t*)result->value[j] + i);
                    break;
                case TSDataType::FLOAT:
                    memcpy(&fval, (int64_t*)result->value[j] + i,
                           sizeof(float));
                    std::cout << fval;
                    break;
                case TSDataType::DOUBLE:
                    memcpy(&dval, (int64_t*)result->value[j] + i,
                           sizeof(double));
                    std::cout << dval;
                    break;
                default:
                    std::cout << "";
            }
        }
        std::cout << std::endl;
    }
}

// }

// storage::Expression construct_query(Expression* exp) {
//   int column_num = exp->children_length;
//   std::vector<storage::Path> paths;
//   for (int i = 0; i < column_num; i++) {
//     Expression* exp = exp->children[i];
//     if (exp->expression_type != )
//     if (exp->column_name != nullptr ) {
//       std::string column_name = exp->column_name;

//     } else if (column->expression_type == AND) {
//       storage::Expression and_exp = construct_query(table_name,
//       column);
//       // add and_exp to the query
//     }
//     column++;
//   }
//   // construct the query using paths and other information
//   // return the constructed query
// }

// storage::Filter get_filter(int operate_type, Constant condition) {
//   switch(operate_type) {
//     case GT:
//       return storage::TimeFilter::gt();

//   }

// }

// storage::Expression construct_query(const char* table_name,
// Expression exp) {
//   std::string table = table_name;
//   int column_num = exp.children_length;
//   std::vector<storage::Path> paths;
//   paths.reserve(column_num);
//   Expression* column = exp.children;
//   for (int i = 0; i < column_num;i++) {
//     if (column_num == 1) {
//       std::string column_name = column->column_name;
//       // select_list
//       paths.push_back(storage::Path(table, column_name));
//       int operate = column->operatype;
//       Filter filter = get_filter(operate, column->const_condition);
//     }
//   }
// }
