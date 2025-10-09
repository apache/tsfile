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

#ifndef UTILS_UTILS_H
#define UTILS_UTILS_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>  // memcpy
#include <sys/time.h>

#include <iostream>
#include <sstream>
#include <string>

#include "common/allocator/my_string.h"
#include "common/db_common.h"
#include "utils/util_define.h"

namespace common {
extern TSEncoding get_value_encoder(TSDataType data_type);
extern CompressionType get_default_compressor();

typedef struct FileID {
    int64_t seq_;  // timestamp when create
    int32_t version_;
    int32_t merge_;

    FileID() : seq_(0), version_(0), merge_(0) {}
    void reset() {
        seq_ = 0;
        version_ = 0;
        merge_ = 0;
    }
    FORCE_INLINE bool is_valid() const { return seq_ != 0; }
    FORCE_INLINE bool operator<(const FileID &that) const {
        return this->seq_ < that.seq_;
    }
    FORCE_INLINE bool operator==(const FileID &that) const {
        return this->seq_ == that.seq_;
    }
#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &out, const FileID &file_id) {
        out << "{seq_=" << file_id.seq_ << ", version_=" << file_id.version_
            << ", merge_=" << file_id.merge_ << "}";
        return out;
    }
#endif
} FileID;

typedef uint16_t NodeID;
struct TsID {
    NodeID db_nid_;
    NodeID device_nid_;
    NodeID measurement_nid_;

    TsID() : db_nid_(0), device_nid_(0), measurement_nid_(0){};

    TsID(NodeID db_nid, NodeID device_nid, NodeID measurement_nid)
        : db_nid_(db_nid),
          device_nid_(device_nid),
          measurement_nid_(measurement_nid) {}

    /*
     * To make TsID to be a trival copyable struct.
     */
#if 0
  TsID(const TsID &other) : db_nid_(other.db_nid_),
                            device_nid_(other.device_nid_),
                            measurement_nid_(other.measurement_nid_) {}

  TsID & operator = (const TsID &other) 
  {
    db_nid_ = other.db_nid_;
    device_nid_ = other.device_nid_;
    measurement_nid_ = other.measurement_nid_;
    return *this;
  }
#endif

    void reset() {
        db_nid_ = 0;
        device_nid_ = 0;
        measurement_nid_ = 0;
    }

    bool is_valid() const {
        // TODO
        return true;
    }

    FORCE_INLINE bool operator==(const TsID &other) const {
        return db_nid_ == other.db_nid_ && device_nid_ == other.device_nid_ &&
               measurement_nid_ == other.measurement_nid_;
    }
    FORCE_INLINE bool operator!=(const TsID &other) const {
        return db_nid_ != other.db_nid_ || device_nid_ != other.device_nid_ ||
               measurement_nid_ != other.measurement_nid_;
    }

    FORCE_INLINE int64_t to_int64() const {
        int64_t res = db_nid_;
        res = (res << 16) | device_nid_;
        res = (res << 16) | measurement_nid_;
        return res;
    }

    FORCE_INLINE bool operator<(const TsID &that) const {
        return to_int64() < that.to_int64();
    }

    FORCE_INLINE bool operator>(const TsID &other) {
        return to_int64() > other.to_int64();
    }

    friend std::ostream &operator<<(std::ostream &out, TsID &ti) {
        out << "(" << ti.db_nid_ << ", " << ti.device_nid_ << ", "
            << ti.measurement_nid_ << ")  ";
        return out;
    }

    FORCE_INLINE void to_string(char *print_buf, int len) const {
        snprintf(print_buf, len, "<%d,%d,%d>", db_nid_, device_nid_,
                 measurement_nid_);
    }
    FORCE_INLINE std::string to_string() const {
        const int buf_len = 32;
        char buf[buf_len];
        snprintf(buf, buf_len, "<%d,%d,%d>", db_nid_, device_nid_,
                 measurement_nid_);
        // construct std::string will invoke memory allocation and copy.
        // try to use first to_string instead.
        return std::string(buf);
    }
};

/**
 * @brief Represents the schema information for a single measurement.
 * @brief Represents the category of a column in a table schema.
 *
 * This enumeration class defines the supported categories for columns within a
 * table schema, distinguishing between tag and field columns.
 */
enum class ColumnCategory { TAG = 0, FIELD = 1 };

/**
 * @brief Represents the schema information for a single column.
 *
 * This structure holds the metadata necessary to describe how a specific column
 * is stored, including its name, data type, category.
 */
struct ColumnSchema {
    std::string column_name_;
    TSDataType data_type_;
    CompressionType compression_;
    TSEncoding encoding_;
    ColumnCategory column_category_;

    ColumnSchema()
        : column_name_(""),
          data_type_(INVALID_DATATYPE),
          compression_(UNCOMPRESSED),
          encoding_(PLAIN) {}

    /**
     * @brief Constructs a ColumnSchema object with the given parameters.
     *
     * @param column_name The name of the column. Must be a non-empty string.
     *                    This name is used to identify the column within the
     * table.
     * @param data_type The data type of the measurement, such as INT32, DOUBLE,
     * TEXT, etc. This determines how the data will be stored and interpreted.
     * @param column_category The category of the column indicating its role or
     * type within the schema, e.g., FIELD, TAG. Defaults to
     * ColumnCategory::FIELD if not specified.
     * @note It is the responsibility of the caller to ensure that `column_name`
     * is not empty.
     */
    ColumnSchema(std::string column_name, TSDataType data_type,
                 CompressionType compression, TSEncoding encoding,
                 ColumnCategory column_category = ColumnCategory::FIELD)
        : column_name_(std::move(column_name)),
          data_type_(data_type),
          compression_(compression),
          encoding_(encoding),
          column_category_(column_category) {}

    ColumnSchema(std::string column_name, TSDataType data_type,
                 ColumnCategory column_category = ColumnCategory::FIELD)
        : column_name_(std::move(column_name)),
          data_type_(data_type),
          compression_(get_default_compressor()),
          encoding_(get_value_encoder(data_type)),
          column_category_(column_category) {}

    const std::string &get_column_name() const { return column_name_; }
    const TSDataType &get_data_type() const { return data_type_; }
    const ColumnCategory &get_column_category() const {
        return column_category_;
    }
    const CompressionType &get_compression() const { return compression_; }
    const TSEncoding &get_encoding() const { return encoding_; }
    bool operator==(const ColumnSchema &other) const {
        return (data_type_ == other.data_type_ &&
                encoding_ == other.encoding_ &&
                compression_ == other.compression_ &&
                column_name_ == other.column_name_);
    }

    bool operator!=(const ColumnSchema &other) const {
        return (data_type_ != other.data_type_ ||
                encoding_ != other.encoding_ ||
                compression_ != other.compression_ ||
                column_name_ != other.column_name_);
    }

    bool is_valid() const {
        return data_type_ != INVALID_DATATYPE &&
               encoding_ != INVALID_ENCODING &&
               compression_ != INVALID_COMPRESSION;
    }

    void reset() {
        // TODO
    }

    void get_device_name(char *ret_device_name_buf, const int buf_len,
                         uint32_t &ret_len) const {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0 && pos < buf_len);
        memcpy(ret_device_name_buf, column_name_.c_str(), pos);
        ret_device_name_buf[pos] = '\0';
        ret_len = pos;
    }
    std::string get_device_name_str() const {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0);
        return column_name_.substr(0, pos);
    }
    void get_device_name(String &device_name) const {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0);
        const char *c_string = column_name_.c_str();
        device_name.buf_ = (char *)c_string;
        device_name.len_ = pos;
    }
    void get_measurement_name(char *ret_measurement_name_buf, const int buf_len,
                              uint32_t &ret_len) const {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0 && pos < buf_len);
        ret_len = column_name_.size() - pos - 1;
        memcpy(ret_measurement_name_buf, column_name_.c_str() + pos + 1,
               ret_len);
        ret_measurement_name_buf[ret_len] = '\0';
    }
    std::string get_measurement_name_str() const {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0);
        return column_name_.substr(pos + 1, column_name_.size() - pos);
    }
    // TODO remove
    void get_measurement_name(String &measurement_name) const {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0);
        const char *c_string = column_name_.c_str();
        measurement_name.buf_ = (char *)c_string + pos + 1;
        measurement_name.len_ = column_name_.size() - pos - 1;
    }
    String get_measurement_name() {
        int pos = column_name_.find_last_of('.');
        ASSERT(pos > 0);
        const char *c_string = column_name_.c_str();
        String res;
        res.buf_ = (char *)c_string + pos + 1;
        res.len_ = column_name_.size() - pos - 1;
        return res;
    }

#ifdef DEBUG
    std::string debug_string()  // for debug
    {
        std::stringstream out;
        out << "print ColumnSchema: " << this << std::endl
            << "name: " << column_name_.c_str() << std::endl
            << "datatype: " << get_data_type_name(data_type_) << std::endl
            << "encoding: " << get_encoding_name(encoding_) << std::endl
            << "compression:" << get_compression_name(compression_)
            << std::endl;
        return out.str();
    }
#endif
};

FORCE_INLINE int64_t get_cur_timestamp() {
    int64_t timestamp = 0;
    struct timeval tv;
    if (gettimeofday(&tv, NULL) >= 0) {
        timestamp = (int64_t)tv.tv_sec * 1000 + tv.tv_usec / 1000;
    }
    return timestamp;
}

#if 0
struct DatabaseIdTTL
{
  NodeID db_nid_;
  int64_t  ttl_;
  int16_t counter_;  // suppose we at most support 64k timeseries.
  DatabaseIdTTL() {}
  DatabaseIdTTL(NodeID db_nid, int64_t ttl, int16_t counter) :  db_nid_(db_nid), ttl_(ttl), counter_(counter) {}  
  DatabaseIdTTL(const DatabaseIdTTL &other) :  db_nid_(other.db_nid_), ttl_(other.ttl_), counter_(other.counter_) {}
  DatabaseIdTTL & operator = (const DatabaseIdTTL &other) 
  {
    this->db_nid_ = other.db_nid_;
    this->ttl_ = other.ttl_;
    this->counter_ = other.counter_;
    return *this;
  }
  bool operator == (const DatabaseIdTTL &other)
  {
    if (db_nid_ != other.db_nid_ || ttl_ != other.ttl_ || counter_ != other.counter_) {
      return false;
    }
    return true;
  }
  friend std::ostream& operator << (std::ostream& out, DatabaseIdTTL& di)
  {

    return out;
  }    
};

struct DeviceIDWithCounter
{
  NodeID device_nid_;
  int16_t counter_;  // suppose we at most support 64k timeseries.
  DeviceIDWithCounter() {}
  DeviceIDWithCounter(NodeID device_nid, int16_t counter) :  device_nid_(device_nid), counter_(counter) {}  
  DeviceIDWithCounter(const DeviceIDWithCounter &other) :  device_nid_(other.device_nid_), counter_(other.counter_) {}
  DeviceIDWithCounter& operator = (const DeviceIDWithCounter &other) 
  {
    this->device_nid_ = other.device_nid_;
    this->counter_ = other.counter_;
    return *this;
  }
  bool operator == (const DeviceID &other)
  {
    if (device_nid_ != other.device_nid_ || counter_ != other.counter_) {
      return false;
    }
    return true;
  }
  friend std::ostream& operator << (std::ostream& out, DeviceID& di)
  {
    out << "(" << di.device_nid_ << ", " << di.counter_ << ")  ";
    return out;
  }    
};
#endif

}  // end namespace common

#endif  // UTILS_UTILS_H
