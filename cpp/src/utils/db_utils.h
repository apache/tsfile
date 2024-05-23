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

struct DeviceID {
    NodeID db_nid_;
    NodeID device_nid_;

    DeviceID() : db_nid_(0), device_nid_(0) {}
    DeviceID(const NodeID db_nid, const NodeID device_nid)
        : db_nid_(db_nid), device_nid_(device_nid) {}
    explicit DeviceID(const TsID &ts_id)
        : db_nid_(ts_id.db_nid_), device_nid_(ts_id.device_nid_) {}

    FORCE_INLINE bool operator==(const DeviceID &other) const {
        return db_nid_ == other.db_nid_ && device_nid_ == other.device_nid_;
    }
    FORCE_INLINE bool operator!=(const DeviceID &other) const {
        return db_nid_ != other.db_nid_ || device_nid_ != other.device_nid_;
    }
    FORCE_INLINE void from(const TsID &ts_id) {
        db_nid_ = ts_id.db_nid_;
        device_nid_ = ts_id.device_nid_;
    }
    FORCE_INLINE bool operator<(const DeviceID &that) const {
        int32_t this_i32 = (((int32_t)db_nid_) << 16) | (device_nid_);
        int32_t that_i32 = (((int32_t)that.db_nid_) << 16) | (that.device_nid_);
        return this_i32 < that_i32;
    }
};

#define INVALID_TTL (-1)

// describe single database
struct DatabaseDesc {
    int64_t ttl_;
    std::string db_name_;
    TsID ts_id_;

    DatabaseDesc() : ttl_(INVALID_TTL), db_name_(""), ts_id_() {}
    DatabaseDesc(uint64_t ttl, const std::string &name, const TsID &ts_id)
        : ttl_(ttl), db_name_(name), ts_id_(ts_id) {}
};

// Describe single timeseries
struct ColumnDesc {
    TSDataType type_;
    TSEncoding encoding_;
    CompressionType compression_;
    int64_t ttl_;  // obtained from the metadata and passed to the storage
    std::string column_name_;  // measurement name or Time
    TsID ts_id_;               // id of timeseries

    ColumnDesc()
        : type_(INVALID_DATATYPE),
          encoding_(PLAIN),
          compression_(UNCOMPRESSED),
          ttl_(INVALID_TTL),
          column_name_(""),
          ts_id_() {}

    ColumnDesc(TSDataType type, TSEncoding encoding,
               CompressionType compression, uint64_t ttl,
               const std::string &name, const TsID &ts_id)
        : type_(type),
          encoding_(encoding),
          compression_(compression),
          ttl_(ttl),
          column_name_(name),
          ts_id_(ts_id) {}

    ~ColumnDesc() {}

    bool operator==(const ColumnDesc &other) const {
        return (type_ == other.type_ && encoding_ == other.encoding_ &&
                compression_ == other.compression_ &&
                column_name_ == other.column_name_ && ts_id_ == other.ts_id_);
    }

    bool operator!=(const ColumnDesc &other) const {
        return (type_ != other.type_ || encoding_ != other.encoding_ ||
                compression_ != other.compression_ ||
                column_name_ != other.column_name_ || ts_id_ != other.ts_id_);
    }

    bool is_valid() const {
        return type_ != INVALID_DATATYPE && encoding_ != INVALID_ENCODING &&
               compression_ != INVALID_COMPRESSION && ts_id_.is_valid();
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

    std::string debug_string()  // for debug
    {
        std::stringstream out;
        out << "print ColumnDesc: " << this << std::endl
            << "name: " << column_name_.c_str() << std::endl
            << "datatype: " << type_ << std::endl
            << "encoding: " << encoding_ << std::endl
            << "compression:" << compression_ << std::endl
            << "ttl:" << ttl_ << std::endl
            << "tsid:" << ts_id_.to_string().c_str() << std::endl;
        return out.str();
    }
};

enum WALFlushPolicy {
    WAL_DISABLED = 0,
    WAL_ASYNC = 1,
    WAL_FLUSH = 2,
    WAL_SYNC = 3,
};

template <typename T>
std::string to_string(const T &val) {
    // todo: There may be a better way to avoid the memory problem of
    // ostringstream
    std::ostringstream oss;
    oss << val;
    return oss.str();
}

// TODO rename to DatabaseIdTTL
struct DatabaseIdTTL {
    NodeID db_nid_;
    int64_t ttl_;
    DatabaseIdTTL() {}
    DatabaseIdTTL(NodeID db_nid, int64_t ttl) : db_nid_(db_nid), ttl_(ttl) {}
    DatabaseIdTTL(const DatabaseIdTTL &other)
        : db_nid_(other.db_nid_), ttl_(other.ttl_) {}
    DatabaseIdTTL &operator=(const DatabaseIdTTL &other) {
        this->db_nid_ = other.db_nid_;
        this->ttl_ = other.ttl_;
        return *this;
    }
    bool operator==(const DatabaseIdTTL &other) {
        if (db_nid_ != other.db_nid_ || ttl_ != other.ttl_) {
            return false;
        }
        return true;
    }
    friend std::ostream &operator<<(std::ostream &out, DatabaseIdTTL &di) {
        out << "(" << di.db_nid_ << ", " << di.ttl_ << ")  ";
        return out;
    }
};

FORCE_INLINE int64_t get_cur_timestamp() {
    int64_t timestamp = 0;
    struct timeval tv;
    if (gettimeofday(&tv, NULL) >= 0) {
        timestamp = tv.tv_sec * 1000 + tv.tv_usec / 1000;
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
