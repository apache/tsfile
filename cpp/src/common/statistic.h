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

#ifndef COMMON_STATISTIC_H
#define COMMON_STATISTIC_H

#include <inttypes.h>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "common/db_common.h"

namespace storage {

/*
 * Since we have to handle different type of statistic,
 * here we use macro.
 * TODO: do we have to update end_time_/last_value_ every time?
 */
#define TIME_STAT_UPDATE(time)                    \
    do {                                          \
        if (UNLIKELY(count_ == 0)) {              \
            start_time_ = time;                   \
            end_time_ = time;                     \
        } else {                                  \
            if (UNLIKELY((time) < start_time_)) { \
                start_time_ = (time);             \
            }                                     \
            if (LIKELY(time > end_time_)) {       \
                end_time_ = (time);               \
            }                                     \
        }                                         \
    } while (false)

#define NUM_VALUE_STAT_UPDATE(value)    \
    do {                                \
        if (UNLIKELY(count_ == 0)) {    \
            sum_value_ = (value);       \
            min_value_ = (value);       \
            max_value_ = (value);       \
            first_value_ = (value);     \
            last_value_ = (value);      \
        } else {                        \
            if ((value) < min_value_) { \
                min_value_ = (value);   \
            }                           \
            if ((value) > max_value_) { \
                max_value_ = (value);   \
            }                           \
            sum_value_ += (value);      \
            last_value_ = (value);      \
        }                               \
    } while (false)

#define BOOL_VALUE_STAT_UPDATE(value) \
    do {                              \
        if (UNLIKELY(count_ == 0)) {  \
            sum_value_ = (value);     \
            first_value_ = (value);   \
            last_value_ = (value);    \
        } else {                      \
            sum_value_ += (value);    \
            last_value_ = (value);    \
        }                             \
    } while (false)

#define NUM_STAT_UPDATE(time, value)    \
    do {                                \
        /* update time */               \
        TIME_STAT_UPDATE((time));       \
        /* update value */              \
        NUM_VALUE_STAT_UPDATE((value)); \
        count_++;                       \
    } while (false)

#define BOOL_STAT_UPDATE(time, value)    \
    do {                                 \
        /* update time */                \
        TIME_STAT_UPDATE((time));        \
        /* update value */               \
        BOOL_VALUE_STAT_UPDATE((value)); \
        count_++;                        \
    } while (false)

// Base Statistic
class Statistic {
   public:
    Statistic() : count_(0), start_time_(0), end_time_(0) {}
    virtual void destroy() {}
    FORCE_INLINE void reset() { count_ = 0; }

    virtual FORCE_INLINE void update(int64_t time, bool value) {
        ASSERT(false);
    }
    virtual FORCE_INLINE void update(int64_t time, int32_t value) {
        ASSERT(false);
    }
    virtual FORCE_INLINE void update(int64_t time, int64_t value) {
        ASSERT(false);
    }
    virtual FORCE_INLINE void update(int64_t time, float value) {
        ASSERT(false);
    }
    virtual FORCE_INLINE void update(int64_t time, double value) {
        ASSERT(false);
    }
    virtual FORCE_INLINE void update(int64_t time) { ASSERT(false); }

    virtual int serialize_to(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_var_uint(count_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui64(start_time_,
                                                                  out))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::write_ui64(end_time_, out))) {
        } else if (RET_FAIL(serialize_typed_stat(out))) {
        }
        return ret;
    }
    virtual int serialize_typed_stat(common::ByteStream &out) {
        ASSERT(false);
        return 0;
    }
    virtual int deserialize_from(common::ByteStream &in) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_var_uint(
                (uint32_t &)count_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)start_time_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)end_time_, in))) {
        } else if (RET_FAIL(deserialize_typed_stat(in))) {
        }
        return ret;
    }
    virtual int deserialize_typed_stat(common::ByteStream &in) {
        ASSERT(false);
        return 0;
    }
    virtual int merge_with(Statistic *that) {
        ASSERT(false);
        return 0;
    }
    virtual int deep_copy_from(Statistic *stat) {
        ASSERT(false);
        return 0;
    }
    virtual common::TSDataType get_type() {
        ASSERT(false);
        return common::INVALID_DATATYPE;
    }
    virtual std::string to_string() const {
        return std::string("UNTYPED_STATISTIC");
    }

   public:
    int32_t count_;
    int64_t start_time_;
    int64_t end_time_;
};

#define MERGE_BOOL_STAT_FROM(StatType, untyped_stat)       \
    do {                                                   \
        if (UNLIKELY(untyped_stat == nullptr)) {           \
            return common::E_INVALID_ARG;                  \
        }                                                  \
        StatType *typed_stat = (StatType *)(untyped_stat); \
        if (UNLIKELY(typed_stat == nullptr)) {             \
            return common::E_TYPE_NOT_MATCH;               \
        }                                                  \
        if (UNLIKELY(typed_stat->count_ == 0)) {           \
            return common::E_OK;                           \
        }                                                  \
        if (count_ == 0) {                                 \
            count_ = typed_stat->count_;                   \
            start_time_ = typed_stat->start_time_;         \
            end_time_ = typed_stat->end_time_;             \
            sum_value_ = typed_stat->sum_value_;           \
            first_value_ = typed_stat->first_value_;       \
            last_value_ = typed_stat->last_value_;         \
        } else {                                           \
            count_ += typed_stat->count_;                  \
            if (typed_stat->start_time_ < start_time_) {   \
                start_time_ = typed_stat->start_time_;     \
                first_value_ = typed_stat->first_value_;   \
            }                                              \
            if (typed_stat->end_time_ > end_time_) {       \
                end_time_ = typed_stat->end_time_;         \
                last_value_ = typed_stat->last_value_;     \
            }                                              \
            sum_value_ += typed_stat->sum_value_;          \
        }                                                  \
        return common::E_OK;                               \
    } while (false)

#define MERGE_NUM_STAT_FROM(StatType, untyped_stat)                    \
    do {                                                               \
        if (UNLIKELY(untyped_stat == nullptr)) {                       \
            return common::E_INVALID_ARG;                              \
        }                                                              \
        StatType *typed_stat = (StatType *)(untyped_stat);             \
        if (UNLIKELY(typed_stat == nullptr)) {                         \
            return common::E_TYPE_NOT_MATCH;                           \
        }                                                              \
        if (UNLIKELY(typed_stat->count_ == 0)) {                       \
            return common::E_OK;                                       \
        }                                                              \
        if (count_ == 0) {                                             \
            count_ = typed_stat->count_;                               \
            start_time_ = typed_stat->start_time_;                     \
            end_time_ = typed_stat->end_time_;                         \
            sum_value_ = typed_stat->sum_value_;                       \
            first_value_ = typed_stat->first_value_;                   \
            last_value_ = typed_stat->last_value_;                     \
            min_value_ = typed_stat->min_value_;                       \
            max_value_ = typed_stat->max_value_;                       \
        } else {                                                       \
            count_ += typed_stat->count_;                              \
            if (typed_stat->start_time_ < start_time_) {               \
                start_time_ = typed_stat->start_time_;                 \
                first_value_ = typed_stat->first_value_;               \
            }                                                          \
            if (typed_stat->end_time_ > end_time_) {                   \
                end_time_ = typed_stat->end_time_;                     \
                last_value_ = typed_stat->last_value_;                 \
            }                                                          \
            sum_value_ += typed_stat->sum_value_;                      \
            min_value_ = std::min(min_value_, typed_stat->min_value_); \
            max_value_ = std::max(max_value_, typed_stat->max_value_); \
        }                                                              \
        return common::E_OK;                                           \
    } while (false)

#define MERGE_TIME_STAT_FROM(StatType, untyped_stat)       \
    do {                                                   \
        if (UNLIKELY(untyped_stat == nullptr)) {           \
            return common::E_INVALID_ARG;                  \
        }                                                  \
        StatType *typed_stat = (StatType *)(untyped_stat); \
        if (UNLIKELY(typed_stat == nullptr)) {             \
            return common::E_TYPE_NOT_MATCH;               \
        }                                                  \
        if (UNLIKELY(typed_stat->count_ == 0)) {           \
            return common::E_OK;                           \
        }                                                  \
        if (count_ == 0) {                                 \
            count_ = typed_stat->count_;                   \
            start_time_ = typed_stat->start_time_;         \
            end_time_ = typed_stat->end_time_;             \
        } else {                                           \
            count_ += typed_stat->count_;                  \
            if (typed_stat->start_time_ < start_time_) {   \
                start_time_ = typed_stat->start_time_;     \
            }                                              \
            if (typed_stat->end_time_ > end_time_) {       \
                end_time_ = typed_stat->end_time_;         \
            }                                              \
        }                                                  \
        return common::E_OK;                               \
    } while (false)

#define DEEP_COPY_BOOL_STAT_FROM(StatType, untyped_stat)   \
    do {                                                   \
        if (UNLIKELY(untyped_stat == nullptr)) {           \
            return common::E_INVALID_ARG;                  \
        }                                                  \
        StatType *typed_stat = (StatType *)(untyped_stat); \
        if (UNLIKELY(typed_stat == nullptr)) {             \
            return common::E_TYPE_NOT_MATCH;               \
        }                                                  \
        count_ = typed_stat->count_;                       \
        start_time_ = typed_stat->start_time_;             \
        end_time_ = typed_stat->end_time_;                 \
        sum_value_ = typed_stat->sum_value_;               \
        first_value_ = typed_stat->first_value_;           \
        last_value_ = typed_stat->last_value_;             \
        return common::E_OK;                               \
    } while (false)

#define DEEP_COPY_NUM_STAT_FROM(StatType, untyped_stat)    \
    do {                                                   \
        if (UNLIKELY(untyped_stat == nullptr)) {           \
            return common::E_INVALID_ARG;                  \
        }                                                  \
        StatType *typed_stat = (StatType *)(untyped_stat); \
        if (UNLIKELY(typed_stat == nullptr)) {             \
            return common::E_TYPE_NOT_MATCH;               \
        }                                                  \
        count_ = typed_stat->count_;                       \
        start_time_ = typed_stat->start_time_;             \
        end_time_ = typed_stat->end_time_;                 \
        sum_value_ = typed_stat->sum_value_;               \
        first_value_ = typed_stat->first_value_;           \
        last_value_ = typed_stat->last_value_;             \
        min_value_ = typed_stat->min_value_;               \
        max_value_ = typed_stat->max_value_;               \
        return common::E_OK;                               \
    } while (false)

#define DEEP_COPY_TIME_STAT_FROM(StatType, untyped_stat)   \
    do {                                                   \
        if (UNLIKELY(untyped_stat == nullptr)) {           \
            return common::E_INVALID_ARG;                  \
        }                                                  \
        StatType *typed_stat = (StatType *)(untyped_stat); \
        if (UNLIKELY(typed_stat == nullptr)) {             \
            return common::E_TYPE_NOT_MATCH;               \
        }                                                  \
        count_ = typed_stat->count_;                       \
        start_time_ = typed_stat->start_time_;             \
        end_time_ = typed_stat->end_time_;                 \
        return common::E_OK;                               \
    } while (false)

/* ================ Typed Statistics ================*/
class BooleanStatistic : public Statistic {
   public:
    int64_t sum_value_;
    bool first_value_;
    bool last_value_;

    BooleanStatistic()
        : sum_value_(0), first_value_(false), last_value_(false) {}

    void clone_from(const BooleanStatistic &that) {
        count_ = that.count_;
        start_time_ = that.start_time_;
        end_time_ = that.end_time_;

        sum_value_ = that.sum_value_;
        first_value_ = that.first_value_;
        last_value_ = that.last_value_;
    }

    FORCE_INLINE void update(int64_t time, bool value) {
        BOOL_STAT_UPDATE(time, value);
    }
    int serialize_typed_stat(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_ui8(first_value_ ? 1 : 0,
                                                          out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui8(
                       last_value_ ? 1 : 0, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui64(sum_value_,
                                                                  out))) {
        }
        return ret;
    }
    int deserialize_typed_stat(common::ByteStream &in) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_ui8(
                (uint8_t &)first_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui8(
                       (uint8_t &)last_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)sum_value_, in))) {
        }
        return ret;
    }

    FORCE_INLINE common::TSDataType get_type() { return common::BOOLEAN; }

    int merge_with(Statistic *stat) {
        MERGE_BOOL_STAT_FROM(BooleanStatistic, stat);
    }

    int deep_copy_from(Statistic *stat) {
        DEEP_COPY_BOOL_STAT_FROM(BooleanStatistic, stat);
    }
};

class Int32Statistic : public Statistic {
   public:
    int64_t sum_value_;
    int32_t min_value_;
    int32_t max_value_;
    int32_t first_value_;
    int32_t last_value_;

    Int32Statistic()
        : sum_value_(0),
          min_value_(0),
          max_value_(0),
          first_value_(0),
          last_value_(0) {}

    void clone_from(const Int32Statistic &that) {
        count_ = that.count_;
        start_time_ = that.start_time_;
        end_time_ = that.end_time_;

        sum_value_ = that.sum_value_;
        min_value_ = that.min_value_;
        max_value_ = that.max_value_;
        first_value_ = that.first_value_;
        last_value_ = that.last_value_;
    }

    FORCE_INLINE void update(int64_t time, int32_t value) {
        NUM_STAT_UPDATE(time, value);
    }

    FORCE_INLINE common::TSDataType get_type() { return common::INT32; }

    int serialize_typed_stat(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_ui32(min_value_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui32(max_value_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui32(first_value_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui32(last_value_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui64(sum_value_,
                                                                  out))) {
        }
        return ret;
    }
    int deserialize_typed_stat(common::ByteStream &in) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_ui32(
                (uint32_t &)min_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui32(
                       (uint32_t &)max_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui32(
                       (uint32_t &)first_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui32(
                       (uint32_t &)last_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)sum_value_, in))) {
        }
        // std::cout << "deserialize_typed_stat. ret=" << ret
        //           << ", min_value_= " << min_value_
        //           << ", max_value_=" << max_value_
        //           << ", first_value_=" << first_value_
        //           << ", last_value_=" << last_value_
        //           << ", sum_value_=" << sum_value_
        //           << std::endl;
        return ret;
    }
    int merge_with(Statistic *stat) {
        MERGE_NUM_STAT_FROM(Int32Statistic, stat);
    }

    int deep_copy_from(Statistic *stat) {
        DEEP_COPY_NUM_STAT_FROM(Int32Statistic, stat);
    }

    std::string to_string() const {
        const int buf_len = 256;
        char buf[buf_len];
        snprintf(buf, buf_len,
                 "{count=%d, start_time=%" PRId64 ", end_time=%" PRId64
                 ", first_val=%d, last_val=%d, sum_value=%" PRId64
                 ", min_value=%d, max_value=%d}",
                 count_, start_time_, end_time_, first_value_, last_value_,
                 sum_value_, min_value_, max_value_);
        return std::string(buf);
    }
};

class Int64Statistic : public Statistic {
   public:
    double sum_value_;
    int64_t min_value_;
    int64_t max_value_;
    int64_t first_value_;
    int64_t last_value_;

    Int64Statistic()
        : sum_value_(0),
          min_value_(0),
          max_value_(0),
          first_value_(0),
          last_value_(0) {}

    void clone_from(const Int64Statistic &that) {
        count_ = that.count_;
        start_time_ = that.start_time_;
        end_time_ = that.end_time_;

        sum_value_ = that.sum_value_;
        min_value_ = that.min_value_;
        max_value_ = that.max_value_;
        first_value_ = that.first_value_;
        last_value_ = that.last_value_;
    }

    FORCE_INLINE void update(int64_t time, int64_t value) {
        NUM_STAT_UPDATE(time, value);
    }

    FORCE_INLINE common::TSDataType get_type() { return common::INT64; }

    int serialize_typed_stat(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_ui64(min_value_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui64(max_value_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui64(first_value_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_ui64(last_value_,
                                                                  out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_double(sum_value_,
                                                                    out))) {
        }
        return ret;
    }
    int deserialize_typed_stat(common::ByteStream &in) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_ui64(
                (uint64_t &)min_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)max_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)first_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_ui64(
                       (uint64_t &)last_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_double(sum_value_,
                                                                   in))) {
        }
        return ret;
    }
    int merge_with(Statistic *stat) {
        MERGE_NUM_STAT_FROM(Int64Statistic, stat);
    }

    int deep_copy_from(Statistic *stat) {
        DEEP_COPY_NUM_STAT_FROM(Int64Statistic, stat);
    }

    std::string to_string() const {
        const int buf_len = 256;
        char buf[buf_len];
        snprintf(buf, buf_len,
                 "{count=%d, start_time=%" PRId64 ", end_time=%" PRId64
                 ", first_val=%" PRId64 ", last_val=%" PRId64
                 ", sum_value=%lf, min_value=%" PRId64 ", max_value=%" PRId64
                 "}",
                 count_, start_time_, end_time_, first_value_, last_value_,
                 sum_value_, min_value_, max_value_);
        return std::string(buf);
    }
};

class FloatStatistic : public Statistic {
   public:
    double sum_value_;
    float min_value_;
    float max_value_;
    float first_value_;
    float last_value_;

    FloatStatistic()
        : sum_value_(0),
          min_value_(0),
          max_value_(0),
          first_value_(0),
          last_value_(0) {}

    void clone_from(const FloatStatistic &that) {
        count_ = that.count_;
        start_time_ = that.start_time_;
        end_time_ = that.end_time_;

        sum_value_ = that.sum_value_;
        min_value_ = that.min_value_;
        max_value_ = that.max_value_;
        first_value_ = that.first_value_;
        last_value_ = that.last_value_;
    }
    FORCE_INLINE void update(int64_t time, float value) {
        NUM_STAT_UPDATE(time, value);
    }

    FORCE_INLINE common::TSDataType get_type() { return common::FLOAT; }

    int serialize_typed_stat(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::write_float(min_value_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_float(max_value_,
                                                                   out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_float(first_value_,
                                                                   out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_float(last_value_,
                                                                   out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_double(sum_value_,
                                                                    out))) {
        }
        return ret;
    }
    int deserialize_typed_stat(common::ByteStream &in) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_float(min_value_, in))) {
        } else if (RET_FAIL(
                       common::SerializationUtil::read_float(max_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_float(first_value_,
                                                                  in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_float(last_value_,
                                                                  in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_double(sum_value_,
                                                                   in))) {
        }
        return ret;
    }
    int merge_with(Statistic *stat) {
        MERGE_NUM_STAT_FROM(FloatStatistic, stat);
    }
    int deep_copy_from(Statistic *stat) {
        DEEP_COPY_NUM_STAT_FROM(FloatStatistic, stat);
    }
};

class DoubleStatistic : public Statistic {
   public:
    double sum_value_;
    double min_value_;
    double max_value_;
    double first_value_;
    double last_value_;

    DoubleStatistic()
        : sum_value_(0),
          min_value_(0),
          max_value_(0),
          first_value_(0),
          last_value_(0) {}

    void clone_from(const DoubleStatistic &that) {
        count_ = that.count_;
        start_time_ = that.start_time_;
        end_time_ = that.end_time_;

        sum_value_ = that.sum_value_;
        min_value_ = that.min_value_;
        max_value_ = that.max_value_;
        first_value_ = that.first_value_;
        last_value_ = that.last_value_;
    }
    FORCE_INLINE void update(int64_t time, double value) {
        NUM_STAT_UPDATE(time, value);
    }

    FORCE_INLINE common::TSDataType get_type() { return common::DOUBLE; }

    int serialize_typed_stat(common::ByteStream &out) {
        int ret = common::E_OK;
        if (RET_FAIL(
                common::SerializationUtil::write_double(min_value_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_double(max_value_,
                                                                    out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_double(
                       first_value_, out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_double(last_value_,
                                                                    out))) {
        } else if (RET_FAIL(common::SerializationUtil::write_double(sum_value_,
                                                                    out))) {
        }
        return ret;
    }
    int deserialize_typed_stat(common::ByteStream &in) {
        int ret = common::E_OK;
        if (RET_FAIL(common::SerializationUtil::read_double(min_value_, in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_double(max_value_,
                                                                   in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_double(first_value_,
                                                                   in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_double(last_value_,
                                                                   in))) {
        } else if (RET_FAIL(common::SerializationUtil::read_double(sum_value_,
                                                                   in))) {
        }
        return ret;
    }
    int merge_with(Statistic *stat) {
        MERGE_NUM_STAT_FROM(DoubleStatistic, stat);
    }
    int deep_copy_from(Statistic *stat) {
        DEEP_COPY_NUM_STAT_FROM(DoubleStatistic, stat);
    }
};

#if 0
class BinaryStatistic : public Statistic
{
  // TODO
};
#endif

class TimeStatistic : public Statistic {
   public:
    TimeStatistic() {}

    void clone_from(const TimeStatistic &that) {
        count_ = that.count_;
        start_time_ = that.start_time_;
        end_time_ = that.end_time_;
    }

    FORCE_INLINE void update(int64_t time) {
        TIME_STAT_UPDATE((time));
        count_++;
    }

    FORCE_INLINE common::TSDataType get_type() { return common::VECTOR; }

    int serialize_typed_stat(common::ByteStream &out) { return common::E_OK; }
    int deserialize_typed_stat(common::ByteStream &in) { return common::E_OK; }
    int merge_with(Statistic *stat) {
        MERGE_TIME_STAT_FROM(TimeStatistic, stat);
    }

    int deep_copy_from(Statistic *stat) {
        DEEP_COPY_TIME_STAT_FROM(TimeStatistic, stat);
    }

    std::string to_string() const {
        const int buf_len = 256;
        char buf[buf_len];
        snprintf(buf, buf_len,
                 "{count=%d, start_time=%" PRId64 ", end_time=%" PRId64 "}",
                 count_, start_time_, end_time_);
        return std::string(buf);
    }
};

FORCE_INLINE uint32_t get_typed_statistic_sizeof(common::TSDataType type) {
    uint32_t ret_size = 0;
    switch (type) {
        case common::BOOLEAN:
            ret_size = sizeof(BooleanStatistic);
            break;
        case common::INT32:
            ret_size = sizeof(Int32Statistic);
            break;
        case common::INT64:
            ret_size = sizeof(Int64Statistic);
            break;
        case common::FLOAT:
            ret_size = sizeof(FloatStatistic);
            break;
        case common::DOUBLE:
            ret_size = sizeof(DoubleStatistic);
            break;
        case common::TEXT:
            ASSERT(false);
            break;
        case common::VECTOR:
            ret_size = sizeof(TimeStatistic);
            break;
        default:
            ASSERT(false);
            break;
    }
    return ret_size;
}

FORCE_INLINE Statistic *placement_new_statistic(common::TSDataType type,
                                                void *buf) {
    Statistic *s = nullptr;
    switch (type) {
        case common::BOOLEAN:
            s = new (buf) BooleanStatistic;
            break;
        case common::INT32:
            s = new (buf) Int32Statistic;
            break;
        case common::INT64:
            s = new (buf) Int64Statistic;
            break;
        case common::FLOAT:
            s = new (buf) FloatStatistic;
            break;
        case common::DOUBLE:
            s = new (buf) DoubleStatistic;
            break;
        case common::TEXT:
            ASSERT(false);
            break;
        case common::VECTOR:
            s = new (buf) TimeStatistic;
            break;
        default:
            ASSERT(false);
            break;
    }
    return s;
}

#define TYPED_CLONE_STATISTIC(StatType) \
    (static_cast<StatType *>(to))       \
        ->clone_from(*(static_cast<const StatType *>(from)))

FORCE_INLINE void clone_statistic(Statistic *from, Statistic *to,
                                  common::TSDataType type) {
    ASSERT(from != nullptr);
    ASSERT(to != nullptr);

    to->count_ = from->count_;
    to->start_time_ = from->start_time_;
    to->end_time_ = from->end_time_;

    switch (type) {
        case common::BOOLEAN:
            TYPED_CLONE_STATISTIC(BooleanStatistic);
            break;
        case common::INT32:
            TYPED_CLONE_STATISTIC(Int32Statistic);
            break;
        case common::INT64:
            TYPED_CLONE_STATISTIC(Int64Statistic);
            break;
        case common::FLOAT:
            TYPED_CLONE_STATISTIC(FloatStatistic);
            break;
        case common::DOUBLE:
            TYPED_CLONE_STATISTIC(DoubleStatistic);
            break;
        case common::TEXT:
            ASSERT(false);
            break;
        case common::VECTOR:
            TYPED_CLONE_STATISTIC(TimeStatistic);
            break;
        default:
            ASSERT(false);
    }
}

#define ALLOC_STATISTIC(StatType)                                             \
    do {                                                                      \
        buf = common::mem_alloc(sizeof(StatType), common::MOD_STATISTIC_OBJ); \
        if (buf != nullptr) {                                                 \
            stat = new (buf) StatType;                                        \
        }                                                                     \
    } while (false)

#define ALLOC_STATISTIC_WITH_PA(StatType)  \
    do {                                   \
        buf = pa->alloc(sizeof(StatType)); \
        if (buf != nullptr) {              \
            stat = new (buf) StatType;     \
        }                                  \
    } while (false);

class StatisticFactory {
   public:
    static Statistic *alloc_statistic(common::TSDataType data_type) {
        void *buf = nullptr;
        Statistic *stat = nullptr;
        switch (data_type) {
            case common::BOOLEAN:
                ALLOC_STATISTIC(BooleanStatistic);
                break;
            case common::INT32:
                ALLOC_STATISTIC(Int32Statistic);
                break;
            case common::INT64:
                ALLOC_STATISTIC(Int64Statistic);
                break;
            case common::FLOAT:
                ALLOC_STATISTIC(FloatStatistic);
                break;
            case common::DOUBLE:
                ALLOC_STATISTIC(DoubleStatistic);
                break;
            case common::TEXT:
                ASSERT(false);
                break;
            case common::VECTOR:
                ALLOC_STATISTIC(TimeStatistic);
                break;
            default:
                abort();
                ASSERT(false);
        }
        return stat;
    }
    static Statistic *alloc_statistic_with_pa(common::TSDataType data_type,
                                              common::PageArena *pa) {
        void *buf = nullptr;
        Statistic *stat = nullptr;
        switch (data_type) {
            case common::BOOLEAN:
                ALLOC_STATISTIC_WITH_PA(BooleanStatistic);
                break;
            case common::INT32:
                ALLOC_STATISTIC_WITH_PA(Int32Statistic);
                break;
            case common::INT64:
                ALLOC_STATISTIC_WITH_PA(Int64Statistic);
                break;
            case common::FLOAT:
                ALLOC_STATISTIC_WITH_PA(FloatStatistic);
                break;
            case common::DOUBLE:
                ALLOC_STATISTIC_WITH_PA(DoubleStatistic);
                break;
            case common::TEXT:
                ASSERT(false);
                break;
            case common::VECTOR:
                ALLOC_STATISTIC_WITH_PA(TimeStatistic);
                break;
            default:
                ASSERT(false);
        }
        return stat;
    }
    static void free(Statistic *stat) { common::mem_free(stat); }
};

}  // end namespace storage
#endif  // COMMON_STATISTIC_H
