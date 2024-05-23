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

#ifndef COMMON_SEQ_TVLIST_H
#define COMMON_SEQ_TVLIST_H

#include "common/allocator/alloc_base.h"
#include "common/allocator/page_arena.h"
#include "common/mutex/mutex.h"
#include "utils/db_utils.h"
#include "utils/errno_define.h"
#include "utils/storage_utils.h"
#include "utils/util_define.h"

namespace storage {

class SeqTVListBase {
   public:
    SeqTVListBase()
        : data_type_(common::VECTOR),
          mutex_(),
          ref_count_(0),
          primary_array_size_(0),
          list_size_(0),
          write_count_(0),
          page_arena_(common::g_base_allocator),
          use_page_arena_(false),
          is_immutable_(false) {}
    virtual ~SeqTVListBase() {}
    virtual void destroy() {}

    FORCE_INLINE void ref() { ATOMIC_AAF(&ref_count_, 1); }
    FORCE_INLINE bool unref() { return 0 == ATOMIC_AAF(&ref_count_, -1); }

    FORCE_INLINE void lock() { mutex_.lock(); }
    FORCE_INLINE void unlock() { mutex_.unlock(); }

    int32_t get_total_count() const { return write_count_; }
    common::TSDataType get_data_type() const { return data_type_; }
    virtual TimeRange get_time_range() const = 0;
    void mark_immutable() { is_immutable_ = true; }
    bool is_immutable() const { return is_immutable_; }

   protected:
    common::TSDataType data_type_;
    mutable common::Mutex mutex_;
    int32_t ref_count_;
    int32_t primary_array_size_;
    int32_t list_size_;
    int32_t write_count_;
    common::PageArena page_arena_;
    bool use_page_arena_;
    bool is_immutable_;
};

template <typename Type>
class SeqTVList : public SeqTVListBase {
   public:
    typedef struct TV {
        int64_t time_;
        Type value_;
    } TV;

    struct Iterator {
        SeqTVList *host_list_;
        int32_t read_idx_;
        int32_t end_idx_;

        Iterator() : host_list_(nullptr), read_idx_(UINT32_MAX), end_idx_(0) {}

        INLINE void init(SeqTVList *host, int32_t start_idx, int32_t end_idx) {
            host_list_ = host;
            read_idx_ = start_idx;
            end_idx_ = end_idx;
        }

        int next(TV &tv) {
            if (read_idx_ >= end_idx_) {
                return common::E_NO_MORE_DATA;
            }
            tv = host_list_->at(read_idx_);
            read_idx_++;
            return common::E_OK;
        }
    };

   public:
    SeqTVList() : tv_array_list_(nullptr), last_time_(-1) {
        data_type_ = common::GetDataTypeFromTemplateType<Type>();
    }
    virtual ~SeqTVList() {}

    int init(int32_t primary_array_size, int32_t max_count,
             bool use_page_arena);
    void destroy() OVERRIDE;

    int push(int64_t time, Type value);
    int push_without_lock(int64_t time, Type value);
    Iterator scan_without_lock(int64_t start_time, int64_t end_time);
    Iterator scan_without_lock();

    TimeRange get_time_range() const OVERRIDE {
        TimeRange time_range;
        common::MutexGuard mg(mutex_);
        if (write_count_ > 0) {
            time_range.start_time_ = time_at(0);
            time_range.end_time_ = time_at(write_count_ - 1);
            ASSERT(time_range.start_time_ <= time_range.end_time_);
        }
        return time_range;
    }

    FORCE_INLINE TV at(int32_t tv_idx) const {
        ASSERT(tv_idx < write_count_);
        int32_t list_idx = tv_idx / primary_array_size_;
        int32_t list_offset = tv_idx % primary_array_size_;
        return tv_array_list_[list_idx][list_offset];
    }

    FORCE_INLINE int64_t time_at(int32_t tv_idx) const {
        return at(tv_idx).time_;
    }

#ifdef ENABLE_TEST
    int32_t TEST_binary_search_upper(int64_t time) {
        return binary_search_upper(time);
    }
    int32_t TEST_binary_search_lower(int64_t time) {
        return binary_search_lower(time);
    }
#endif

   private:
    FORCE_INLINE void *alloc(uint32_t size) {
        if (use_page_arena_) {
            return page_arena_.alloc(size);
        } else {
            return common::mem_alloc(size, common::MOD_TVLIST_DATA);
        }
    }

    // return the first tv which is larger or equal to @time
    int32_t binary_search_upper(int64_t time);
    // return the last tv which is less or equal to @time
    int32_t binary_search_lower(int64_t time);

   private:
    TV **tv_array_list_;
    int64_t last_time_;
};

}  // namespace storage

#include "seq_tvlist.inc"

#endif  // COMMON_SEQ_TVLIST_H
