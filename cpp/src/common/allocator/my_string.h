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
#ifndef COMMON_ALLOCATOR_MY_STRING_H
#define COMMON_ALLOCATOR_MY_STRING_H

#include <common/error_info/error_info.h>
#include <cstring>

#include <numeric>

#include "common/allocator/page_arena.h"
#include "common/error_info/errno_define.h"

namespace common {

// String that use PageArena or system spec.
struct String {
    char *buf_;
    // if len_ == 0: equal to "";
    // if len_ == -1: equal to nullptr;
    int32_t len_;
    // if owned is true, buf_ should be free.
    bool owned_;

    String() : buf_(nullptr), len_(0), owned_(false) {}
    String(char *buf, int32_t len) : buf_(buf), len_(len), owned_(false) {}
    String(const std::string &str, common::PageArena &pa)
        : buf_(nullptr), len_(0), owned_(false) {
        dup_from(str, pa);
    }
    explicit String(const std::string &str) {
        buf_ = const_cast<char *>(str.c_str());
        len_ = static_cast<int32_t>(str.size());
        owned_ = false;
    }
    FORCE_INLINE bool is_null() const { return len_ == -1; }
    FORCE_INLINE bool is_empty() const { return len_ == 0; }
    FORCE_INLINE void reset() {
        len_ = 0;
        buf_ = nullptr;
    }
    FORCE_INLINE int dup_from(const std::string &str, common::PageArena &pa) {
        ASSERT(str.length() <=
               static_cast<int32_t>(std::numeric_limits<int32_t>::max()));
        len_ = static_cast<int32_t>(str.size());
        if (UNLIKELY(len_ == 0)) {
            return error_info::E_OK;
        }
        // len_ must >= 0;
        buf_ = pa.alloc(static_cast<uint32_t>(len_));
        if (IS_NULL(buf_)) {
            RETURN_ERR(error_info::E_OOM, "oom when dup from str: " + str);
        }
        memcpy(buf_, str.c_str(), static_cast<size_t>(len_));
        return error_info::E_OK;
    }

    FORCE_INLINE bool operator==(const String &other) const {
        return equal_to(other);
    }

    FORCE_INLINE int dup_from(const String &str, common::PageArena &pa) {
        len_ = str.len_;
        if (UNLIKELY(len_ == 0)) {
            buf_ = nullptr;
            return error_info::E_OK;
        }
        buf_ = pa.alloc(static_cast<uint32_t>(len_));
        if (IS_NULL(buf_)) {
            RETURN_ERR(error_info::E_OOM,
                       "oom when dup from str: " + str.to_std_string());
        }
        memcpy(buf_, str.buf_, static_cast<uint32_t>(len_));
        return error_info::E_OK;
    }
    FORCE_INLINE int build_from(const String &s1, const String &s2,
                                common::PageArena &pa) {
        len_ = s1.len_ + s2.len_;
        buf_ = pa.alloc(static_cast<uint32_t>(len_));
        if (IS_NULL(buf_)) {
            RETURN_ERR(error_info::E_OOM,
                       "oom when build from str1: " + s1.to_std_string() +
                           ", str2: " + s2.to_std_string());
        }
        memcpy(buf_, s1.buf_, static_cast<size_t>(s1.len_));
        memcpy(buf_ + s1.len_, s2.buf_, static_cast<size_t>(s2.len_));
        return error_info::E_OK;
    }
    FORCE_INLINE void shallow_copy_from(const String &src) {
        buf_ = src.buf_;
        len_ = src.len_;
    }
    FORCE_INLINE bool equal_to(const String &that) const {
        return (len_ == 0 && that.len_ == 0) ||
               (len_ == -1 && that.len_ == -1) ||
               ((len_ == that.len_) && (0 == memcmp(buf_, that.buf_, static_cast<size_t>(len_))));
    }
    // strict less than. If @this equals to @that, return false.
    FORCE_INLINE bool less_than(const String &that) const {
        if (len_ == -1) {
            return that.len_ != -1;
        }
        if (that.len_ == -1) {
            return false;
        }

        if (len_ == 0) {
            return that.len_ > 0;
        }

        if (that.len_ == 0) {
            return false;
        }

        const int32_t min_len = std::min(len_, that.len_);
        const int cmp_res = memcmp(buf_, that.buf_, static_cast<size_t>(min_len));
        if (cmp_res < 0) {
            return true;
        } else if (cmp_res > 0) {
            return false;
        } else {
            return len_ < that.len_;
        }
    }

    // return = 0, if this = that
    // return < 0, if this < that
    // return > 0, if this > that
    FORCE_INLINE int compare(const String &that) const {
        // Rule 1: nullptr is less than everything
        if (len_ == -1 && that.len_ == -1) return 0;
        if (len_ == -1) return -1;
        if (that.len_ == -1) return 1;

        // Rule 2: empty is less than normal string
        if (len_ == 0 && that.len_ == 0) return 0;
        if (len_ == 0) return -1;
        if (that.len_ == 0) return 1;

        // Rule 3: normal string compare (lexicographical)
        const int32_t min_len = std::min(len_, that.len_);

        const int cmp_res = memcmp(buf_, that.buf_, static_cast<size_t>(min_len));
        if (cmp_res != 0) {
            return cmp_res;  // >0 / <0
        }

        // Prefix equal, shorter one is less
        return len_ - that.len_;
    }

    FORCE_INLINE void max(const String &that, common::PageArena &pa) {
        if (compare(that) < 0) {
            this->dup_from(that, pa);
        }
    }

    FORCE_INLINE void min(const String &that, common::PageArena &pa) {
        if (compare(that) > 0) {
            this->dup_from(that, pa);
        }
    }

    bool operator<(const String &other) const {
        if (this->is_null() && other.is_null()) {
            return false;
        }
        if (this->is_null()) {
            return true;
        }
        if (other.is_null()) {
            return false;
        }

        const int min_len = std::min(this->len_, other.len_);
        const int cmp = std::memcmp(this->buf_, other.buf_, static_cast<size_t>(min_len));
        if (cmp != 0) {
            return cmp < 0;
        }

        return this->len_ < other.len_;
    }
    std::string to_std_string() const {
        return {buf_, static_cast<size_t>(len_)};
    }

#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &os, const String &s) {
        os << s.len_ << "@";
        for (uint32_t i = 0; i < s.len_; i++) {
            os << s.buf_[i];
        }
        return os;
    }
#endif  // ifndef NDEBUG
};

struct StringLessThan {
    FORCE_INLINE bool operator()(const String &s1, const String &s2) const {
        return s1.less_than(s2);
    }
};

}  // end namespace common
#endif  // COMMON_ALLOCATOR_MY_STRING_H
