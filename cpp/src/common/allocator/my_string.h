#ifndef COMMON_ALLOCATOR_MY_STRING_H
#define COMMON_ALLOCATOR_MY_STRING_H

#include <string.h>

#include <iostream>

#include "common/allocator/page_arena.h"
#include "utils/errno_define.h"

namespace common {

// String that use PageArena
struct String {
    char *buf_;
    uint32_t len_;

    String() : buf_(nullptr), len_(0) {}
    String(char *buf, uint32_t len) : buf_(buf), len_(len) {}
    FORCE_INLINE bool is_null() const { return buf_ == nullptr && len_ == 0; }
    FORCE_INLINE void reset() {
        len_ = 0;
        buf_ = nullptr;
    }
    FORCE_INLINE int dup_from(const std::string &str, common::PageArena &pa) {
        len_ = str.size();
        if (UNLIKELY(len_ == 0)) {
            return common::E_OK;
        }
        buf_ = pa.alloc(len_);
        if (IS_NULL(buf_)) {
            return common::E_OOM;
        }
        memcpy(buf_, str.c_str(), len_);
        return common::E_OK;
    }
    FORCE_INLINE int dup_from(const String &str, common::PageArena &pa) {
        len_ = str.len_;
        if (UNLIKELY(len_ == 0)) {
            return common::E_OK;
        }
        buf_ = pa.alloc(len_);
        if (IS_NULL(buf_)) {
            return common::E_OOM;
        }
        memcpy(buf_, str.buf_, len_);
        return common::E_OK;
    }
    FORCE_INLINE int build_from(const String &s1, const String &s2,
                                common::PageArena &pa) {
        len_ = s1.len_ + s2.len_;
        buf_ = pa.alloc(len_);
        if (IS_NULL(buf_)) {
            return common::E_OOM;
        }
        memcpy(buf_, s1.buf_, s1.len_);
        memcpy(buf_ + s1.len_, s2.buf_, s2.len_);
        return common::E_OK;
    }
    FORCE_INLINE void shallow_copy_from(const String &src) {
        buf_ = src.buf_;
        len_ = src.len_;
    }
    FORCE_INLINE bool equal_to(const String &that) const {
        return (len_ == 0 && that.len_ == 0) ||
               ((len_ == that.len_) && (0 == memcmp(buf_, that.buf_, len_)));
    }

    // FORCE_INLINE bool equal_to(const std::string &that)
    // {
    //   return (len_ == 0 && that.size() == 0)
    //          || ((len_ == that.len_) && (0 == memcmp(buf_, that.c_str(),
    //          len_)));
    // }

    // strict less than. If @this equals to @that, return false.
    FORCE_INLINE bool less_than(const String &that) const {
        if (len_ == 0 || that.len_ == 0) {
            return false;
        }
        uint32_t min_len = std::min(len_, that.len_);
        int cmp_res = memcmp(buf_, that.buf_, min_len);
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
        if (len_ == 0 || that.len_ == 0) {
            return 0;
        }
        uint32_t min_len = std::min(len_, that.len_);
        int cmp_res = memcmp(buf_, that.buf_, min_len);
        if (cmp_res == 0) {
            return len_ - that.len_;
        } else {
            return cmp_res;
        }
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
