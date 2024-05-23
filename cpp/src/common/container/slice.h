#ifndef COMMON_CONTAINER_SLICE_H
#define COMMON_CONTAINER_SLICE_H

#include <cassert>
#include <cstddef>
#include <cstring>
#include <string>

#include "common/allocator/alloc_base.h"
#include "common/logger/elog.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace common {

class Slice {
   public:
    // Create an empty slice.
    // Slice() : data_(""), size_(0) {}
    Slice() {
        data_ = nullptr;
        size_ = 0;
    }

    // Create a slice that refers to the contents of "s"
    // Slice(const std::string& s) : data_(s.data()), size_(s.size()) {}
    Slice(const std::string &s) {
        int len = s.size();
        data_ = (char *)malloc(len + 1);  // TODO: user ourself's mem_alloc()
        if (UNLIKELY(nullptr == data_)) {
            // log_err("malloc() failed.");
        }
        strcpy(data_, s.c_str());
        size_ = len;
    }

    // Create a slice that refers to s[0,strlen(s)-1]
    // Slice(const char* s) : data_(s), size_(strlen(s)) {}
    Slice(const char *s) {
        int len = strlen(s);
        data_ = (char *)malloc(len + 1);  // TODO: user ourself's mem_alloc()
        if (UNLIKELY(nullptr == data_)) {
            // log_err("malloc() failed.");
        }
        strcpy(data_, s);
        size_ = len;
    }

    ~Slice() {
        free(data_);  // TODO: user ourself's mem_free()
        data_ = nullptr;
        size_ = 0;
    }

    // Intentionally copyable.
    // Slice(const Slice&) = default;
    // Slice& operator=(const Slice&) = default;
    Slice(const Slice &other) {
        int len = other.size_;
        data_ = (char *)malloc(len + 1);  // TODO: user ourself's mem_alloc()
        if (UNLIKELY(nullptr == data_)) {
            // log_err("malloc() failed.");
        }
        strcpy(data_, other.data_);
        size_ = len;
    }
    Slice &operator=(const Slice &other) {
        if (this->data_ != nullptr) {
            free(data_);  // TODO: user ourself's mem_free()
            data_ = nullptr;
        }
        int len = other.size_;
        this->data_ =
            (char *)malloc(len + 1);  // TODO: user ourself's mem_alloc()
        if (UNLIKELY(nullptr == data_)) {
            // log_err("malloc() failed.");
        }
        strcpy(this->data_, other.data_);
        this->size_ = len;
        return *this;
    }

    // Return a pointer to the beginning of the referenced data
    char *data() const { return data_; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return size_; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return size_ == 0; }

    // Return the ith byte in the referenced data.
    char operator[](size_t n) const {
        ASSERT(n < size());
        return data_[n];
    }

    // Return a string that contains the copy of the referenced data.
    std::string to_string() const { return std::string(data_, size_); }

    friend bool operator==(const Slice &x, const Slice &y) {
        return ((x.size() == y.size()) &&
                (memcmp(x.data(), y.data(), x.size()) == 0));
    }

    friend bool operator!=(const Slice &x, const Slice &y) { return !(x == y); }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Slice &b) const {
        const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
        int r = memcmp(data_, b.data_, min_len);
        if (r == 0) {
            if (size_ < b.size_)
                r = -1;
            else if (size_ > b.size_)
                r = +1;
        }
        return r;
    }

   private:
    char *data_;
    size_t size_;
};

}  // end namespace common
#endif  // COMMON_CONTAINER_SLICE_H
