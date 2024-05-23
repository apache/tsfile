/*
 * no duplicated elements
 */
#ifndef COMMON_CONTAINER_SORTEDARRAY_H
#define COMMON_CONTAINER_SORTEDARRAY_H

#include <stdint.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "common/allocator/alloc_base.h"
#include "common/logger/elog.h"
#include "utils/errno_define.h"
#include "utils/util_define.h"

#define SORTED_ARRAY_INIT_CAPACITY 1000   // TODO: configurable mode
#define SORTED_ARRAY_GROWTH_THRESHOLD 50  // TODO: configurable mode
#define SORTED_ARRAY_SHRINK_THRESHOLD 50  // TODO: configurable mode

namespace common {

template <class ValueType>
class SortedArray {
   public:
#ifndef NDEBUG
    friend std::ostream &operator<<(std::ostream &out,
                                    SortedArray<ValueType> &sa) {
        for (size_t idx = 0; idx < sa.size(); idx++) {
            if (idx > 0) {
                out << ' ';
            }
            out << sa.at(idx);
        }
        out << std::endl;
        return out;
    }
#endif  // NDEBUG

    SortedArray() {
        capacity_ = SORTED_ARRAY_INIT_CAPACITY;
        size_ = 0;
        is_inited_ = false;
        array_ = nullptr;
    }

    SortedArray(size_t capacity) {
        capacity_ = capacity;
        size_ = 0;
        is_inited_ = false;
        array_ = nullptr;
    }

    ~SortedArray() {
        if (is_inited_) {
            destroy();
        }
        capacity_ = 0;
        size_ = 0;
    }

    // int deep_copy(const SortedArray &other)
    // {
    //   this->capacity_ = other.capacity_;
    //   this->size_ = other.size_;
    //   if (init() == E_OK) {
    //     memcpy(this->array_, other.array_, this->capacity_ *
    //     sizeof(*(this->array_))); return E_OK;
    //   }
    //   return E_OOM;
    // }

    int init() {
        if (is_inited_) {
            // log_err("init repeated.");
            ASSERT(!is_inited_);
        }
        array_ =
            (ValueType *)mem_alloc(capacity_ * sizeof(*(array_)), MOD_ARRAY);
        if (UNLIKELY(nullptr == array_)) {
            is_inited_ = false;
            // log_err("malloc failed.");
            return E_OOM;
        }
        is_inited_ = true;
        return E_OK;
    }

    void destroy() {
        capacity_ = 0;
        size_ = 0;
        if (is_inited_) {
            mem_free(array_);
            array_ = nullptr;
        }
        is_inited_ = false;
    }

   private:
    static size_t linear_calc(size_t input) {
        if (input < SORTED_ARRAY_GROWTH_THRESHOLD) {
            return input;
        }
        size_t res = 1;
        input /= 10;
        while (input) {
            input /= 10;
            res *= 10;
        }
        return res;
    }

    int extend() {
        // Check for a potential overflow
        uint64_t tmp = (uint64_t)(linear_calc(capacity_) + capacity_);
        if (tmp > SIZE_MAX) {
            // log_err("size overflow.");
            return E_OVERFLOW;
        }

        size_t new_capacity = (size_t)tmp;
        array_ =
            (ValueType *)mem_realloc(array_, new_capacity * sizeof(*(array_)));
        if (UNLIKELY(nullptr == array_)) {
            // log_err("realloc failed.");
            return E_OOM;
        }
        capacity_ = new_capacity;
        return E_OK;
    }

    int shrink() {
        size_t new_capacity = (size_t)(capacity_ / 2);
        // if the size passed to realloc is smaller than before, OS will
        // automatically release the rest memory
        array_ = (ValueType *)mem_realloc(
            array_,
            new_capacity *
                sizeof(*(array_)));  // TODO: user ourself's mem_alloc()
        if (UNLIKELY(nullptr == array_)) {
            // log_err("malloc failed.");
            return E_OOM;
        }
        capacity_ = new_capacity;
        return E_OK;
    }

   public:
    int insert(const ValueType &value) {
        if (size_ >= capacity_) {
            int ret = extend();
            if (ret != E_OK) {
                return ret;
            }
        }

        bool found = false;
        size_t pos = find(value, found);
        if (found) {
            // log_warn("the new value has already in the sorted array.");
            return E_ALREADY_EXIST;
        }

        if (pos == size_) {
            array_[pos] = value;
            size_++;
            return E_OK;
        }

        memmove(array_ + (pos + 1), array_ + pos,
                (size_ - pos) * sizeof(*(array_)));
        array_[pos] = value;
        size_++;
        return E_OK;
    }

    FORCE_INLINE ValueType at(size_t idx) {
        if (idx >= size_) {
            // log_err("index %lu is out of range, because size is %lu.", idx,
            // size_);
            ASSERT(idx < size_);
            return dummy_;
        }
        return array_[idx];
    }

    FORCE_INLINE ValueType operator[](size_t idx) {
        if (idx >= size_) {
            // log_err("index %lu is out of range, because size is %lu.", idx,
            // size_);
            ASSERT(idx < size_);
            return dummy_;
        }
        return array_[idx];
    }

    bool contain(const ValueType &target_val) {
        if (size_ == 0) {
            return false;
        }
        int64_t left = 0;
        int64_t right = size_ - 1;
        while (left <= right) {
            size_t mid = (left + right) / 2;
            ValueType mid_val = array_[mid];
            if (mid_val == target_val) {
                return true;
            } else if (mid_val < target_val) {
                left = mid + 1;
            } else if (mid_val > target_val) {
                right = mid - 1;
            }
        }
        return false;
    }

    /*
     * if the value is not found, then set 'found' is false ant return the first
     * bigger one's location
     */
    size_t find(const ValueType &target_val, bool &found) {
        if (size_ == 0) {
            found = false;
            return 0;
        }
        int64_t left = 0;
        int64_t right = size_ - 1;
        while (left <= right) {
            size_t mid = (left + right) / 2;
            ValueType mid_val = array_[mid];
            if (mid_val == target_val) {
                found = true;
                return mid;
            } else if (mid_val < target_val) {
                left = mid + 1;
            } else if (mid_val > target_val) {
                right = mid - 1;
            }
        }
        found = false;
        return (size_t)left;
    }

    ValueType remove(size_t idx) {
        if (idx >= size_) {
            // log_err("index %lu is out of range, because size is %lu.", idx,
            // size_);
            ASSERT(idx < size_);
        }
        if (capacity_ > SORTED_ARRAY_SHRINK_THRESHOLD &&
            size_ <= capacity_ / 4) {
            shrink();
        }
        ValueType ret = array_[idx];
        memmove(array_ + idx, array_ + idx + 1,
                (size_ - idx - 1) * sizeof(*(array_)));
        size_--;

        return ret;
    }

    int remove_value(const ValueType &value) {
        bool found = false;
        size_t pos = find(value, found);
        if (found) {
            remove(pos);
        } else {
            // log_warn("the value is not exist in the array.");
            return E_NOT_EXIST;
        }
        return E_OK;
    }

    FORCE_INLINE void clear() { size_ = 0; }

    ValueType get_min() {
        if (size_ == 0) {
            // log_warn("The array is empty now.");
            ASSERT(size_ > 0);
            return dummy_;
        }
        return array_[0];
    }

    ValueType &get_max() {
        if (size_ == 0) {
            // log_warn("The array is empty now.");
            ASSERT(size_ > 0);
            return dummy_;
        }
        return array_[size_ - 1];
    }

    FORCE_INLINE bool empty() const { return ATOMIC_LOAD(&size_) == 0; }

    FORCE_INLINE size_t size() const { return ATOMIC_LOAD(&size_); }

    FORCE_INLINE size_t capacity() const { return ATOMIC_LOAD(&capacity_); }

   private:
    size_t capacity_;
    size_t size_;
    ValueType *array_;
    ValueType dummy_;
    bool is_inited_;
};

}  // end namespace common
#endif  // COMMON_CONTAINER_SORTEDARRAY_H
