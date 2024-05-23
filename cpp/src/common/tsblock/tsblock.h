#ifndef COMMON_TSBLOCK_TSBLOCK_H
#define COMMON_TSBLOCK_TSBLOCK_H

#include <stdint.h>

#include "common/allocator/byte_stream.h"
#include "common/container/byte_buffer.h"
#include "common/global.h"
#include "common/logger/elog.h"
#include "tuple_desc.h"
#include "vector/fixed_length_vector.h"
#include "vector/variable_length_vector.h"
#include "vector/vector.h"

namespace common {
class TsBlock {
   public:
    friend class RowIterator;
    friend class ColIterator;

    friend class RowAppender;
    friend class ColAppender;
    /*
     * row_count: If we can clearly estimate the number of tsblock rows,
     *            such as limit scenarios, such as based on statistical
     * information, such as insert scenarios, etc. Then we will use the given
     * number of rows
     */
    explicit TsBlock(TupleDesc *tupledesc, uint32_t max_row_count = 0)
        : capacity_(g_config_value_.tsblock_max_memory_),
          row_count_(0),
          max_row_count_(max_row_count),
          tuple_desc_(tupledesc) {}

    virtual ~TsBlock() {
        int size = vectors_.size();
        for (int i = 0; i < size; ++i) {
            delete vectors_[i];
        }
    }

    FORCE_INLINE uint32_t get_row_count() const { return row_count_; }

    FORCE_INLINE TupleDesc *get_tuple_desc() const { return tuple_desc_; }

    FORCE_INLINE Vector *get_vector(uint32_t index) { return vectors_[index]; }

    FORCE_INLINE uint32_t get_column_count() const {
        return tuple_desc_->get_column_count();
    }

    FORCE_INLINE uint32_t get_max_row_count() const { return max_row_count_; }

    FORCE_INLINE uint32_t get_capacity() const { return capacity_; }

    FORCE_INLINE void update_capacity(uint32_t extend_size) {
        capacity_ += extend_size;
    }

    // need to call flush_row_count after using colappender
    FORCE_INLINE int flush_row_count(uint32_t row_count) {
        int errnum = E_OK;
        if (row_count_ == 0) {
            row_count_ = row_count;
        } else if (row_count_ != row_count) {
            LOGE("Inconsistent number of rows in two columns");
            errnum = E_TSBLOCK_DATA_INCONSISTENCY;
        }
        return errnum;
    }

    FORCE_INLINE void reset() {
        int size = vectors_.size();
        for (int i = 0; i < size; ++i) {
            vectors_[i]->reset();
        }
        row_count_ = 0;
    }

    int init();
    void tsblock_to_json(ByteStream *byte_stream);

    std::string debug_string();

   private:
    int build_vector(common::TSDataType type, uint32_t row_count);
    void write_data(ByteStream *__restrict byte_stream, char *__restrict val,
                    uint32_t len, bool has_null, TSDataType type);

   private:
    uint32_t capacity_;   // maximum memory capacity
    uint32_t row_count_;  // real row count
    uint32_t max_row_count_;

    common::BitMap select_list_;
    TupleDesc *tuple_desc_;
    std::vector<Vector *> vectors_;
};

class RowAppender {
   public:
    explicit RowAppender(TsBlock *tsblock) : tsblock_(tsblock) {}
    ~RowAppender() {}

    // todo:(yanghao) maybe need to consider select-list
    FORCE_INLINE bool add_row() {
        if (LIKELY(tsblock_->row_count_ < tsblock_->max_row_count_)) {
            ++tsblock_->row_count_;
            return true;
        } else {
            return false;
        }
    }
    FORCE_INLINE void backoff_add_row() {
        ASSERT(tsblock_->row_count_ > 0);
        tsblock_->row_count_--;
    }

    FORCE_INLINE void append(uint32_t slot_index, const char *value,
                             uint32_t len) {
        ASSERT(slot_index < tsblock_->tuple_desc_->get_column_count());
        Vector *vec = tsblock_->vectors_[slot_index];
        vec->append(value, len);
    }

    FORCE_INLINE void append_null(uint32_t slot_index) {
        Vector *vec = tsblock_->vectors_[slot_index];
        vec->set_null(tsblock_->row_count_ - 1);
    }

   private:
    TsBlock *tsblock_;
};

class ColAppender {
   public:
    ColAppender(uint32_t column_index, TsBlock *tsblock)
        : column_index_(column_index), column_row_count_(0), tsblock_(tsblock) {
        ASSERT(column_index < tsblock_->tuple_desc_->get_column_count());
        vec_ = tsblock_->vectors_[column_index];
    }

    ~ColAppender() {}

    // todo:(yanghao) maybe need to consider select-list
    FORCE_INLINE bool add_row() {
        if (LIKELY(column_row_count_ < tsblock_->max_row_count_)) {
            ++column_row_count_;
            return true;
        } else {
            return false;
        }
    }

    FORCE_INLINE void append(const char *value, uint32_t len) {
        vec_->append(value, len);
    }

    FORCE_INLINE void append_null() { vec_->set_null(column_row_count_ - 1); }

    FORCE_INLINE uint32_t get_col_row_count() { return column_row_count_; }
    FORCE_INLINE uint32_t get_column_index() { return column_index_; }

   private:
    uint32_t column_index_;
    uint32_t column_row_count_;
    TsBlock *tsblock_;
    Vector *vec_;
};

// todo:(yanghao) need to deal with select-list
class RowIterator {
   public:
    explicit RowIterator(TsBlock *tsblock) : tsblock_(tsblock), row_id_(0) {
        column_count_ = tsblock_->tuple_desc_->get_column_count();
    }

    ~RowIterator() {
        /*
         * if use RowIterator and ColIterator at the same time,
         * need to reset the offset after one is used,
         * otherwise it will cause the offset to be wrong
         */
        for (uint32_t i = 0; i < column_count_; ++i) {
            tsblock_->vectors_[i]->reset_offset();
        }
    }

    FORCE_INLINE bool end() { return row_id_ >= tsblock_->row_count_; }

    FORCE_INLINE void next() {
        ASSERT(row_id_ < tsblock_->row_count_);
        ++row_id_;
        for (uint32_t i = 0; i < column_count_; ++i) {
            tsblock_->vectors_[i]->update_offset();
        }
    }

    FORCE_INLINE char *read(uint32_t slot_index, uint32_t *__restrict len,
                            bool *__restrict null) {
        ASSERT(slot_index < column_count_);
        Vector *vec = tsblock_->vectors_[slot_index];
        return vec->read(len, null, row_id_);
    }

    std::string debug_string();  // for debug

   private:
    TsBlock *tsblock_;
    uint32_t row_id_;  // The line number currently being reader
    uint32_t column_count_;
};

// todo:(yanghao) need to deal with select-list
class ColIterator {
   public:
    ColIterator(uint32_t column_index, const TsBlock *tsblock)
        : column_index_(column_index), row_id_(0), tsblock_(tsblock) {
        ASSERT(column_index < tsblock_->tuple_desc_->get_column_count());
        vec_ = tsblock_->vectors_[column_index];
    }

    ~ColIterator() { vec_->reset_offset(); }

    FORCE_INLINE bool end() const { return row_id_ >= tsblock_->row_count_; }

    FORCE_INLINE void next() {
        ++row_id_;
        vec_->update_offset();
    }

    FORCE_INLINE bool has_null() { return vec_->has_null(); }

    FORCE_INLINE TSDataType get_data_type() { return vec_->get_vector_type(); }

    FORCE_INLINE char *read(uint32_t *__restrict len, bool *__restrict null) {
        return vec_->read(len, null, row_id_);
    }

    FORCE_INLINE char *read(uint32_t *len) { return vec_->read(len); }

    FORCE_INLINE uint32_t get_column_index() { return column_index_; }

   private:
    uint32_t column_index_;
    uint32_t row_id_;
    const TsBlock *tsblock_;
    Vector *vec_;
};

int merge_tsblock_by_row(TsBlock *sea, TsBlock *river);

}  // end namespace common
#endif  // COMMON_TSBLOCK_TSBLOCK_H
