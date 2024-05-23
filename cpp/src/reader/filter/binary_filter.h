#ifndef READER_FILTER_BASIC_BINARY_FILTER_H
#define READER_FILTER_BASIC_BINARY_FILTER_H

#include "filter.h"
#include "filter_type.h"

namespace storage {
class BinaryFilter : public Filter {
   public:
    BinaryFilter() : Filter() {}
    BinaryFilter(Filter *left, Filter *right)
        : Filter(), left_(left), right_(right) {}
    virtual ~BinaryFilter() {}

    void set_left(Filter *left) { left_ = left; }
    void set_right(Filter *right) { right_ = right; }
    Filter get_left() { return *left_; }
    Filter get_right() { return *right_; }

   protected:
    Filter *left_;
    Filter *right_;
};

}  // namespace storage

#endif  // READER_FILTER_BASIC_BINARY_FILTER_H
