#ifndef READER_FILTER_BASIC_UNARY_FILTER_H
#define READER_FILTER_BASIC_UNARY_FILTER_H

#include "reader/filter/filter.h"
#include "reader/filter/filter_type.h"

namespace storage {
template <typename T>
class UnaryFilter : public Filter {
   public:
    UnaryFilter() : Filter() {}
    UnaryFilter(T value, FilterType type) : Filter() {
        value_ = value;
        type_ = type;
    }
    virtual ~UnaryFilter() {}

    T get_value() { return value_; }

    void set_value(T value) { value_ = value; }

    FilterType get_filter_type() { return type_; }

    virtual bool satisfy(Statistic *statistic) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfy(long time, T value) {
        ASSERT(false);
        return false;
    }
    virtual bool satisfy_start_end_time(long start_time, long end_time) {
        ASSERT(false);
        return false;
    }
    virtual bool contain_start_end_time(long start_time, long end_time) {
        ASSERT(false);
        return false;
    }

   protected:
    T value_;
    FilterType type_;
};

}  // namespace storage

#endif  // READER_FILTER_BASIC_UNARY_FILTER_H
