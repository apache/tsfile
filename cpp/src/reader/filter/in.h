#ifndef READER_FILTER_OPERATOR_IN_H
#define READER_FILTER_OPERATOR_IN_H

#include <vector>

#include "filter/binary_filter.h"

namespace storage {
template <typename T>
class In : public Filter {
   public:
    In() {}
    In(std::vector<T> &values, FilterType type, bool not_in)
        : values_(values), type_(type), not_(not_in) {}
    virtual ~In() {}

    bool satisfy(Statistic *statistic) { return true; }

    bool satisfy(long time, Object value) {
        Object v = (filterType == TIME_FILTER ? time : value);
        std::vector<T>::iterator it = find(values_.begin(), values_.end(), v);
        bool result = (it != values_.end() ? true : false);
        return result != not_;
    }

    bool satisfy_start_end_time(long start_time, long end_time) { return true; }

    bool contain_start_end_time(long start_time, long end_time) { return true; }

   protected:
    std::vector<T> values_;
    FilterType type_;
    bool not_;
};
}  // namespace storage

#endif  // READER_FILTER_OPERATOR_IN_H
