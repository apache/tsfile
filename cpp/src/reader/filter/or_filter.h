#ifndef READER_FILTER_OPERATOR_OR_FILTER_H
#define READER_FILTER_OPERATOR_OR_FILTER_H

#include "binary_filter.h"
// #include "storage/storage_utils.h"

namespace storage {

class OrFilter : public BinaryFilter {
   public:
    OrFilter() {}
    OrFilter(Filter *left, Filter *right) { BinaryFilter(left, right); }
    ~OrFilter() {}

    FORCE_INLINE bool satisfy(Statistic *statistic) {
        return left_->satisfy(statistic) || right_->satisfy(statistic);
    }

    FORCE_INLINE bool satisfy(long time, int64_t value) {
        return left_->satisfy(time, value) || right_->satisfy(time, value);
    }

    FORCE_INLINE bool satisfy_start_end_time(long start_time, long end_time) {
        return left_->satisfy_start_end_time(start_time, end_time) ||
               right_->satisfy_start_end_time(start_time, end_time);
    }

    FORCE_INLINE bool contain_start_end_time(long start_time, long end_time) {
        return left_->contain_start_end_time(start_time, end_time) ||
               right_->contain_start_end_time(start_time, end_time);
    }

    std::vector<TimeRange *> *get_time_ranges() {
        std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
        std::vector<TimeRange *> *left_time_ranges = left_->get_time_ranges();
        std::vector<TimeRange *> *right_time_ranges = right_->get_time_ranges();

        int left_index = 0, right_index = 0;
        int left_size = left_time_ranges->size();
        int right_size = right_time_ranges->size();

        while (left_index < left_size || right_index < right_size) {
            TimeRange *left_range = left_time_ranges->at(left_index);
            TimeRange *right_range = right_time_ranges->at(right_index);

            if (left_range->end_time_ < right_range->start_time_) {
                left_index++;
            } else if (right_range->end_time_ < left_range->start_time_) {
                right_index++;
            } else {
                TimeRange *intersection = new TimeRange(
                    std::max(left_range->start_time_, right_range->start_time_),
                    std::min(left_range->end_time_, right_range->end_time_));
                result->push_back(intersection);
                if (left_range->end_time_ <= intersection->end_time_) {
                    left_index++;
                }
                if (right_range->end_time_ <= intersection->end_time_) {
                    right_index++;
                }
            }
        }
        return result;
    }

   private:
};

}  // namespace storage

#endif  // READER_FILTER_OPERATOR_OR_FILTER_H
