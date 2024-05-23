#ifndef READER_FILTER_OPERATOR_BETWEEN_H
#define READER_FILTER_OPERATOR_BETWEEN_H

#include "reader/filter/binary_filter.h"

namespace storage {
template <typename T>
class Between : public Filter {
   public:
    Between() : Filter() {}
    Between(T value1, T value2, FilterType filter_type, bool not_between)
        : Filter(),
          value1_(value1),
          value2_(value2),
          not_(not_between),
          type_(filter_type) {}

    virtual ~Between() {}

    bool satisfy(Statistic *statistic) {
        if (type_ == TIME_FILTER) {
            if (not_) {
                return statistic->end_time_ < value1_ ||
                       statistic->start_time_ > value2_;
            } else {
                return statistic->end_time_ >= value1_ &&
                       statistic->start_time_ <= value2_;
            }
        } else {
            if (statistic->get_type() == common::TEXT ||
                statistic->get_type() == common::BOOLEAN) {
                return true;
            } else {
                if (statistic->get_type() == common::INT32) {
                    Int32Statistic *stat =
                        dynamic_cast<Int32Statistic *>(statistic);
                    if (not_) {
                        return ((stat->min_value_ < value1_) ||
                                (stat->max_value_ > value2_));
                    } else {
                        return ((stat->max_value_ >= value1_) &&
                                (stat->min_value_ <= value2_));
                    }
                } else if (statistic->get_type() == common::INT64) {
                    Int64Statistic *stat =
                        dynamic_cast<Int64Statistic *>(statistic);
                    if (not_) {
                        return ((stat->min_value_ < value1_) ||
                                (stat->max_value_ > value2_));
                    } else {
                        return ((stat->max_value_ >= value1_) &&
                                (stat->min_value_ <= value2_));
                    }
                } else if (statistic->get_type() == common::FLOAT) {
                    FloatStatistic *stat =
                        dynamic_cast<FloatStatistic *>(statistic);
                    if (not_) {
                        return ((stat->min_value_ < value1_) ||
                                (stat->max_value_ > value2_));
                    } else {
                        return ((stat->max_value_ >= value1_) &&
                                (stat->min_value_ <= value2_));
                    }
                } else if (statistic->get_type() == common::DOUBLE) {
                    DoubleStatistic *stat =
                        dynamic_cast<DoubleStatistic *>(statistic);
                    if (not_) {
                        return ((stat->min_value_ < value1_) ||
                                (stat->max_value_ > value2_));
                    } else {
                        return ((stat->max_value_ >= value1_) &&
                                (stat->min_value_ <= value2_));
                    }
                }
            }
        }
    }

    bool satisfy(long time, Object value) {
        Object v = (type_ == TIME_FILTER ? time : value);
        return (value1_ <= v) && (v <= value2_) ^ not_;
    }

    bool satisfy_start_end_time(long start_time, long end_time) {
        if (type_ == TIME_FILTER) {
            if (not_) {
                return start_time < value1_ || end_time > value2_;
            } else {
                return end_time >= value1_ && start_time <= value2_;
            }
        } else {
            return true;
        }
    }

    bool contain_start_end_time(long start_time, long end_time) {
        if (type_ == TIME_FILTER) {
            if (not_) {
                return end_time < value1_ || start_time > value2_;
            } else {
                return start_time >= value1_ && end_time <= value2_;
            }
        } else {
            return true;
        }
    }

   protected:
    T value1_;
    T value2_;
    bool not_;
    FilterType type_;
};

}  // namespace storage

#endif  // READER_FILTER_OPERATOR_BETWEEN_H
