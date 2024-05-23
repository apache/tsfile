#include "time_operator.h"

#include "common/statistic.h"
#include "utils/storage_utils.h"

namespace storage {

TimeBetween::TimeBetween(int64_t value1, int64_t value2, bool not_between)
    : value1_(value1), value2_(value2), not_(not_between), type_(TIME_FILTER) {}

TimeBetween::~TimeBetween() {}

bool TimeBetween::satisfy(Statistic *statistic) {
    if (not_) {
        return statistic->end_time_ < value1_ ||
               statistic->start_time_ > value2_;
    } else {
        return statistic->end_time_ >= value1_ &&
               statistic->start_time_ <= value2_;
    }
}

bool TimeBetween::satisfy(long time, int64_t value) {
    return (value1_ <= time) && (time <= value2_) ^ not_;
}

bool TimeBetween::satisfy_start_end_time(long start_time, long end_time) {
    if (not_) {
        return start_time < value1_ || end_time > value2_;
    } else {
        return end_time >= value1_ && start_time <= value2_;
    }
}

bool TimeBetween::contain_start_end_time(long start_time, long end_time) {
    if (not_) {
        return end_time < value1_ || start_time > value2_;
    } else {
        return start_time >= value1_ && end_time <= value2_;
    }
}

std::vector<TimeRange *> *TimeBetween::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    if (not_) {
        if (value1_ != std::numeric_limits<int64_t>::min()) {
            result->push_back(new TimeRange(std::numeric_limits<int64_t>::min(),
                                            value1_ - 1));
        }
        if (value2_ != std::numeric_limits<int64_t>::max()) {
            result->push_back(new TimeRange(
                value2_ + 1, std::numeric_limits<int64_t>::max()));
        }
    } else {
        result->push_back(new TimeRange(value1_, value2_));
    }
    return result;
}

// TimeIn
TimeIn::TimeIn(const std::vector<int64_t> &values, bool not_in)
    : values_(values), type_(TIME_FILTER), not_(not_in) {}

TimeIn::~TimeIn() {}

bool TimeIn::satisfy(Statistic *statistic) { return true; }

bool TimeIn::satisfy(long time, int64_t value) {
    std::vector<int64_t>::iterator it =
        find(values_.begin(), values_.end(), time);
    bool result = (it != values_.end() ? true : false);
    return result != not_;
}

bool TimeIn::satisfy_start_end_time(long start_time, long end_time) {
    return true;
}

bool TimeIn::contain_start_end_time(long start_time, long end_time) {
    return true;
}

std::vector<TimeRange *> *TimeIn::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    int size = values_.size();
    for (int i = 0; i < size; ++i) {
        result->push_back(new TimeRange(values_[i], values_[i]));
    }
    return result;
}

// TimeEq
TimeEq::TimeEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeEq::~TimeEq() {}

bool TimeEq::satisfy(Statistic *statistic) {
    return value_ >= statistic->start_time_ && value_ <= statistic->end_time_;
}

bool TimeEq::satisfy(long time, int64_t value) { return value_ == time; }

bool TimeEq::satisfy_start_end_time(long start_time, long end_time) {
    return value_ <= end_time && value_ >= start_time;
}

bool TimeEq::contain_start_end_time(long start_time, long end_time) {
    return value_ == start_time && value_ == end_time;
}

std::vector<TimeRange *> *TimeEq::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    result->push_back(new TimeRange(value_, value_));
    return result;
}

// TimeNotEq
TimeNotEq::TimeNotEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeNotEq::~TimeNotEq() {}

bool TimeNotEq::satisfy(Statistic *statistic) {
    return !(value_ == statistic->start_time_ &&
             value_ == statistic->end_time_);
}

bool TimeNotEq::satisfy(long time, int64_t value) { return !(value_ == time); }

bool TimeNotEq::satisfy_start_end_time(long start_time, long end_time) {
    return value_ != end_time && value_ != start_time;
}

bool TimeNotEq::contain_start_end_time(long start_time, long end_time) {
    return value_ < start_time || value_ > end_time;
}

std::vector<TimeRange *> *TimeNotEq::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    if (value_ == std::numeric_limits<int64_t>::min()) {
        result->push_back(
            new TimeRange(value_ + 1, std::numeric_limits<int64_t>::max()));
    } else if (value_ == std::numeric_limits<int64_t>::max()) {
        result->push_back(
            new TimeRange(std::numeric_limits<int64_t>::min(), value_ - 1));
    } else {
        result->push_back(
            new TimeRange(std::numeric_limits<int64_t>::min(), value_ - 1));
        result->push_back(
            new TimeRange(value_ + 1, std::numeric_limits<int64_t>::max()));
    }
    return result;
}

// TimeGt
TimeGt::TimeGt(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeGt::~TimeGt() {}

bool TimeGt::satisfy(Statistic *statistic) {
    return value_ < statistic->end_time_;
}

bool TimeGt::satisfy(long time, int64_t value) { return value_ < time; }

bool TimeGt::satisfy_start_end_time(long start_time, long end_time) {
    return value_ < end_time;
}

bool TimeGt::contain_start_end_time(long start_time, long end_time) {
    return value_ < start_time;
}

std::vector<TimeRange *> *TimeGt::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    if (value_ != std::numeric_limits<int64_t>::max()) {
        result->push_back(
            new TimeRange(value_ + 1, std::numeric_limits<int64_t>::max()));
    }
    return result;
}

// TimeGtEq
TimeGtEq::TimeGtEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeGtEq::~TimeGtEq() {}

bool TimeGtEq::satisfy(Statistic *statistic) {
    return value_ <= statistic->end_time_;
}

bool TimeGtEq::satisfy(long time, int64_t value) { return value_ <= time; }

bool TimeGtEq::satisfy_start_end_time(long start_time, long end_time) {
    return value_ <= end_time;
}

bool TimeGtEq::contain_start_end_time(long start_time, long end_time) {
    return value_ <= start_time;
}

std::vector<TimeRange *> *TimeGtEq::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    result->push_back(
        new TimeRange(value_, std::numeric_limits<int64_t>::max()));
    return result;
}

// TimeLt
TimeLt::TimeLt(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeLt::~TimeLt() {}

bool TimeLt::satisfy(Statistic *statistic) {
    return value_ > statistic->start_time_;
}

bool TimeLt::satisfy(long time, int64_t value) { return value_ > time; }

bool TimeLt::satisfy_start_end_time(long start_time, long end_time) {
    return value_ > start_time;
}

bool TimeLt::contain_start_end_time(long start_time, long end_time) {
    return value_ > end_time;
}

std::vector<TimeRange *> *TimeLt::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    if (value_ != std::numeric_limits<int64_t>::min()) {
        result->push_back(
            new TimeRange(std::numeric_limits<int64_t>::min(), value_ - 1));
    }
    return result;
}

// TimeLtEq
TimeLtEq::TimeLtEq(int64_t value) : value_(value), type_(TIME_FILTER) {}
TimeLtEq::~TimeLtEq() {}

bool TimeLtEq::satisfy(Statistic *statistic) {
    return value_ >= statistic->start_time_;
}

bool TimeLtEq::satisfy(long time, int64_t value) { return value_ >= time; }

bool TimeLtEq::satisfy_start_end_time(long start_time, long end_time) {
    return value_ >= start_time;
}

bool TimeLtEq::contain_start_end_time(long start_time, long end_time) {
    return value_ >= end_time;
}

std::vector<TimeRange *> *TimeLtEq::get_time_ranges() {
    std::vector<TimeRange *> *result = new std::vector<TimeRange *>();
    result->push_back(
        new TimeRange(std::numeric_limits<int64_t>::min(), value_));
    return result;
}

}  // namespace storage
