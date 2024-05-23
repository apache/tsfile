#ifndef READER_FILTER_TIME_OPERATOR_H
#define READER_FILTER_TIME_OPERATOR_H

#include <algorithm>
#include <limits>

#include "filter.h"
#include "filter_type.h"

namespace storage {

class Statistic;
struct TimeRange;

class TimeBetween : public Filter {
   public:
    TimeBetween(int64_t value1, int64_t value2, bool not_between);

    ~TimeBetween();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();
    FilterType get_filter_type() { return type_; }

   private:
    int64_t value1_;
    int64_t value2_;
    bool not_;
    FilterType type_;
};

class TimeIn : public Filter {
   public:
    TimeIn(const std::vector<int64_t> &values, bool not_in);

    ~TimeIn();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();

    FilterType get_filter_type() { return type_; }

   private:
    std::vector<int64_t> values_;
    FilterType type_;
    bool not_;
};

class TimeEq : public Filter {
   public:
    explicit TimeEq(int64_t value);
    ~TimeEq();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();

    FilterType get_filter_type() { return type_; }

   private:
    int64_t value_;
    FilterType type_;
};

class TimeNotEq : public Filter {
   public:
    explicit TimeNotEq(int64_t value);
    ~TimeNotEq();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();
    FilterType get_filter_type() { return type_; }

   private:
    int64_t value_;
    FilterType type_;
};

class TimeGt : public Filter {
   public:
    explicit TimeGt(int64_t value);
    ~TimeGt();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();

    FilterType get_filter_type() { return type_; }

   private:
    int64_t value_;
    FilterType type_;
};

class TimeGtEq : public Filter {
   public:
    explicit TimeGtEq(int64_t value);
    ~TimeGtEq();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();
    void reset_value(int64_t val) { value_ = val; }
    FilterType get_filter_type() { return type_; }

   private:
    int64_t value_;
    FilterType type_;
};

class TimeLt : public Filter {
   public:
    explicit TimeLt(int64_t value);
    ~TimeLt();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();

    FilterType get_filter_type() { return type_; }

   private:
    int64_t value_;
    FilterType type_;
};

class TimeLtEq : public Filter {
   public:
    explicit TimeLtEq(int64_t value);
    ~TimeLtEq();

    bool satisfy(Statistic *statistic);

    bool satisfy(long time, int64_t value);

    bool satisfy_start_end_time(long start_time, long end_time);

    bool contain_start_end_time(long start_time, long end_time);

    std::vector<TimeRange *> *get_time_ranges();
    FilterType get_filter_type() { return type_; }

   private:
    int64_t value_;
    FilterType type_;
};

}  // namespace storage

#endif  // READER_FILTER_TIME_OPERATOR_H
