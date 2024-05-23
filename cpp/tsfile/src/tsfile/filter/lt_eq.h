#ifndef STORAGE_TSFILE_READ_FILTER_OPERATOR_LT_EQ_H
#define STORAGE_TSFILE_READ_FILTER_OPERATOR_LT_EQ_H

#include "storage/tsfile/filter/unary_filter.h"

namespace timecho
{
namespace storage
{
template <typename T>
class LtEq : public UnaryFilter<T>
{
public:
  LtEq() {}
  LtEq(T value, FilterType type) { UnaryFilter(value, type); }
  virtual ~LtEq() {}

  bool satisfy(Statistic *statistic)
  {
    if (type_ == TIME_FILTER) {
      return value_ >= statistic->start_time_;
    } else {
      if (statistic->get_type() == common::TEXT || statistic->get_type() == common::BOOLEAN) {
        return true;
      } else {
        // todo value filter
        return true;
      }
    }
  }

  bool satisfy(long time, Object value)
  {
    Object v = (type_ == TIME_FILTER ? time : value);
    return value_ >= v;
  }

  bool satisfy_start_end_time(long start_time, long end_time)
  {
    if (type_ == TIME_FILTER) {
      return value_ >= start_time;
    } else {
      return true;
    }
  }

  bool contain_start_end_time(long start_time, long end_time)
  {
    if (type_ == TIME_FILTER) {
      return value_ >= end_time;
    } else {
      return true;
    }
  }
};

}  // storage
}  // timecho

#endif // STORAGE_TSFILE_READ_FILTER_OPERATOR_LT_EQ_H