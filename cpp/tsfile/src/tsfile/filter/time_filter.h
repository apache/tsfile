#ifndef STORAGE_TSFILE_READ_FILTER_TIME_FILTER_H
#define STORAGE_TSFILE_READ_FILTER_TIME_FILTER_H

#include <stdint.h>

#include <limits>
#include <vector>

namespace timecho
{
namespace storage
{

class TimeEq;
class TimeGt;
class TimeGtEq;
class TimeLt;
class TimeLtEq;
class TimeNotEq;
class TimeIn;
class TimeBetween;

class TimeFilter
{
public:
  static TimeEq* eq(int64_t value);
  static TimeGt* gt(int64_t value);
  static TimeGtEq* gt_eq(int64_t value);
  static TimeLt* lt(int64_t value);
  static TimeLtEq* lt_eq(int64_t value);
  static TimeNotEq* not_eqt(int64_t value);
  static TimeIn* in(std::vector<int64_t> &values, bool not_filter);
  static TimeBetween* between(int64_t value1, int64_t value2, bool not_filter);
};

}  // storage
}  // timecho

#endif // STORAGE_TSFILE_READ_FILTER_TIME_FILTER_H