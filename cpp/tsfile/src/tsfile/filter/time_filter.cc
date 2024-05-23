#include "time_filter.h"

#include "time_operator.h"

namespace timecho
{
namespace storage
{

TimeEq* TimeFilter::eq(int64_t value) { return new TimeEq(value); }

TimeGt* TimeFilter::gt(int64_t value) { return new TimeGt(value); }

TimeGtEq* TimeFilter::gt_eq(int64_t value) { return new TimeGtEq(value); }

TimeLt* TimeFilter::lt(int64_t value) { return new TimeLt(value); }

TimeLtEq* TimeFilter::lt_eq(int64_t value) { return new TimeLtEq(value); }

TimeNotEq* TimeFilter::not_eqt(int64_t value) { return new TimeNotEq(value); }

TimeIn* TimeFilter::in(std::vector<int64_t> &values, bool not_filter) { return new TimeIn(values, not_filter); }

TimeBetween* TimeFilter::between(int64_t value1, int64_t value2, bool not_filter)
{
  return new TimeBetween(value1, value2, not_filter);
}

}  // storage
}  // timecho
