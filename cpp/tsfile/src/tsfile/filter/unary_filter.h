#ifndef STORAGE_TSFILE_READ_FILTER_BASIC_UNARY_FILTER_H
#define STORAGE_TSFILE_READ_FILTER_BASIC_UNARY_FILTER_H

#include "storage/tsfile/filter/filter.h"
#include "storage/tsfile/filter/filter_type.h"

namespace timecho
{
namespace storage
{
template <typename T>
class UnaryFilter : public Filter
{
public:
  UnaryFilter() : Filter() {}
  UnaryFilter(T value, FilterType type) : Filter()
  {
    value_ = value;
    type_ = type;
  }
  virtual ~UnaryFilter() {}

  T get_value() { return value_; }

  void set_value(T value) { value_ = value; }

  FilterType get_filter_type() { return type_; }

  virtual bool satisfy(Statistic *statistic) { ASSERT(false); return false; }
  virtual bool satisfy(long time, T value) { ASSERT(false); return false; }
  virtual bool satisfy_start_end_time(long start_time, long end_time) { ASSERT(false); return false; }
  virtual bool contain_start_end_time(long start_time, long end_time) { ASSERT(false); return false; }

protected:
  T value_;
  FilterType type_;
};

}  // storage
}  // timecho

#endif  // STORAGE_TSFILE_READ_FILTER_BASIC_UNARY_FILTER_H
