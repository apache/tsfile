#ifndef STORAGE_TSFILE_READ_FILTER_BASIC_FILTER_H
#define STORAGE_TSFILE_READ_FILTER_BASIC_FILTER_H

#include <vector>

#include "common/db_common.h"

namespace timecho
{
namespace storage
{

struct TimeRange;
class Statistic;

class Filter
{
public:
  Filter() {}
  virtual ~Filter() {}

  virtual bool satisfy(Statistic *statistic) { ASSERT(false); return false; }
  virtual bool satisfy(long time, int64_t value) { ASSERT(false); return false; }
  virtual bool satisfy_start_end_time(long start_time, long end_time) { ASSERT(false); return false; }
  virtual bool contain_start_end_time(long start_time, long end_time) { ASSERT(false); return false; }
  virtual std::vector<TimeRange*>* get_time_ranges() { ASSERT(false); return nullptr; }
};

}  // storage
}  // timecho

#endif // STORAGE_TSFILE_READ_FILTER_BASIC_FILTER_H
