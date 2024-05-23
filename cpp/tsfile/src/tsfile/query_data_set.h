
#ifndef STORAGE_TSFILE_READ_QUERY_QUERY_DATA_SET_H
#define STORAGE_TSFILE_READ_QUERY_QUERY_DATA_SET_H

#include "row_record.h"

namespace timecho
{
namespace storage
{

class QueryDataSet
{
public:
  QueryDataSet() {}
  virtual ~QueryDataSet() {}
  virtual RowRecord *get_next() = 0;
};

}
}

#endif // STORAGE_TSFILE_READ_QUERY_QUERY_DATA_SET_H
