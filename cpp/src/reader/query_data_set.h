
#ifndef READER_QUERY_DATA_SET_H
#define READER_QUERY_DATA_SET_H

#include "common/row_record.h"

namespace storage {

class QueryDataSet {
   public:
    QueryDataSet() {}
    virtual ~QueryDataSet() {}
    virtual RowRecord *get_next() = 0;
};

}  // namespace storage

#endif  // READER_QUERY_DATA_SET_H
