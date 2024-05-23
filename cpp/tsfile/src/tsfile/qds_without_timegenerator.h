
#ifndef STORAGE_TSFILE_READ_QUERY_QDS_WITHOUT_TIMEGENERATOR_H
#define STORAGE_TSFILE_READ_QUERY_QDS_WITHOUT_TIMEGENERATOR_H

#include <vector>
#include <map>
#include "tsfile_io_reader.h"
#include "query_data_set.h"
#include "expression.h"

namespace timecho
{
namespace storage
{

class QDSWithoutTimeGenerator : public QueryDataSet
{
public:
  QDSWithoutTimeGenerator()
    : row_record_(nullptr),
      io_reader_(nullptr),
      qe_(nullptr),
      ssi_vec_(),
      tsblocks_(),
      time_iters_(),
      value_iters_(),
      heap_time_() {}
  ~QDSWithoutTimeGenerator() { destroy(); }
  int init(TsFileIOReader *io_reader,
           QueryExpression *qe);
  void destroy();
  RowRecord *get_next();

private:
  int get_next_tsblock(uint32_t index, bool alloc_mem);

private:
  RowRecord *row_record_;
  TsFileIOReader *io_reader_;
  QueryExpression *qe_;
  std::vector<TsFileSeriesScanIterator*> ssi_vec_;
  std::vector<common::TsBlock*> tsblocks_;
  std::vector<common::ColIterator*> time_iters_;
  std::vector<common::ColIterator*> value_iters_;
  std::multimap<int64_t, uint32_t>  heap_time_;   // key-->time, value-->path_index
};

}
}

#endif // STORAGE_TSFILE_READ_QUERY_QDS_WITHOUT_TIMEGENERATOR_H

