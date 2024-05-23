
#ifndef MAIN_LIBTSFILE_TSFILE_READER_H
#define MAIN_LIBTSFILE_TSFILE_READER_H


#include "row_record.h"
#include "expression.h"
#include "read_file.h"

namespace timecho
{
namespace storage
{
class TsFileExecutor;
class ReadFile;
class QueryDataSet;
}
}

namespace timecho
{
namespace storage
{

extern int libtsfile_init();
extern void libtsfile_destroy();

class TsFileReader
{
public:
  TsFileReader();
  ~TsFileReader();
  int open(const std::string &file_path);
  int query(storage::QueryExpression *qe, QueryDataSet *&ret_qds);
  void destroy_query_data_set(QueryDataSet *qds);

private:
  storage::ReadFile *read_file_;
  storage::TsFileExecutor *tsfile_executor_;
};

}
}

#endif
