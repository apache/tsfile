#ifndef READER_TSFILE_EXECUTOR_H
#define READER_TSFILE_EXECUTOR_H

#include <map>

#include "file/read_file.h"
#include "query_data_set.h"
#include "query_executor.h"

namespace storage {

class TsFileExecutor  // : public QueryExecutor
{
   public:
    TsFileExecutor();
    ~TsFileExecutor();
    int init(ReadFile *read_file);
    int init(const std::string &file_path);
    int execute(QueryExpression *query_expr, QueryDataSet *&ret_qds);
    void destroy_query_data_set(QueryDataSet *qds);

   private:
    int execute_may_with_global_timefilter(QueryExpression *qe,
                                           QueryDataSet *&ret_qds);
    int execute_with_timegenerator(QueryExpression *qe, QueryDataSet *&ret_qds);

   private:
    TsFileIOReader io_reader_;
    QueryExpression *query_exprs_;
    std::vector<TsFileSeriesScanIterator *> data_scan_iter_;
    std::vector<common::TsBlock *> tsblocks_;
    std::vector<common::ColIterator *> time_iters_;
    std::vector<common::ColIterator *> value_iters_;
    bool is_inited_;
};

}  // namespace storage

#endif  // READER_TSFILE_EXECUTOR_H
