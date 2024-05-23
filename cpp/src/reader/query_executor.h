#ifndef READER_QUERY_EXECUTOR_H
#define READER_QUERY_EXECUTOR_H

#include <vector>

#include "common/row_record.h"
#include "expression.h"
#include "file/read_file.h"
#include "reader/tsfile_series_scan_iterator.h"

namespace storage {

class QueryExecutor {
   public:
    QueryExecutor() { query_exprs_ = nullptr; }

    virtual ~QueryExecutor() {
        int size = data_scan_iter_.size();
        for (int i = 0; i < size; ++i) {
            delete data_scan_iter_[i];
            data_scan_iter_[i] = nullptr;
        }
    }

    // virtual int init(QueryExpression *query_expr, ReadFile *read_file) {
    // ASSERT(false); return 0; };

    virtual RowRecord* execute() {
        ASSERT(false);
        return nullptr;
    };

    virtual void end() { ASSERT(false); };

   protected:
    QueryExpression* query_exprs_;
    std::vector<TsFileSeriesScanIterator*> data_scan_iter_;
    std::vector<common::TsBlock*> tsblocks_;
    std::vector<common::ColIterator*> time_iters_;
    std::vector<common::ColIterator*> value_iters_;
};

}  // namespace storage

#endif  // READER_QUERY_EXECUTOR_H
