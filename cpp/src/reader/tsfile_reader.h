
#ifndef READER_TSFILE_READER_H
#define READER_TSFILE_READER_H

#include "common/row_record.h"
#include "expression.h"
#include "file/read_file.h"

namespace storage {
class TsFileExecutor;
class ReadFile;
class QueryDataSet;
}  // namespace storage

namespace storage {

extern int libtsfile_init();
extern void libtsfile_destroy();

class TsFileReader {
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

}  // namespace storage

#endif // READER_TSFILE_READER
