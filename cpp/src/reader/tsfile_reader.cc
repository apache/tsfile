#include "tsfile_reader.h"

#include "tsfile_executor.h"

using namespace common;
using namespace storage;

namespace storage {

TsFileReader::TsFileReader() : read_file_(nullptr), tsfile_executor_(nullptr) {}

TsFileReader::~TsFileReader() {
    if (tsfile_executor_ != nullptr) {
        delete tsfile_executor_;
        tsfile_executor_ = nullptr;
    }
    if (read_file_ != nullptr) {
        read_file_->close();
        delete read_file_;
        read_file_ = nullptr;
    }
}

int TsFileReader::open(const std::string &file_path) {
    int ret = E_OK;
    read_file_ = new storage::ReadFile;
    tsfile_executor_ = new storage::TsFileExecutor();
    if (RET_FAIL(read_file_->open(file_path))) {
        std::cout << "filed to open file " << ret << std::endl;
    } else if (RET_FAIL(tsfile_executor_->init(read_file_))) {
        std::cout << "filed to init " << ret << std::endl;
    }
    return ret;
}

int TsFileReader::query(QueryExpression *qe, QueryDataSet *&ret_qds) {
    return tsfile_executor_->execute(qe, ret_qds);
}

void TsFileReader::destroy_query_data_set(storage::QueryDataSet *qds) {
    tsfile_executor_->destroy_query_data_set(qds);
}

}  // namespace storage
