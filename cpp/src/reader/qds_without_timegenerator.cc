
#include "qds_without_timegenerator.h"

#include "utils/util_define.h"

using namespace common;

namespace storage {

int QDSWithoutTimeGenerator::init(TsFileIOReader *io_reader,
                                  QueryExpression *qe) {
    int ret = E_OK;  // cppcheck-suppress unreadVariable
    io_reader_ = io_reader;
    qe_ = qe;

    std::vector<Path> paths = qe_->selected_series_;
    size_t origin_path_count = paths.size();
    std::vector<Path> valid_paths;
    Expression *global_time_expression = qe->expression_;
    Filter *global_time_filter = nullptr;
    if (global_time_expression != nullptr) {
        global_time_filter = global_time_expression->filter_;
    }
    for (size_t i = 0; i < origin_path_count; i++) {
        TsFileSeriesScanIterator *ssi = nullptr;
        ret = io_reader_->alloc_ssi(paths[i].device_, paths[i].measurement_,
                                    ssi, global_time_filter);
        if (ret != 0) {
            return ret;
        } else {
            ssi_vec_.push_back(ssi);
            valid_paths.push_back(paths[i]);
        }
    }

    size_t path_count = valid_paths.size();
    row_record_ = new RowRecord(path_count);
    tsblocks_.resize(path_count);
    time_iters_.resize(path_count);
    value_iters_.resize(path_count);

    for (size_t i = 0; i < path_count; i++) {
        get_next_tsblock(i, true);
    }
    return E_OK;  // ignore invalid timeseries
}

void QDSWithoutTimeGenerator::destroy() {
    if (row_record_ != nullptr) {
        delete row_record_;
        row_record_ = nullptr;
    }
    for (size_t i = 0; i < time_iters_.size(); i++) {
        delete time_iters_[i];
        time_iters_[i] = nullptr;
    }
    time_iters_.clear();
    for (size_t i = 0; i < value_iters_.size(); i++) {
        delete value_iters_[i];
        value_iters_[i] = nullptr;
    }
    value_iters_.clear();
    heap_time_.clear();

    ASSERT(ssi_vec_.size() == tsblocks_.size());
    for (size_t i = 0; i < ssi_vec_.size(); i++) {
        ssi_vec_[i]->revert_tsblock();
    }
    for (size_t i = 0; i < ssi_vec_.size(); i++) {
        TsFileSeriesScanIterator *ssi = ssi_vec_[i];
        io_reader_->revert_ssi(ssi);
    }
    ssi_vec_.clear();
}

RowRecord *QDSWithoutTimeGenerator::get_next() {
    row_record_->reset();
    if (heap_time_.size() == 0) {
        return nullptr;
    }
    int64_t time = heap_time_.begin()->first;
    row_record_->set_timestamp(time);

    uint32_t count = heap_time_.count(time);
    std::multimap<int64_t, uint32_t>::iterator iter = heap_time_.find(time);
    for (uint32_t i = 0; i < count; ++i) {
        uint32_t len = 0;
        row_record_->get_field(iter->second)
            ->set_value(value_iters_[iter->second]->get_data_type(),
                        value_iters_[iter->second]->read(&len));
        value_iters_[iter->second]->next();
        if (!time_iters_[iter->second]->end()) {
            int64_t timev = *(int64_t *)(time_iters_[iter->second]->read(&len));
            heap_time_.insert(std::make_pair(timev, iter->second));
            time_iters_[iter->second]->next();
        } else {
            get_next_tsblock(iter->second, false);
        }
        std::multimap<int64_t, uint32_t>::iterator cur = iter;
        iter++;  // cppcheck-suppress postfixOperator
        heap_time_.erase(cur);
    }
    return row_record_;
}

int QDSWithoutTimeGenerator::get_next_tsblock(uint32_t index, bool alloc_mem) {
    if (tsblocks_[index] != nullptr) {
        delete time_iters_[index];
        time_iters_[index] = nullptr;
        delete value_iters_[index];
        value_iters_[index] = nullptr;
        tsblocks_[index]->reset();
    }

    int ret = ssi_vec_[index]->get_next(tsblocks_[index], alloc_mem);
    if (IS_SUCC(ret)) {
        time_iters_[index] = new ColIterator(0, tsblocks_[index]);
        uint32_t len = 0;
        int64_t time = *(int64_t *)(time_iters_[index]->read(&len));
        time_iters_[index]->next();
        heap_time_.insert(std::pair<uint64_t, uint32_t>(time, index));
        value_iters_[index] = new ColIterator(1, tsblocks_[index]);
    } else {
        if (time_iters_[index]) {
            delete time_iters_[index];
            time_iters_[index] = nullptr;
        }
        if (value_iters_[index]) {
            delete value_iters_[index];
            value_iters_[index] = nullptr;
        }
        if (tsblocks_[index]) {
            ssi_vec_[index]->destroy();
            tsblocks_[index] = nullptr;
        }
        ret = E_OK;  // TODO
    }
    return ret;
}

}  // namespace storage
