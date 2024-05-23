
#include "scan_iterator.h"

using namespace timecho::common;

namespace timecho
{
namespace storage
{

int DataRun::remove_tsfile(const FileID &to_remove)
{
  ASSERT(run_type_ == DRT_TSFILE);
  int ret = E_OK;

  SimpleList<OpenFile*>::Iterator it = tsfile_list_.begin();
  OpenFile *of = nullptr;
  for (; it != tsfile_list_.end(); it++) {
    OpenFile *cur = it.get();
    if (cur->get_file_id() == to_remove) {
      of = cur;
      break;
    }
  }
  if (of != nullptr) {
    ret = tsfile_list_.remove(of);
  }
  return ret;
}

int DataRun::get_next(TsBlock *ret_block, TimeRange &ret_time_range, bool alloc_tsblock)
{
  if (run_type_ == DRT_TVLIST) {
    return tvlist_get_next(ret_block, ret_time_range, alloc_tsblock);
  } else {
    ASSERT(run_type_ == DRT_TSFILE);
    return tsfile_get_next(ret_block, ret_time_range, alloc_tsblock);
  }
}

TsBlock *DataRun::alloc_tsblock()
{
  tuple_desc_.reset();
  // TODO default config of time_cd
  tuple_desc_.push_back(g_time_column_desc);
  tuple_desc_.push_back(*col_desc_);
  return (new TsBlock(&tuple_desc_));
}

int DataRun::tvlist_get_next(TsBlock *ret_block, TimeRange &ret_time_range, bool tsblock)
{
  // TODO @tsblock
  int ret = E_OK;
  if (UNLIKELY(!tvlist_list_iter_.is_inited())) {
    tvlist_list_iter_ = tvlist_list_.begin();
  }

  while (true) {
    if (tvlist_list_iter_ == tvlist_list_.end()) {
      return E_NO_MORE_DATA;
    } else {
      SeqTVListBase *tvlist_base = tvlist_list_iter_.get();
      if (!tvlist_base->is_immutable()) {
        tvlist_base->lock();
      }
      ret = fill_tsblock_from_tvlist(tvlist_base, ret_block, ret_time_range);
      if (ret == E_NO_MORE_DATA) {
        ret = E_OK;
      }
      if (!tvlist_base->is_immutable()) {
        tvlist_base->unlock();
      }
      tvlist_list_iter_++;
      return ret;
    }
  }
  return ret;
}

int DataRun::fill_tsblock_from_tvlist(SeqTVListBase *tvlist,
                                      TsBlock *ret_block,
                                      TimeRange &ret_time_range)
{
  int ret = E_OK;
  switch (col_desc_->type_) {
    case timecho::common::BOOLEAN:
      ret = fill_tsblock_from_typed_tvlist<bool>(tvlist, ret_block, ret_time_range);
      break;
    case timecho::common::INT32:
      ret = fill_tsblock_from_typed_tvlist<int32_t>(tvlist, ret_block, ret_time_range);
      break;
    case timecho::common::INT64:
      ret = fill_tsblock_from_typed_tvlist<int64_t>(tvlist, ret_block, ret_time_range);
      break;
    case timecho::common::FLOAT:
      ret = fill_tsblock_from_typed_tvlist<float>(tvlist, ret_block, ret_time_range);
      break;
    case timecho::common::DOUBLE:
      ret = fill_tsblock_from_typed_tvlist<double>(tvlist, ret_block, ret_time_range);
      break;
    default:
      ASSERT(false);
      break;
  }
  return ret;
}

template<typename T>
int DataRun::fill_tsblock_from_typed_tvlist(SeqTVListBase *tvlist,
                                            TsBlock *ret_block,
                                            TimeRange &ret_time_range)
{
  int ret = E_OK;

  SeqTVList<T> *typed_tvlist = static_cast<SeqTVList<T>*>(tvlist);
  typename SeqTVList<T>::Iterator it;
  it = typed_tvlist->scan_without_lock();
  typename SeqTVList<T>::TV tv;
  
  // FIXME do not append all tvlist data into tsblock in one time.
  ret_time_range.start_time_ = typed_tvlist->time_at(0);
  RowAppender row_appender(ret_block);
#ifndef NDEBUG
  int count = 0;
#endif
  while (E_OK == (ret = it.next(tv))) {
    ret_time_range.end_time_ = tv.time_;
#ifndef NDEBUG
    std::cout << "DataRun::fill_tsblock_from_typed_tvlist: [" << count << "] = <" << tv.time_ << ", " << tv.value_ << ">"<< std::endl;
#endif
    row_appender.add_row();
    row_appender.append(0, reinterpret_cast<char*>(&tv.time_), sizeof(tv.time_));
    row_appender.append(1, reinterpret_cast<char*>(&tv.value_), sizeof(tv.value_));
  }
  return ret;
}

int DataRun::reinit_io_reader(SimpleList<OpenFile*>::Iterator &it)
{
  int ret = E_OK;
  // maybe io_reader_ destroy before re-init
  OpenFile *open_file = it.get();
  io_reader_.reset();
  if (RET_FAIL(io_reader_.init(open_file->get_file_path()))) {
    log_error("io_reader init error, ret=%d, file_path=%s",
               ret, open_file->get_file_path().c_str());
  } else {
    std::string device_name = col_desc_->get_device_name_str();
    std::string measurement_name = col_desc_->get_measurement_name_str();
    if (ssi_ != nullptr) {
      delete ssi_;
      ssi_ = nullptr;
    }
    if (RET_FAIL(io_reader_.alloc_ssi(device_name, measurement_name, ssi_))) {
    }
  }
  return ret;
}

int DataRun::tsfile_get_next(TsBlock *ret_tsblock, TimeRange &ret_time_range, bool alloc_tsblock)
{
  int ret = E_OK;
  if (UNLIKELY(!tsfile_list_iter_.is_inited())) {
    tsfile_list_iter_ = tsfile_list_.begin();
    if (tsfile_list_iter_ == tsfile_list_.end()) { // all file iterated
      ret = E_NO_MORE_DATA;
    } else if (RET_FAIL(reinit_io_reader(tsfile_list_iter_))) {
    }
  }

  if (IS_SUCC(ret)) {
    // ret = io_reader_.get_next(*col_desc_, ret_tsblock, ret_time_range);
    ret = ssi_->get_next(ret_tsblock, alloc_tsblock);
    if (E_NO_MORE_DATA == ret) { // current file reach end
      tsfile_list_iter_++;
      if (tsfile_list_iter_ == tsfile_list_.end()) { // all file iterated
        ret = E_NO_MORE_DATA;
      } else if (RET_FAIL(reinit_io_reader(tsfile_list_iter_))) {
      }
    }
  }
  return ret;
}

DataRun* DataScanIterator::alloc_data_run(DataRunType run_type)
{
  void *buf = page_arena_.alloc(sizeof(DataRun));
  if (IS_NULL(buf)) {
    return nullptr;
  }
  return (new (buf) DataRun(run_type, &col_desc_, &page_arena_));
}

#ifndef NDEBUG
void DataScanIterator::DEBUG_dump_data_run_list()
{
  SimpleList<DataRun*>::Iterator it;
  std::cout << "\n/---- DEBUG_dump_data_run_list: size=" << data_run_list_.size() << "----\\" << std::endl;
  int idx = 0;
  for (it = data_run_list_.begin(); it != data_run_list_.end(); it++) {
    std::cout << "[" << (idx++) << "]" << *it.get() << std::endl;
  }
  std::cout << "\\---- DEBUG_dump_data_run_list: size=" << data_run_list_.size() << "----/\n" << std::endl;
}
#endif

int DataScanIterator::get_next(TsBlock *ret_block, bool alloc_tsblock)
{
#ifndef NDEBUG
  DEBUG_dump_data_run_list();
#endif
  int ret = E_OK;

  if (UNLIKELY(!cursor_.is_inited())) {
    cursor_ = data_run_list_.begin();
  }

  while (true) {
    TimeRange time_range;
    DataRun *data_run = cursor_.get();
    ret = data_run->get_next(ret_block, time_range, alloc_tsblock);
    if (ret == E_OK) {
      return ret;
    } else if (ret == E_NO_MORE_DATA) {
      cursor_++;
      if (cursor_ == data_run_list_.end()) {
        return E_NO_MORE_DATA;
      }
    } else {
      log_error("data run get next batch error, ret=%d", ret);
      break;
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace timecho
