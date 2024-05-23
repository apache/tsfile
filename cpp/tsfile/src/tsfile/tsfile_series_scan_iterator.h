
#ifndef STORAGE_TSFILE_TSFILE_SERIES_SCAN_ITERATOR_H
#define STORAGE_TSFILE_TSFILE_SERIES_SCAN_ITERATOR_H

#include <string>
#include "common/util_define.h"
#include "common/tsblock/tsblock.h"
#include "read_file.h"
#include "tsfile_io_reader.h"
#include "chunk_reader.h"
#include "tsfile/filter/filter.h"

namespace timecho
{
namespace storage
{

class TsFileIOReader;

class TsFileSeriesScanIterator
{
public:
  TsFileSeriesScanIterator() : read_file_(nullptr),
                               device_path_(),
                               measurement_name_(),
                               timeseries_index_(),
                               timeseries_index_pa_(),
                               chunk_meta_cursor_(),
                               chunk_reader_(),
                               tuple_desc_(),
                               tsblock_(nullptr),
                               time_filter_(nullptr) {}
  ~TsFileSeriesScanIterator() { destroy(); }
  int init(const std::string &device_path,
           const std::string &measurement_name,
           ReadFile *read_file,
           Filter *time_filter)
  {
    ASSERT(read_file != nullptr);
    device_path_ = device_path;
    measurement_name_ = measurement_name;
    read_file_ = read_file;
    time_filter_ = time_filter;
    return common::E_OK;
  }
  void destroy();
  /*
   * If oneshoot filter specified, use it instead of this->time_filter_
   */
  int get_next(common::TsBlock *&ret_tsblock,
               bool alloc_tsblock,
               Filter *oneshoot_filter = nullptr);
  void revert_tsblock();

  friend class TsFileIOReader;

private:
  int init_chunk_reader();
  FORCE_INLINE bool has_next_chunk() const
  {
    return chunk_meta_cursor_ != timeseries_index_.get_chunk_meta_list()->end();
  }
  FORCE_INLINE void advance_to_next_chunk()
  {
    chunk_meta_cursor_++;
  }
  FORCE_INLINE ChunkMeta* get_current_chunk_meta()
  {
    return chunk_meta_cursor_.get();
  }
  common::TsBlock *alloc_tsblock();


private:
  ReadFile *read_file_;
  std::string device_path_;
  std::string measurement_name_;

  TimeseriesIndex timeseries_index_;
  common::PageArena timeseries_index_pa_;
  common::SimpleList<ChunkMeta*>::Iterator chunk_meta_cursor_;
  ChunkReader chunk_reader_;

  common::TupleDesc tuple_desc_;
  common::TsBlock *tsblock_;
  Filter *time_filter_;
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_TSFILE_SERIES_SCAN_ITERATOR_H

