
#ifndef STORAGE_TSFILE_OPEN_FILE_H
#define STORAGE_TSFILE_OPEN_FILE_H

#include <map>
#include <vector>
#include "common/container/list.h"
#include "common/mutex/mutex.h"
#include "tsfile/storage_utils.h"
#include "tsfile_common.h"

namespace timecho
{
namespace storage
{

/*
 * Data of a TSStore consist of:
 * - some tsfiles
 * - some immutable tvlists
 * - the active tvlist.
 *
 * When some immutable tvlists are flushed into tsfile, we
 * should guarantee that we have a consisteny view of TSStore
 * data. That means we should see one and only one of the
 * flushed immutable tvlist or the corresponding tsfile.
 * Otherwise, we may see duplicated data in the immutable tvlist
 * and the tsfile, or we may lose the data if we see neither
 * of them.
 * So we should do the following steps atomically:
 * - remove the immutable tvlist from tsstore
 * - make it visible in tsfile_mgr
 */

// opened tsfile
class OpenFile
{
public:
  // maybe use sorted array instead of map.
  typedef std::map<common::TsID, TimeRange> TsTimeRangeMap;
  typedef std::map<common::TsID, TimeRange>::iterator TsTimeRangeMapIterator;
public:
  OpenFile() : file_id_(),
               file_path_(),
               fd_(-1),
               bloom_filter_(nullptr),
               ts_time_range_map_(nullptr),
               mutex_() {}
  int init();
  // reset the map to reclaim memory
  void reset();

  void set_file_id_and_path(const common::FileID &file_id, const std::string &file_path);
  FORCE_INLINE common::FileID get_file_id() { return file_id_; }
  FORCE_INLINE std::string get_file_path() { return file_path_; }
  int build_from(const std::vector<TimeseriesTimeIndexEntry>& time_index_vec);
  int add(const common::TsID &ts_id, const TimeRange &time_range);

  bool contain_timeseries(const common::TsID &ts_id) const;
  int get_time_range(const common::TsID &ts_id, TimeRange &ret_time_range) const;

#ifndef NDEBUG
  friend std::ostream& operator << (std::ostream& out, OpenFile &open_file)
  {
    out << "file_id=" << open_file.file_id_
        << ", file_path=" << open_file.file_path_
        << ", fd=" << open_file.fd_
        << ", ts_time_range_map=";
    if (open_file.ts_time_range_map_ == nullptr) {
      out << "nil" << std::endl;
    } else {
      TsTimeRangeMapIterator it;
      out << std::endl;
      for (it = open_file.ts_time_range_map_->begin();
           it != open_file.ts_time_range_map_->end();
           it++) { // cppcheck-suppress postfixOperator
        out << "{ts_id=" << it->first.to_string()
            << ", ts_time_range={start_time=" << it->second.start_time_
            << ", end_time=" << it->second.end_time_ << "}}";
      }
    }
    return out;
  }
#endif

private:
  common::FileID file_id_;
  std::string file_path_;
  int fd_;
  /*
   * Why use pointer instead of object:
   * we may want to reclaim the memory in case of memory overused.
   */
  BloomFilter *bloom_filter_;
  TsTimeRangeMap *ts_time_range_map_; // TODO: use custom hashtable to monitor memory.
  mutable common::Mutex mutex_;
};

class OpenFileFactory
{
public:
  static OpenFile* alloc()
  {
    void *buf = common::mem_alloc(sizeof(OpenFile), common::MOD_OPEN_FILE_OBJ);
    if (IS_NULL(buf)) {
      return nullptr;
    }
    return new (buf) OpenFile;
  }

  static void free(OpenFile *of)
  {
    if (of != nullptr) {
      common::mem_free(of);
    }
  }
};

} // end namespace storage
} // end namespace timecho

#endif // STORAGE_TSFILE_OPEN_FILE_H

