
#ifndef COMMON_TSFILE_MGR_H
#define COMMON_TSFILE_MGR_H

#include "common/db_common.h"
#include "common/mutex/mutex.h"
#include "reader/scan_iterator.h"

namespace storage {

// TODO use header file instead
class TimeFilter;

struct TimeRangeOpenFilePair {
    TimeRange time_range_;
    OpenFile *open_file_;
};

FORCE_INLINE bool compare_timerange_openfile_pair(
    const TimeRangeOpenFilePair &x, const TimeRangeOpenFilePair &y) {
    return x.time_range_.start_time_ < y.time_range_.start_time_;
}

FORCE_INLINE void merge_time_range(TimeRange &dest, const TimeRange &src) {
    dest.start_time_ = UTIL_MIN(dest.start_time_, src.start_time_);
    dest.end_time_ = UTIL_MAX(dest.end_time_, src.end_time_);
}

class TsFileMgr {
   public:
    typedef std::map<common::FileID, OpenFile *> AllOpenFileMap;
    typedef AllOpenFileMap::iterator AllOpenFileMapIter;

   public:
    TsFileMgr() : all_open_files_(), version_(0), all_open_files_mutex_() {}
    static TsFileMgr &get_instance();
    int init();
    void destroy() { all_open_files_.clear(); }

    int add_new_file(const std::string &file_path);
    int add_new_file(const common::FileID &file_id, OpenFile *open_file);

    // int get_files_for_query(const common::TsID &ts_id,
    //                         const TimeFilter &time_filter,
    //                         common::SimpleList<DataRun> &ret_data_runs);
    int get_files_for_query(const common::TsID &ts_id,
                            const TimeFilter *time_filter,
                            DataRun *ret_data_run, int64_t &ret_version);
    int64_t get_version() {
        common::MutexGuard mg(all_open_files_mutex_);
        return version_;
    }

#ifndef NDEBUG
    void DEBUG_dump(const char *tag);
#endif

   private:
    bool time_range_stasify(const TimeFilter *time_filter,
                            const TimeRange &time_range);

   private:
    // Map<file_path, OpenFile>
    AllOpenFileMap all_open_files_;
    int64_t version_;
    common::Mutex all_open_files_mutex_;
};

#ifndef NDEBUG
#define DUMP_TSFILE_MGR(tag) TsFileMgr::get_instance().DEBUG_dump(tag)
#else
#define DUMP_TSFILE_MGR(tag) (void)
#endif

}  // namespace storage

#endif  // COMMON_TSFILE_MGR_H
