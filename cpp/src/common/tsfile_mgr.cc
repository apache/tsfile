/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "tsfile_mgr.h"

#include <algorithm>  // std::sort
#include <iomanip>
#include <iostream>

#include "utils/errno_define.h"

using namespace common;

namespace storage {

TsFileMgr &TsFileMgr::get_instance() {
    static TsFileMgr g_s_tsfile_mgr;
    return g_s_tsfile_mgr;
}

int TsFileMgr::init() {
    int ret = E_OK;
    return ret;
}

// used when recover
int TsFileMgr::add_new_file(const std::string &file_path) {
    int ret = E_OK;
    MutexGuard mg(all_open_files_mutex_);
    // TODO
    return ret;
}

int TsFileMgr::add_new_file(const FileID &file_id, OpenFile *open_file) {
    MutexGuard mg(all_open_files_mutex_);
    AllOpenFileMapIter find_iter = all_open_files_.find(file_id);
    if (find_iter != all_open_files_.end()) {
        return E_ALREADY_EXIST;
    }
    std::pair<FileID, OpenFile *> pair;
    pair.first = file_id;
    pair.second = open_file;
    std::pair<AllOpenFileMapIter, bool> ins_res = all_open_files_.insert(pair);
    if (!ins_res.second) {
        ASSERT(false);
    }
    version_++;
    return E_OK;
}

/*
 * Currently, we only allow sequence data writing,
 * So we have only one DataRun returned.
 */
int TsFileMgr::get_files_for_query(const TsID &ts_id,
                                   const TimeFilter *time_filter,
                                   DataRun *ret_data_run,
                                   int64_t &ret_version) {
    int ret = E_OK;

    // Step 1: get all tsfiles that contain this ts_id, store them in tsfile_vec
    std::vector<TimeRangeOpenFilePair> tsfile_vec;

    all_open_files_mutex_.lock();
    for (AllOpenFileMapIter iter = all_open_files_.begin();
         iter != all_open_files_.end() && IS_SUCC(ret); iter++) {
        OpenFile *open_file = iter->second;
        TimeRange time_range;
        int tmp_ret = open_file->get_time_range(ts_id, time_range);
        if (tmp_ret == E_OK) {
            if (time_range_stasify(time_filter, time_range)) {
                TimeRangeOpenFilePair pair;
                pair.open_file_ = open_file;
                pair.time_range_ = time_range;
                tsfile_vec.push_back(pair);
            }
        } else if (tmp_ret == E_NOT_EXIST) {
            // continue next
        } else {
            ret = tmp_ret;
            // log_err("get time range for ts_id error, ret=%d, ts_id=%s", ret,
            // ts_id.to_string().c_str());
        }
    }  // end for
    ret_version = version_;
    all_open_files_mutex_.unlock();

    // Step 2: since we have only one DataRun, sort these tsfiles
    std::sort(tsfile_vec.begin(), tsfile_vec.end(),
              compare_timerange_openfile_pair);

    // Step 3: wrap them as DataRun
    for (size_t i = 0; i < tsfile_vec.size() && IS_SUCC(ret); i++) {
        merge_time_range(ret_data_run->time_range_, tsfile_vec[i].time_range_);
        ret = ret_data_run->tsfile_list_.push_back(tsfile_vec[i].open_file_);
    }
    return ret;
}

bool TsFileMgr::time_range_stasify(const TimeFilter *time_filter,
                                   const TimeRange &time_range) {
    // TODO
    UNUSED(time_filter);
    UNUSED(time_range);
    return true;
}

#ifndef NDEBUG
void TsFileMgr::DEBUG_dump(const char *tag) {
    MutexGuard mg(all_open_files_mutex_);
    AllOpenFileMapIter it;
    std::cout << tag << "Dump TsFileMgr Start" << std::endl;
    int count = 0;
    for (it = all_open_files_.begin(); it != all_open_files_.end(); it++) {
        std::cout << tag << "Dump TsFileMgr:\n  [" << std::setw(3)
                  << std::setfill(' ') << count << "]\n  file_id=" << it->first
                  << "\n  open_file=" << *it->second;
    }
    std::cout << tag << "Dump TsFileMgr End" << std::endl;
}
#endif

}  // end namespace storage