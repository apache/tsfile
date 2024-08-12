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

#include "write_file.h"

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "common/config/config.h"
#include "common/logger/elog.h"
#include "utils/errno_define.h"

#ifdef _WIN32
int fsync(int);
#endif

using namespace common;

namespace storage {

#ifndef LIBTSFILE_SDK
int WriteFile::create(const FileID &file_id, int flags, mode_t mode) {
    if (fd_ > 0) {
        // log_err("file already opened, fd=%d", fd_);
        ASSERT(false);
        return E_ALREADY_EXIST;
    }
    file_id_ = file_id;
    path_ = get_file_path_from_file_id(file_id_);
    return do_create(flags, mode);
}
#endif

int WriteFile::create(const std::string &file_path, int flags, mode_t mode) {
    if (fd_ > 0) {
        // log_err("file already opened, fd=%d", fd_);
        ASSERT(false);
        return E_ALREADY_EXIST;
    }
    path_ = file_path;
    return do_create(flags, mode);
}

int WriteFile::do_create(int flags, mode_t mode) {
    int ret = E_OK;
    // TODO make sure no same file exists
    fd_ = ::open(path_.c_str(), flags, mode);
    if (fd_ < 0) {
        // log_err("open file error, path=%s, errno=%d", path_.c_str(), errno);
        ret = E_FILE_OPEN_ERR;
    } else {
    }
    return ret;
}

int WriteFile::write(const char *buf, uint32_t len) {
    ASSERT(fd_ > 0);

#if 0  // DEBUG_SE
    struct stat statbuf;
    if (fstat(fd_, &statbuf) < 0) {
      perror("fstat");
      ASSERT(false);
    }
    printf("writer buffer(%u bytes) to file at %zu\n", len, statbuf.st_size);
    for (uint32_t i = 0; i < len;) {
      printf("0x%02x ", (uint8_t)buf[i]);
      if ((++i) % 16 == 0) {
        printf("\n");
      }
    }
    printf("\n");
#endif

    int ret = E_OK;
    uint32_t write_done = 0;
    while (write_done < len && IS_SUCC(ret)) {
        int32_t cur_write = ::write(fd_, buf + write_done, len - write_done);
        if (cur_write < 0) {
            ret = E_FILE_WRITE_ERR;
            // log_err("file writer error, path=%s, error=%d", path_.c_str(),
            // errno);
        } else {
            write_done += cur_write;
        }
    }
    return ret;
}

int WriteFile::sync() {
    ASSERT(fd_ > 0);
    if (::fsync(fd_) < 0) {
        // log_err("file fsync error, path=%s, errno=%d", path_.c_str(), errno);
        return E_FILE_SYNC_ERR;
    }
    return E_OK;
}

int WriteFile::close() {
    ASSERT(fd_ > 0);
    if (::close(fd_) < 0) {
#ifdef DEBUG_SE
        std::cout << "failed to close " << path_ << " errorno " << errno
                  << std::endl;
#endif
        // log_err("file close error, path=%s, errno=%d", path_.c_str(), errno);
        return E_FILE_CLOSE_ERR;
    }
#ifdef DEBUG_SE
    std::cout << "close finish" << std::endl;
#endif
    return E_OK;
}

}  // end namespace storage

#ifdef _WIN32
int fsync(int fd) { return _commit(fd); }
#endif
