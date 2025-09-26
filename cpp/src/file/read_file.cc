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

#include "file/read_file.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include "common/logger/elog.h"
#include "common/tsfile_common.h"

#ifdef _WIN32
#include <io.h>
#include <windows.h>

ssize_t pread(int fd, void *buf, size_t count, uint64_t offset);
#endif

using namespace common;
namespace storage {

void ReadFile::close() {
    if (fd_ > 0) {
        ::close(fd_);
        fd_ = -1;
    }
    file_size_ = -1;
}

int ReadFile::open(const std::string &file_path) {
    int ret = E_OK;
    file_path_ = file_path;
    fd_ = ::open(file_path_.c_str(), O_RDONLY);
    if (fd_ < 0) {
        std::cout << "open file " << file_path << "  error :" << fd_
                  << std::endl;
        std::cout << "open error" << errno << "  " << strerror(errno)
                  << std::endl;
        return E_FILE_OPEN_ERR;
    }

    if (RET_FAIL(get_file_size(file_size_))) {
    } else if (RET_FAIL(check_file_magic())) {
    }
    if (IS_FAIL(ret)) {
        ::close(fd_);
    }
    return ret;
}

int ReadFile::get_file_size(int32_t &file_size) {
    struct stat s;
    if (fstat(fd_, &s) < 0) {
        LOGE("fstat error, file_path=" << file_path_.c_str() << "fd=" << fd_
                                       << "errno" << errno);
        return E_FILE_STAT_ERR;
    }
    file_size = s.st_size;
    return E_OK;
}

int ReadFile::check_file_magic() {
    int ret = E_OK;
    if (file_size_ < MIN_FILE_SIZE) {
        ret = E_TSFILE_CORRUPTED;
        LOGE("tsfile" << file_path_.c_str()
                      << "is corrupted, file_size=" << file_size_);
    } else {
        char buf[MAGIC_STRING_TSFILE_LEN];
        int32_t read_len = 0;
        // file header magic
        memset(buf, 0, MAGIC_STRING_TSFILE_LEN);
        if (RET_FAIL(read(0, buf, MAGIC_STRING_TSFILE_LEN, read_len))) {
        } else if (read_len != MAGIC_STRING_TSFILE_LEN) {
            ret = E_TSFILE_CORRUPTED;
        } else if (memcmp(buf, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) !=
                   0) {
            ret = E_TSFILE_CORRUPTED;
        }

        // file footer magic
        memset(buf, 0, MAGIC_STRING_TSFILE_LEN);
        if (RET_FAIL(read(file_size_ - MAGIC_STRING_TSFILE_LEN, buf,
                          MAGIC_STRING_TSFILE_LEN, read_len))) {
        } else if (read_len != MAGIC_STRING_TSFILE_LEN) {
            ret = E_TSFILE_CORRUPTED;
        } else if (memcmp(buf, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) !=
                   0) {
            ret = E_TSFILE_CORRUPTED;
        }
    }
    return ret;
}

int ReadFile::read(int32_t offset, char *buf, int32_t buf_size,
                   int32_t &read_len) {
    int ret = E_OK;
    read_len = 0;
    while (read_len < buf_size) {
        ssize_t pread_size = ::pread(fd_, buf + read_len, buf_size - read_len,
                                     offset + read_len);
        if (pread_size < 0) {
            ret = E_FILE_READ_ERR;
            ////log_err("tsfile reader error, file_path=%s, errno=%d",
            /// file_path_.c_str(), errno);
            break;
        } else if (pread_size == 0) {
            break;
        } else {
            read_len += pread_size;
        }
    }
    return ret;
}

}  // end namespace storage

#ifdef _WIN32
ssize_t pread(int fd, void *buf, size_t count, uint64_t offset) {
    long unsigned int read_bytes = 0;

    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(OVERLAPPED));

    overlapped.OffsetHigh = (uint32_t)((offset & 0xFFFFFFFF00000000LL) >> 32);
    overlapped.Offset = (uint32_t)(offset & 0xFFFFFFFFLL);

    HANDLE file = (HANDLE)_get_osfhandle(fd);
    SetLastError(0);
    bool RF = ReadFile(file, buf, count, &read_bytes, &overlapped);

    // For some reason it errors when it hits end of file so we don't want to
    // check that
    if ((RF == 0) && GetLastError() != ERROR_HANDLE_EOF) {
        errno = GetLastError();
        // printf ("Error reading file : %d\n", GetLastError());
        return -1;
    }

    return read_bytes;
}
#endif
