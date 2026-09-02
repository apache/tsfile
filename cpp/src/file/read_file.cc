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

#ifdef _WIN32
#include <io.h>
#include <windows.h>

ssize_t pread(int fd, void* buf, size_t count, uint64_t offset);
#else
#include <unistd.h>
#endif

#include "common/logger/elog.h"
#include "common/tsfile_common.h"
#include "utils/util_define.h"  // ssize_t and other platform-compat shims

using namespace common;
namespace storage {

namespace {
uint64_t generation_hash(uint64_t size, int64_t mtime_ns) {
    uint64_t hash = 1469598103934665603ULL;
    const uint64_t values[2] = {size, static_cast<uint64_t>(mtime_ns)};
    for (size_t word = 0; word < 2; ++word) {
        for (size_t byte = 0; byte < 8; ++byte) {
            hash ^= static_cast<uint8_t>(values[word] >> (byte * 8));
            hash *= 1099511628211ULL;
        }
    }
    return hash;
}
}  // namespace

int ReadFile::generation(uint64_t& size, uint64_t& fingerprint) const {
    if (fd_ < 0) {
        return E_FILE_READ_ERR;
    }
    int64_t mtime_ns = 0;
#ifdef _WIN32
    intptr_t handle_value = _get_osfhandle(fd_);
    if (handle_value == -1) {
        return E_FILE_READ_ERR;
    }
    FILE_BASIC_INFO info;
    if (!GetFileInformationByHandleEx(reinterpret_cast<HANDLE>(handle_value),
                                      FileBasicInfo, &info, sizeof(info))) {
        return E_FILE_READ_ERR;
    }
    static const int64_t WINDOWS_TO_UNIX_100NS = 116444736000000000LL;
    mtime_ns = (info.LastWriteTime.QuadPart - WINDOWS_TO_UNIX_100NS) * 100;
#else
    struct stat info;
    if (::fstat(fd_, &info) != 0) {
        return E_FILE_READ_ERR;
    }
#ifdef __APPLE__
    mtime_ns = static_cast<int64_t>(info.st_mtimespec.tv_sec) * 1000000000LL +
               info.st_mtimespec.tv_nsec;
#else
    mtime_ns = static_cast<int64_t>(info.st_mtim.tv_sec) * 1000000000LL +
               info.st_mtim.tv_nsec;
#endif
#endif
    size = static_cast<uint64_t>(file_size_);
    fingerprint = generation_hash(size, mtime_ns);
    return E_OK;
}

void ReadFile::close() {
    if (fd_ >= 0) {
        ::close(fd_);
        fd_ = -1;
    }
    file_size_ = -1;
    file_version_ = 0;
}

int ReadFile::open(const std::string& file_path) {
    int ret = E_OK;
    file_path_ = file_path;
    int flags = O_RDONLY;
#ifdef _WIN32
    flags |= O_BINARY;
    // Windows cannot open a directory with _open(), whereas POSIX permits
    // opening one and lets fstat() identify it.  Preflight existing
    // non-regular paths so both platforms report the same stable error code.
    // If stat itself fails, keep the normal open() path so missing or
    // inaccessible files continue to report E_FILE_OPEN_ERR.
    struct __stat64 preopen_stat;
    if (_stat64(file_path_.c_str(), &preopen_stat) == 0 &&
        (preopen_stat.st_mode & _S_IFMT) != _S_IFREG) {
        return E_INVALID_PATH;
    }
#endif
    fd_ = ::open(file_path_.c_str(), flags);
    if (fd_ < 0) {
        return E_FILE_OPEN_ERR;
    }

    // The CLI accepts regular files (and links to regular files) only.  Do
    // this check while the descriptor is open so a directory, FIFO, device,
    // or socket cannot reach the TsFile parser and produce a misleading
    // format/read error.  The descriptor is deliberately closed before
    // returning so callers never retain a special-file handle.
#ifdef _WIN32
    struct __stat64 path_stat;
    if (_fstat64(fd_, &path_stat) != 0) {
        close();
        return E_FILE_STAT_ERR;
    }
    if ((path_stat.st_mode & _S_IFMT) != _S_IFREG) {
        close();
        return E_INVALID_PATH;
    }
#else
    struct stat path_stat;
    if (::fstat(fd_, &path_stat) != 0) {
        close();
        return E_FILE_STAT_ERR;
    }
    if (!S_ISREG(path_stat.st_mode)) {
        close();
        return E_INVALID_PATH;
    }
#endif

    if (RET_FAIL(get_file_size(file_size_))) {
    } else if (RET_FAIL(check_file_magic())) {
    }
    if (IS_FAIL(ret)) {
        close();
    }
    return ret;
}

int ReadFile::get_file_size(int64_t& file_size) {
#ifdef _WIN32
    struct __stat64 s;
    if (_fstat64(fd_, &s) < 0) {
#else
    struct stat s;
    if (fstat(fd_, &s) < 0) {
#endif
        LOGE("fstat error, file_path=" << file_path_.c_str() << "fd=" << fd_
                                       << "errno" << errno);
        return E_FILE_STAT_ERR;
    }
    file_size = static_cast<int64_t>(s.st_size);
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
        if (IS_FAIL(ret)) {
            return ret;
        }

        char version = 0;
        if (RET_FAIL(read(MAGIC_STRING_TSFILE_LEN, &version, 1, read_len))) {
        } else if (read_len != 1) {
            ret = E_TSFILE_CORRUPTED;
        } else {
            file_version_ = static_cast<unsigned char>(version);
            // Version 3 remains readable for backward compatibility; version
            // 4 is the current writer format.  Other values are not safely
            // interpretable and must be reported as an input failure before
            // metadata parsing begins.
            if (file_version_ != 3 &&
                file_version_ != static_cast<unsigned char>(VERSION_NUM_BYTE)) {
                ret = E_UNSUPPORTED_VERSION;
            }
        }
        if (IS_FAIL(ret)) {
            return ret;
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

int ReadFile::read(int64_t offset, char* buf, int32_t buf_size,
                   int32_t& read_len) {
    int ret = E_OK;
    read_len = 0;
    while (read_len < buf_size) {
#ifdef _WIN32
        ssize_t pread_size = ::pread(fd_, buf + read_len, buf_size - read_len,
                                     static_cast<uint64_t>(offset + read_len));
#else
        ssize_t pread_size = ::pread(fd_, buf + read_len, buf_size - read_len,
                                     static_cast<off_t>(offset + read_len));
#endif
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
ssize_t pread(int fd, void* buf, size_t count, uint64_t offset) {
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
