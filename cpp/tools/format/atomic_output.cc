/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

#include "format/atomic_output.h"

#include <fcntl.h>
#include <sys/stat.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <sstream>

#include "cli/exit_codes.h"

#ifdef _WIN32
#include <io.h>
#include <process.h>
#include <windows.h>
#define lstat stat
#define close _close
#define getpid _getpid
#else
#include <unistd.h>
#endif

namespace tsfile_cli {
namespace {

bool is_regular_file(const struct stat& st) {
#ifdef _WIN32
    return (st.st_mode & S_IFREG) != 0;
#else
    return S_ISREG(st.st_mode);
#endif
}

bool is_same_file(const std::string& source_path,
                  const std::string& target_path, const struct stat& target) {
#ifdef _WIN32
    (void)target;
    const DWORD share = FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE;
    HANDLE source = CreateFileA(source_path.c_str(), 0, share, nullptr,
                                OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (source == INVALID_HANDLE_VALUE) {
        return false;
    }
    HANDLE output = CreateFileA(target_path.c_str(), 0, share, nullptr,
                                OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (output == INVALID_HANDLE_VALUE) {
        CloseHandle(source);
        return false;
    }
    BY_HANDLE_FILE_INFORMATION source_info;
    BY_HANDLE_FILE_INFORMATION output_info;
    const bool same =
        GetFileInformationByHandle(source, &source_info) != 0 &&
        GetFileInformationByHandle(output, &output_info) != 0 &&
        source_info.dwVolumeSerialNumber == output_info.dwVolumeSerialNumber &&
        source_info.nFileIndexHigh == output_info.nFileIndexHigh &&
        source_info.nFileIndexLow == output_info.nFileIndexLow;
    CloseHandle(output);
    CloseHandle(source);
    return same;
#else
    (void)target_path;
    struct stat source;
    return stat(source_path.c_str(), &source) == 0 &&
           source.st_dev == target.st_dev && source.st_ino == target.st_ino;
#endif
}

int validate_target(const std::string& target_path,
                    const std::string& source_path, bool force,
                    std::ostream& err) {
    struct stat target;
    if (lstat(target_path.c_str(), &target) != 0) {
        if (errno == ENOENT) {
            return kExitOk;
        }
        err << "Error: cannot inspect output target '" << target_path
            << "': " << std::strerror(errno) << "\n";
        return kExitRuntime;
    }
#ifndef _WIN32
    if (S_ISLNK(target.st_mode)) {
        err << "Error: output target '" << target_path
            << "' must not be a symbolic link\n";
        return kExitRuntime;
    }
#else
    DWORD attributes = GetFileAttributesA(target_path.c_str());
    if (attributes != INVALID_FILE_ATTRIBUTES &&
        (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
        err << "Error: output target '" << target_path
            << "' must not be a reparse point\n";
        return kExitRuntime;
    }
#endif
    if (!is_regular_file(target)) {
        err << "Error: output target '" << target_path
            << "' is not a regular file\n";
        return kExitRuntime;
    }
    if (!source_path.empty() &&
        is_same_file(source_path, target_path, target)) {
        err << "Error: output target '" << target_path
            << "' is the same as the input file '" << source_path << "'\n";
        return kExitRuntime;
    }
    if (!force) {
        err << "Error: output target '" << target_path
            << "' already exists; use --force to replace a regular file\n";
        return kExitRuntime;
    }
    return kExitOk;
}

}  // namespace

int prepare_atomic_output(const std::string& target_path,
                          const std::string& source_path, bool force,
                          std::string& temp_path, std::ostream& err) {
    int ret = validate_target(target_path, source_path, force, err);
    if (ret != kExitOk) {
        return ret;
    }
    for (unsigned int attempt = 0; attempt < 1000; ++attempt) {
        std::ostringstream candidate;
        candidate << target_path << ".tmp." << getpid() << "." << attempt;
        temp_path = candidate.str();
        int flags = O_WRONLY | O_CREAT | O_EXCL;
#ifdef _WIN32
        flags |= O_BINARY;
#endif
        int fd = ::open(temp_path.c_str(), flags, 0600);
        if (fd >= 0) {
            if (::close(fd) != 0) {
                err << "Error: failed to close temporary output '" << temp_path
                    << "': " << std::strerror(errno) << "\n";
                std::remove(temp_path.c_str());
                return kExitRuntime;
            }
            return kExitOk;
        }
        if (errno != EEXIST) {
            err << "Error: cannot create output temporary file for '"
                << target_path << "': " << std::strerror(errno) << "\n";
            return kExitRuntime;
        }
    }
    err << "Error: cannot allocate a unique temporary output for '"
        << target_path << "'\n";
    return kExitRuntime;
}

int commit_atomic_output(const std::string& temp_path,
                         const std::string& target_path, bool force,
                         std::ostream& err) {
#ifdef _WIN32
    DWORD flags = MOVEFILE_WRITE_THROUGH;
    if (force) {
        flags |= MOVEFILE_REPLACE_EXISTING;
    }
    if (MoveFileExA(temp_path.c_str(), target_path.c_str(), flags) != 0) {
        return kExitOk;
    }
#else
    if (force) {
        if (std::rename(temp_path.c_str(), target_path.c_str()) == 0) {
            return kExitOk;
        }
    } else if (::link(temp_path.c_str(), target_path.c_str()) == 0) {
        if (std::remove(temp_path.c_str()) == 0) {
            return kExitOk;
        }
        err << "Error: output was committed but temporary path remains: '"
            << temp_path << "'\n";
        return kExitRuntime;
    }
#endif
    err << "Error: failed to commit output target '" << target_path << "'\n";
    return kExitRuntime;
}

bool remove_atomic_temp(const std::string& temp_path, std::ostream& err) {
    if (temp_path.empty() || std::remove(temp_path.c_str()) == 0 ||
        errno == ENOENT) {
        return true;
    }
    err << "Error: failed to remove temporary output '" << temp_path
        << "': " << std::strerror(errno) << "\n";
    return false;
}

}  // namespace tsfile_cli
