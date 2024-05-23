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

#ifndef FILE_WRITE_FILE_H
#define FILE_WRITE_FILE_H

#include <string>

#include "utils/util_define.h"
#ifndef LIBTSFILE_SDK
#include "utils/storage_utils.h"
#endif

namespace storage {

class WriteFile {
   public:
#ifndef LIBTSFILE_SDK
    WriteFile() : path_(), file_id_(), fd_(-1) {}
    FORCE_INLINE common::FileID get_file_id() const { return file_id_; }
    int create(const common::FileID &file_id, int flags, mode_t mode);
#else
    WriteFile() : path_(), fd_(-1) {}
#endif
    int create(const std::string &file_name, int flags, mode_t mode);
    bool file_opened() const { return fd_ > 0; }
    int write(const char *buf, uint32_t len);
    // int flush() { return common::E_OK; } // TODO
    int sync();
    int close();
    FORCE_INLINE std::string get_file_path() { return path_; }

   private:
    int do_create(int flags, mode_t mode);

   private:
    std::string path_;
#ifndef LIBTSFILE_SDK
    common::FileID file_id_;
#endif
    int fd_;
};

}  // end namespace storage
#endif  // FILE_WRITE_FILE_H
