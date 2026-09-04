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

#include "file/random_access_file.h"

#include <cstring>

#include "common/tsfile_common.h"
#include "utils/errno_define.h"

namespace storage {

int validate_tsfile(RandomAccessFile& file, unsigned char* file_version) {
    static const int64_t MIN_FILE_SIZE = 2 * MAGIC_STRING_TSFILE_LEN + 1;
    if (!file.is_opened()) {
        return common::E_FILE_READ_ERR;
    }
    if (file.file_size() < MIN_FILE_SIZE) {
        return common::E_TSFILE_CORRUPTED;
    }

    char buffer[MAGIC_STRING_TSFILE_LEN];
    int32_t read_size = 0;
    int ret = file.read(0, buffer, MAGIC_STRING_TSFILE_LEN, read_size);
    if (ret != common::E_OK) {
        return ret;
    }
    if (read_size != MAGIC_STRING_TSFILE_LEN ||
        std::memcmp(buffer, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) !=
            0) {
        return common::E_TSFILE_CORRUPTED;
    }

    char version = 0;
    ret = file.read(MAGIC_STRING_TSFILE_LEN, &version, 1, read_size);
    if (ret != common::E_OK) {
        return ret;
    }
    if (read_size != 1) {
        return common::E_TSFILE_CORRUPTED;
    }
    const unsigned char parsed_version = static_cast<unsigned char>(version);
    if (parsed_version != 3 &&
        parsed_version != static_cast<unsigned char>(VERSION_NUM_BYTE)) {
        return common::E_UNSUPPORTED_VERSION;
    }

    ret = file.read(file.file_size() - MAGIC_STRING_TSFILE_LEN, buffer,
                    MAGIC_STRING_TSFILE_LEN, read_size);
    if (ret != common::E_OK) {
        return ret;
    }
    if (read_size != MAGIC_STRING_TSFILE_LEN ||
        std::memcmp(buffer, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) !=
            0) {
        return common::E_TSFILE_CORRUPTED;
    }
    if (file_version != nullptr) {
        *file_version = parsed_version;
    }
    return common::E_OK;
}

}  // namespace storage
