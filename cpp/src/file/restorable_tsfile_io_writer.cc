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

#include "file/restorable_tsfile_io_writer.h"

#include <fcntl.h>

#include <cstring>
#include <memory>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/device_id.h"
#include "common/statistic.h"
#include "utils/errno_define.h"

#ifdef _WIN32
#include <io.h>
#include <sys/stat.h>
#include <windows.h>
ssize_t pread(int fd, void* buf, size_t count, uint64_t offset);
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

using namespace common;

namespace storage {

namespace {

const int HEADER_LEN = MAGIC_STRING_TSFILE_LEN + 1;  // magic + version
const int BUF_SIZE = 4096;

/**
 * Lightweight read-only file handle for self-check only.
 * Use init_from_fd() when WriteFile is already open to avoid opening the file
 * twice (fixes Windows file sharing and ensures we read the same content).
 */
struct SelfCheckReader {
    int fd_;
    int32_t file_size_;
    bool own_fd_;  // if false, do not close fd_

    SelfCheckReader() : fd_(-1), file_size_(-1), own_fd_(true) {}

    int init_from_fd(int fd) {
        fd_ = fd;
        own_fd_ = false;
        if (fd_ < 0) {
            return E_FILE_OPEN_ERR;
        }
#ifdef _WIN32
        struct _stat st;
        if (_fstat(fd_, &st) < 0) {
            return E_FILE_STAT_ERR;
        }
        file_size_ = static_cast<int32_t>(st.st_size);
#else
        struct stat st;
        if (fstat(fd_, &st) < 0) {
            return E_FILE_STAT_ERR;
        }
        file_size_ = static_cast<int32_t>(st.st_size);
#endif
        return E_OK;
    }

    int open(const std::string& path) {
#ifdef _WIN32
        fd_ = ::_open(path.c_str(), _O_RDONLY | _O_BINARY);
#else
        fd_ = ::open(path.c_str(), O_RDONLY);
#endif
        if (fd_ < 0) {
            return E_FILE_OPEN_ERR;
        }
        own_fd_ = true;
#ifdef _WIN32
        struct _stat st;
        if (_fstat(fd_, &st) < 0) {
            close();
            return E_FILE_STAT_ERR;
        }
        file_size_ = static_cast<int32_t>(st.st_size);
#else
        struct stat st;
        if (fstat(fd_, &st) < 0) {
            close();
            return E_FILE_STAT_ERR;
        }
        file_size_ = static_cast<int32_t>(st.st_size);
#endif
        return E_OK;
    }

    void close() {
        if (own_fd_ && fd_ >= 0) {
#ifdef _WIN32
            ::_close(fd_);
#else
            ::close(fd_);
#endif
        }
        fd_ = -1;
        file_size_ = -1;
    }

    int32_t file_size() const { return file_size_; }

    int read(int32_t offset, char* buf, int32_t buf_size, int32_t& read_len) {
        read_len = 0;
        if (fd_ < 0) {
            return E_FILE_READ_ERR;
        }
        ssize_t n = ::pread(fd_, buf, buf_size, offset);
        if (n < 0) {
            return E_FILE_READ_ERR;
        }
        read_len = static_cast<int32_t>(n);
        return E_OK;
    }
};

#ifdef _WIN32
ssize_t pread(int fd, void* buf, size_t count, uint64_t offset) {
    DWORD read_bytes = 0;
    OVERLAPPED ov = {};
    ov.OffsetHigh = (DWORD)((offset >> 32) & 0xFFFFFFFF);
    ov.Offset = (DWORD)(offset & 0xFFFFFFFF);
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (!ReadFile(h, buf, (DWORD)count, &read_bytes, &ov)) {
        if (GetLastError() != ERROR_HANDLE_EOF) {
            return -1;
        }
    }
    return (ssize_t)read_bytes;
}
#endif

static int parse_chunk_header_and_skip(SelfCheckReader& reader,
                                       int64_t chunk_start,
                                       int64_t& bytes_consumed,
                                       ChunkHeader* header_out = nullptr) {
    int32_t file_size = reader.file_size();
    int32_t max_read = static_cast<int32_t>(
        std::min(static_cast<int64_t>(BUF_SIZE), file_size - chunk_start));
    if (max_read < ChunkHeader::MIN_SERIALIZED_SIZE) {
        return E_TSFILE_CORRUPTED;
    }

    std::vector<char> buf(max_read);
    int32_t read_len = 0;
    int ret = reader.read(static_cast<int32_t>(chunk_start), buf.data(),
                          max_read, read_len);
    if (ret != E_OK || read_len < ChunkHeader::MIN_SERIALIZED_SIZE) {
        return E_TSFILE_CORRUPTED;
    }

    ByteStream bs;
    bs.wrap_from(buf.data(), read_len);

    ChunkHeader header;
    ret = header.deserialize_from(bs);
    if (ret != E_OK) {
        return E_TSFILE_CORRUPTED;
    }

    int header_len = bs.read_pos();
    int64_t total = header_len + header.data_size_;
    if (chunk_start + total > file_size) {
        return E_TSFILE_CORRUPTED;
    }

    if (header_out != nullptr) {
        *header_out = header;
    }
    bytes_consumed = total;
    return E_OK;
}

}  // namespace

RestorableTsFileIOWriter::RestorableTsFileIOWriter()
    : TsFileIOWriter(),
      write_file_(nullptr),
      write_file_owned_(false),
      truncated_size_(-1),
      crashed_(false),
      can_write_(false) {
    self_check_arena_.init(512, MOD_TSFILE_READER);
}

RestorableTsFileIOWriter::~RestorableTsFileIOWriter() { close(); }

void RestorableTsFileIOWriter::close() {
    if (write_file_owned_ && write_file_ != nullptr) {
        write_file_->close();
        delete write_file_;
        write_file_ = nullptr;
        write_file_owned_ = false;
    }
    self_check_arena_.destroy();
}

int RestorableTsFileIOWriter::open(const std::string& file_path,
                                   bool truncate_corrupted) {
    if (write_file_ != nullptr) {
        return E_ALREADY_EXIST;
    }

    file_path_ = file_path;
    write_file_ = new WriteFile();
    write_file_owned_ = true;

    // O_RDWR|O_CREAT without O_TRUNC: preserve existing file content
#ifdef _WIN32
    const int flags = O_RDWR | O_CREAT | O_BINARY;
#else
    const int flags = O_RDWR | O_CREAT;
#endif
    const mode_t mode = 0644;

    int ret = write_file_->create(file_path_, flags, mode);
    if (ret != E_OK) {
        close();
        return ret;
    }

    ret = self_check(truncate_corrupted);
    if (ret != E_OK) {
        close();
        return ret;
    }

    return E_OK;
}

int RestorableTsFileIOWriter::self_check(bool truncate_corrupted) {
    SelfCheckReader reader;
    // Use a separate read-only handle for self-check - on Windows, sharing
    // the O_RDWR fd can cause stale/cached reads for complete-file detection
    int ret = reader.open(file_path_);
    if (ret != E_OK) {
        return ret;
    }

    int32_t file_size = reader.file_size();
    if (file_size == 0) {
        reader.close();
        truncated_size_ = 0;
        crashed_ = true;
        can_write_ = true;
        if (write_file_->seek_to_end() != E_OK) {
            return E_FILE_READ_ERR;
        }
        ret = init(write_file_);
        if (ret != E_OK) {
            return ret;
        }
        ret = start_file();
        if (ret != E_OK) {
            return ret;
        }
        return E_OK;
    }

    if (file_size < HEADER_LEN) {
        reader.close();
        truncated_size_ = TSFILE_CHECK_INCOMPATIBLE;
        return E_TSFILE_CORRUPTED;
    }

    char header_buf[HEADER_LEN];
    int32_t read_len = 0;
    ret = reader.read(0, header_buf, HEADER_LEN, read_len);
    if (ret != E_OK || read_len != HEADER_LEN) {
        reader.close();
        truncated_size_ = TSFILE_CHECK_INCOMPATIBLE;
        return E_TSFILE_CORRUPTED;
    }

    if (memcmp(header_buf, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) != 0) {
        reader.close();
        truncated_size_ = TSFILE_CHECK_INCOMPATIBLE;
        return E_TSFILE_CORRUPTED;
    }

    if (header_buf[MAGIC_STRING_TSFILE_LEN] != VERSION_NUM_BYTE) {
        reader.close();
        truncated_size_ = TSFILE_CHECK_INCOMPATIBLE;
        return E_TSFILE_CORRUPTED;
    }

    // Completeness check per Java isComplete(): only header+tail magic
    // size >= MAGIC*2 + version_byte, tail magic equals head magic
    bool is_complete = false;
    if (file_size >= static_cast<int32_t>(MAGIC_STRING_TSFILE_LEN * 2 + 1)) {
        char tail_buf[MAGIC_STRING_TSFILE_LEN];
        ret = reader.read(file_size - MAGIC_STRING_TSFILE_LEN, tail_buf,
                          MAGIC_STRING_TSFILE_LEN, read_len);
        if (ret == E_OK && read_len == MAGIC_STRING_TSFILE_LEN &&
            memcmp(tail_buf, MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN) ==
                0) {
            is_complete = true;
        }
    }

    if (is_complete) {
        reader.close();
        truncated_size_ = TSFILE_CHECK_COMPLETE;
        crashed_ = false;
        can_write_ = false;
        write_file_->close();
        delete write_file_;
        write_file_ = nullptr;
        write_file_owned_ = false;
        return E_OK;
    }

    int64_t truncated = HEADER_LEN;
    int64_t pos = HEADER_LEN;
    std::vector<char> buf(BUF_SIZE);

    // Recover schema and chunk group meta per Java selfCheck
    std::shared_ptr<IDeviceID> cur_device_id;
    ChunkGroupMeta* cur_cgm = nullptr;
    std::vector<ChunkGroupMeta*> recovered_cgm_list;

    auto flush_chunk_group = [this, &cur_device_id, &cur_cgm,
                              &recovered_cgm_list]() {
        if (cur_cgm != nullptr && cur_device_id != nullptr) {
            get_schema()->update_table_schema(cur_cgm);
            recovered_cgm_list.push_back(cur_cgm);
            cur_cgm = nullptr;
        }
    };

    while (pos < file_size) {
        unsigned char marker;
        ret = reader.read(static_cast<int32_t>(pos),
                          reinterpret_cast<char*>(&marker), 1, read_len);
        if (ret != E_OK || read_len != 1) {
            break;
        }
        pos += 1;

        if (marker == static_cast<unsigned char>(SEPARATOR_MARKER)) {
            truncated = pos - 1;
            flush_chunk_group();
            break;
        }

        if (marker == static_cast<unsigned char>(CHUNK_GROUP_HEADER_MARKER)) {
            truncated = pos - 1;
            flush_chunk_group();
            int seg_len = 0;
            ret = reader.read(static_cast<int32_t>(pos), buf.data(), BUF_SIZE,
                              read_len);
            if (ret != E_OK || read_len < 1) {
                break;
            }
            ByteStream bs;
            bs.wrap_from(buf.data(), read_len);
            cur_device_id = std::make_shared<StringArrayDeviceID>("init");
            ret = cur_device_id->deserialize(bs);
            if (ret != E_OK) {
                break;
            }
            seg_len = bs.read_pos();
            pos += seg_len;
            cur_cgm = new (self_check_arena_.alloc(sizeof(ChunkGroupMeta)))
                ChunkGroupMeta(&self_check_arena_);
            cur_cgm->init(cur_device_id);
            continue;
        }

        if (marker == static_cast<unsigned char>(OPERATION_INDEX_RANGE)) {
            truncated = pos - 1;
            flush_chunk_group();
            cur_device_id.reset();
            if (pos + 2 * 8 > static_cast<int64_t>(file_size)) {
                break;
            }
            char range_buf[16];
            ret =
                reader.read(static_cast<int32_t>(pos), range_buf, 16, read_len);
            if (ret != E_OK || read_len != 16) {
                break;
            }
            pos += 16;
            truncated = pos;
            continue;
        }

        if (marker == static_cast<unsigned char>(CHUNK_HEADER_MARKER) ||
            marker ==
                static_cast<unsigned char>(ONLY_ONE_PAGE_CHUNK_HEADER_MARKER) ||
            (marker & 0x3F) ==
                static_cast<unsigned char>(CHUNK_HEADER_MARKER) ||
            (marker & 0x3F) ==
                static_cast<unsigned char>(ONLY_ONE_PAGE_CHUNK_HEADER_MARKER)) {
            int64_t chunk_start = pos - 1;
            int64_t consumed = 0;
            ChunkHeader chdr;
            ret = parse_chunk_header_and_skip(reader, chunk_start, consumed,
                                              &chdr);
            if (ret != E_OK) {
                break;
            }
            pos = chunk_start + consumed;
            truncated = pos;
            if (cur_cgm != nullptr) {
                void* cm_buf = self_check_arena_.alloc(sizeof(ChunkMeta));
                if (IS_NULL(cm_buf)) {
                    ret = common::E_OOM;
                    break;
                }
                auto* cm = new (cm_buf) ChunkMeta();
                common::String mname;
                mname.dup_from(chdr.measurement_name_, self_check_arena_);
                Statistic* stat = StatisticFactory::alloc_statistic_with_pa(
                    static_cast<common::TSDataType>(chdr.data_type_),
                    &self_check_arena_);
                if (IS_NULL(stat)) {
                    ret = common::E_OOM;
                    break;
                }
                stat->reset();
                cm->init(mname,
                         static_cast<common::TSDataType>(chdr.data_type_),
                         chunk_start, stat, 0,
                         static_cast<common::TSEncoding>(chdr.encoding_type_),
                         static_cast<common::CompressionType>(
                             chdr.compression_type_),
                         self_check_arena_);
                cur_cgm->push(cm);
                if (cur_device_id != nullptr &&
                    (static_cast<unsigned char>(chdr.chunk_type_) & 0x80) !=
                        0) {
                    aligned_devices_.insert(cur_device_id->get_table_name());
                }
            }
            continue;
        }

        truncated_size_ = TSFILE_CHECK_INCOMPATIBLE;
        flush_chunk_group();
        reader.close();
        return E_TSFILE_CORRUPTED;
    }

    flush_chunk_group();
    reader.close();
    truncated_size_ = truncated;

    if (truncate_corrupted && truncated < static_cast<int64_t>(file_size)) {
        ret = write_file_->truncate(truncated);
        if (ret != E_OK) {
            return ret;
        }
    }

    if (write_file_->seek_to_end() != E_OK) {
        return E_FILE_READ_ERR;
    }

    crashed_ = true;
    can_write_ = true;

    ret = init(write_file_);
    if (ret != E_OK) {
        return ret;
    }

    for (ChunkGroupMeta* cgm : recovered_cgm_list) {
        push_chunk_group_meta(cgm);
    }

    return E_OK;
}

bool RestorableTsFileIOWriter::is_device_aligned(
    const std::string& device) const {
    return aligned_devices_.find(device) != aligned_devices_.end();
}

TsFileIOWriter* RestorableTsFileIOWriter::get_tsfile_io_writer() {
    return can_write_ ? this : nullptr;
}

WriteFile* RestorableTsFileIOWriter::get_write_file() {
    return can_write_ ? write_file_ : nullptr;
}

std::string RestorableTsFileIOWriter::get_file_path() const {
    return file_path_;
}

}  // namespace storage
