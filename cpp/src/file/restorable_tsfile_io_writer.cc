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

#include <algorithm>
#include <cstring>
#include <memory>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "common/device_id.h"
#include "common/statistic.h"
#include "common/tsfile_common.h"
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"
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
const unsigned char kTimeChunkTypeMask = 0x80;

// -----------------------------------------------------------------------------
// Self-check helpers: read file, parse chunk header, recover chunk statistics
// -----------------------------------------------------------------------------

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

/**
 * Parse chunk header at chunk_start and compute total chunk size (header +
 * data). Does not read full chunk data; used to advance scan position.
 * @param header_out If non-null, filled with the deserialized chunk header.
 * @param bytes_consumed Set to header_len + data_size on success.
 */
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

/**
 * Recover chunk-level statistic from chunk data so that tail metadata can be
 * generated correctly after recovery (aligned with Java TsFileSequenceReader
 * selfCheck). Multi-page: merge each page header's statistic. Single-page:
 * decode page data and update stat. For aligned value chunks, time_batch
 * (from the time chunk in the same group) must be provided.
 */
static int recover_chunk_statistic(
    const ChunkHeader& chdr, const char* chunk_data, int32_t data_size,
    Statistic* out_stat, common::PageArena* pa,
    const std::vector<int64_t>* time_batch = nullptr,
    std::vector<int64_t>* out_time_batch = nullptr) {
    if (chunk_data == nullptr || data_size <= 0 || out_stat == nullptr) {
        return E_OK;
    }
    common::ByteStream bs;
    bs.wrap_from(const_cast<char*>(chunk_data),
                 static_cast<uint32_t>(data_size));
    // Multi-page chunk: high bits of chunk_type_ are 0x00, low 6 bits =
    // CHUNK_HEADER_MARKER
    const bool multi_page =
        (static_cast<unsigned char>(chdr.chunk_type_) & 0x3F) ==
        static_cast<unsigned char>(CHUNK_HEADER_MARKER);

    if (multi_page) {
        while (bs.remaining_size() > 0) {
            PageHeader ph;
            int ret = ph.deserialize_from(bs, true, chdr.data_type_);
            if (ret != common::E_OK) {
                return ret;
            }
            uint32_t comp = ph.compressed_size_;
            if (ph.statistic_ != nullptr) {
                if (out_stat->merge_with(ph.statistic_) != common::E_OK) {
                    ph.reset();
                    return common::E_TSFILE_CORRUPTED;
                }
            }
            ph.reset();
            bs.wrapped_buf_advance_read_pos(comp);
        }
        return E_OK;
    }

    // Single-page chunk: statistic is not in page header; decompress and decode
    // to fill out_stat. is_time_column: bit 0x80 in chunk_type_ indicates time
    // column (aligned model).
    const bool is_time_column = (static_cast<unsigned char>(chdr.chunk_type_) &
                                 kTimeChunkTypeMask) != 0;
    PageHeader ph;
    int ret = ph.deserialize_from(bs, false, chdr.data_type_);
    if (ret != common::E_OK || ph.compressed_size_ == 0 ||
        bs.remaining_size() < ph.compressed_size_) {
        // Align with Java selfCheck behavior: malformed/incomplete page in this
        // chunk is treated as corrupted data.
        return common::E_TSFILE_CORRUPTED;
    }
    const char* compressed_ptr =
        chunk_data + (data_size - static_cast<int32_t>(bs.remaining_size()));
    char* uncompressed_buf = nullptr;
    uint32_t uncompressed_size = 0;
    Compressor* compressor =
        CompressorFactory::alloc_compressor(chdr.compression_type_);
    if (compressor == nullptr) {
        return common::E_OOM;
    }
    ret = compressor->reset(false);
    if (ret != common::E_OK) {
        CompressorFactory::free(compressor);
        return ret;
    }
    ret = compressor->uncompress(const_cast<char*>(compressed_ptr),
                                 ph.compressed_size_, uncompressed_buf,
                                 uncompressed_size);
    if (ret != common::E_OK || uncompressed_buf == nullptr ||
        uncompressed_size != ph.uncompressed_size_) {
        if (uncompressed_buf != nullptr) {
            compressor->after_uncompress(uncompressed_buf);
        }
        CompressorFactory::free(compressor);
        return (ret == common::E_OK) ? common::E_TSFILE_CORRUPTED : ret;
    }
    if (is_time_column) {
        /* Time chunk: uncompressed = raw time stream only (no var_uint). */
        Decoder* time_decoder = DecoderFactory::alloc_time_decoder();
        if (time_decoder == nullptr) {
            compressor->after_uncompress(uncompressed_buf);
            CompressorFactory::free(compressor);
            return common::E_OOM;
        }
        common::ByteStream time_in;
        time_in.wrap_from(uncompressed_buf, uncompressed_size);
        time_decoder->reset();
        int64_t t;
        if (out_time_batch != nullptr) {
            out_time_batch->clear();
        }
        while (time_decoder->has_remaining(time_in)) {
            if (time_decoder->read_int64(t, time_in) != common::E_OK) {
                break;
            }
            out_stat->update(t);
            if (out_time_batch != nullptr) {
                out_time_batch->push_back(t);
            }
        }
        DecoderFactory::free(time_decoder);
        compressor->after_uncompress(uncompressed_buf);
        CompressorFactory::free(compressor);
        return E_OK;
    }

    /* Value chunk: parse layout and decode. */
    const char* value_buf = nullptr;
    uint32_t value_buf_size = 0;
    std::vector<int64_t> time_decode_buf;
    const std::vector<int64_t>* times = nullptr;

    if (time_batch != nullptr && !time_batch->empty()) {
        // Aligned value page: uncompressed layout = uint32(num_values) + bitmap
        // + value_buf
        if (uncompressed_size < 4) {
            compressor->after_uncompress(uncompressed_buf);
            CompressorFactory::free(compressor);
            return E_OK;
        }
        uint32_t num_values =
            (static_cast<uint32_t>(
                 static_cast<unsigned char>(uncompressed_buf[0]))
             << 24) |
            (static_cast<uint32_t>(
                 static_cast<unsigned char>(uncompressed_buf[1]))
             << 16) |
            (static_cast<uint32_t>(
                 static_cast<unsigned char>(uncompressed_buf[2]))
             << 8) |
            (static_cast<uint32_t>(
                static_cast<unsigned char>(uncompressed_buf[3])));
        uint32_t bitmap_size = (num_values + 7) / 8;
        if (uncompressed_size < 4 + bitmap_size) {
            compressor->after_uncompress(uncompressed_buf);
            CompressorFactory::free(compressor);
            return E_OK;
        }
        value_buf = uncompressed_buf + 4 + bitmap_size;
        value_buf_size = uncompressed_size - 4 - bitmap_size;
        times = time_batch;
    } else {
        // Non-aligned value page: var_uint(time_buf_size) + time_buf +
        // value_buf
        int var_size = 0;
        uint32_t time_buf_size = 0;
        ret = common::SerializationUtil::read_var_uint(
            time_buf_size, uncompressed_buf,
            static_cast<int>(uncompressed_size), &var_size);
        if (ret != common::E_OK ||
            static_cast<uint32_t>(var_size) + time_buf_size >
                uncompressed_size) {
            compressor->after_uncompress(uncompressed_buf);
            CompressorFactory::free(compressor);
            return (ret == common::E_OK) ? common::E_TSFILE_CORRUPTED : ret;
        }
        const char* time_buf = uncompressed_buf + var_size;
        value_buf = time_buf + time_buf_size;
        value_buf_size =
            uncompressed_size - static_cast<uint32_t>(var_size) - time_buf_size;
        Decoder* time_decoder = DecoderFactory::alloc_time_decoder();
        if (time_decoder == nullptr) {
            compressor->after_uncompress(uncompressed_buf);
            CompressorFactory::free(compressor);
            return common::E_OOM;
        }
        common::ByteStream time_in;
        time_in.wrap_from(const_cast<char*>(time_buf), time_buf_size);
        time_decoder->reset();
        time_decode_buf.clear();
        int64_t t;
        while (time_decoder->has_remaining(time_in)) {
            if (time_decoder->read_int64(t, time_in) != common::E_OK) {
                break;
            }
            time_decode_buf.push_back(t);
        }
        DecoderFactory::free(time_decoder);
        times = &time_decode_buf;
    }

    Decoder* value_decoder = DecoderFactory::alloc_value_decoder(
        chdr.encoding_type_, chdr.data_type_);
    if (value_decoder == nullptr) {
        compressor->after_uncompress(uncompressed_buf);
        CompressorFactory::free(compressor);
        return common::E_OOM;
    }
    common::ByteStream value_in;
    value_in.wrap_from(const_cast<char*>(value_buf), value_buf_size);
    value_decoder->reset();
    size_t idx = 0;
    const size_t num_times = times->size();
    while (idx < num_times && value_decoder->has_remaining(value_in)) {
        int64_t t = (*times)[idx];
        switch (chdr.data_type_) {
            case common::BOOLEAN: {
                bool v;
                if (value_decoder->read_boolean(v, value_in) == common::E_OK) {
                    out_stat->update(t, v);
                }
                break;
            }
            case common::INT32:
            case common::DATE: {
                int32_t v;
                if (value_decoder->read_int32(v, value_in) == common::E_OK) {
                    out_stat->update(t, v);
                }
                break;
            }
            case common::INT64:
            case common::TIMESTAMP: {
                int64_t v;
                if (value_decoder->read_int64(v, value_in) == common::E_OK) {
                    out_stat->update(t, v);
                }
                break;
            }
            case common::FLOAT: {
                float v;
                if (value_decoder->read_float(v, value_in) == common::E_OK) {
                    out_stat->update(t, v);
                }
                break;
            }
            case common::DOUBLE: {
                double v;
                if (value_decoder->read_double(v, value_in) == common::E_OK) {
                    out_stat->update(t, v);
                }
                break;
            }
            case common::TEXT:
            case common::BLOB:
            case common::STRING: {
                common::String v;
                if (pa != nullptr && value_decoder->read_String(
                                         v, *pa, value_in) == common::E_OK) {
                    out_stat->update(t, v);
                }
                break;
            }
            default:
                break;
        }
        idx++;
    }
    DecoderFactory::free(value_decoder);
    compressor->after_uncompress(uncompressed_buf);
    CompressorFactory::free(compressor);
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
    for (ChunkGroupMeta* cgm : self_check_recovered_cgm_) {
        cgm->device_id_.reset();
    }
    self_check_recovered_cgm_.clear();
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
    // Use a separate read-only handle for self-check: on Windows, sharing the
    // O_RDWR fd can cause stale/cached reads when detecting a complete file.
    int ret = reader.open(file_path_);
    if (ret != E_OK) {
        return ret;
    }

    int32_t file_size = reader.file_size();

    // --- Empty file: treat as crashed, allow writing from scratch ---
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

    // --- File too short or invalid header => not a valid TsFile ---
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

    // --- Completeness check (aligned with Java isComplete()) ---
    // Require size >= 2*magic + version_byte and tail magic same as head magic.
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

    // --- File is complete: no recovery, close write handle and return ---
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

    // --- Recovery path: scan from header to find last valid truncation point
    // ---
    int64_t truncated = HEADER_LEN;
    int64_t pos = HEADER_LEN;
    std::vector<char> buf(BUF_SIZE);

    // Recover schema and chunk group meta (aligned with Java selfCheck).
    // cur_group_time_batch: timestamps decoded from time chunk, used by aligned
    // value chunks.
    std::shared_ptr<IDeviceID> cur_device_id;
    ChunkGroupMeta* cur_cgm = nullptr;
    std::vector<ChunkGroupMeta*> recovered_cgm_list;
    std::vector<int64_t> cur_group_time_batch;

    auto flush_chunk_group = [this, &cur_device_id, &cur_cgm,
                              &recovered_cgm_list]() {
        if (cur_cgm != nullptr && cur_device_id != nullptr) {
            get_schema()->update_table_schema(cur_cgm);
            recovered_cgm_list.push_back(cur_cgm);
            self_check_recovered_cgm_.push_back(cur_cgm);
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
            cur_group_time_batch.clear();
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
                if (chdr.data_size_ > 0) {
                    const int32_t header_len =
                        static_cast<int32_t>(consumed) - chdr.data_size_;
                    if (header_len > 0 && chunk_start + consumed <=
                                              static_cast<int64_t>(file_size)) {
                        std::vector<char> chunk_data(chdr.data_size_);
                        int32_t read_len = 0;
                        ret = reader.read(
                            static_cast<int32_t>(chunk_start + header_len),
                            chunk_data.data(), chdr.data_size_, read_len);
                        if (ret == E_OK && read_len == chdr.data_size_) {
                            ret = recover_chunk_statistic(
                                chdr, chunk_data.data(), chdr.data_size_, stat,
                                &self_check_arena_, &cur_group_time_batch,
                                &cur_group_time_batch);
                        }
                        if (ret != E_OK) {
                            break;
                        }
                    }
                }
                cm->init(mname,
                         static_cast<common::TSDataType>(chdr.data_type_),
                         chunk_start, stat, 0,
                         static_cast<common::TSEncoding>(chdr.encoding_type_),
                         static_cast<common::CompressionType>(
                             chdr.compression_type_),
                         self_check_arena_);
                cur_cgm->push(cm);
                if (cur_device_id != nullptr &&
                    (static_cast<unsigned char>(chdr.chunk_type_) &
                     kTimeChunkTypeMask) != 0) {
                    // For aligned series, a time chunk implies this device
                    // uses aligned layout. Record it so recovered writer state
                    // can keep alignment behavior consistent.
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
    get_schema()->finalize_table_schemas();
    reader.close();
    truncated_size_ = truncated;

    // --- Optionally truncate file to last valid offset ---
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

    // --- Restore write_stream_ logical position from existing file size ---
    const int64_t restored_size = write_file_->get_position();
    if (restored_size > 0) {
        ret = restore_recovered_file_position(restored_size);
        if (ret != E_OK) {
            return ret;
        }
    }

    // --- Attach recovered ChunkGroupMeta to writer; destroy() will not free
    // them ---
    for (ChunkGroupMeta* cgm : recovered_cgm_list) {
        push_chunk_group_meta(cgm);
    }
    chunk_group_meta_from_recovery_ = true;

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
