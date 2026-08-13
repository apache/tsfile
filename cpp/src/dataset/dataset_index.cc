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

#include "dataset/dataset_index.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <sstream>

#ifdef _WIN32
#include <Windows.h>
#include <direct.h>
#include <fcntl.h>
#include <io.h>
#include <process.h>
#else
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

namespace storage {
namespace dataset {

namespace {

const char DATASET_INDEX_MAGIC[8] = {'T', 'S', 'I', 'D', 'X', 0, 0, 0};

bool is_little_endian() {
    const uint16_t value = 1;
    return *reinterpret_cast<const uint8_t*>(&value) == 1;
}

uint64_t align64(uint64_t value) {
    if (value > std::numeric_limits<uint64_t>::max() - 63) {
        return 0;
    }
    return (value + 63) & ~static_cast<uint64_t>(63);
}

bool add_overflows(uint64_t left, uint64_t right) {
    return left > std::numeric_limits<uint64_t>::max() - right;
}

bool multiply_overflows(uint64_t left, uint64_t right) {
    return right != 0 && left > std::numeric_limits<uint64_t>::max() / right;
}

bool range_valid(uint32_t first, uint32_t count, uint32_t total) {
    return first <= total && count <= total - first;
}

uint32_t expected_record_size(DatasetIndexSectionType type) {
    switch (type) {
        case DatasetIndexSectionType::STRING_OFFSETS:
            return sizeof(uint32_t);
        case DatasetIndexSectionType::STRING_BYTES:
            return 0;
        case DatasetIndexSectionType::TABLE_NAME_INDEX:
            return sizeof(TableNameIndexRecord);
        case DatasetIndexSectionType::TABLE_RECORD:
            return sizeof(TableRecord);
        case DatasetIndexSectionType::DEVICE_NAME_INDEX:
            return sizeof(DeviceNameIndexRecord);
        case DatasetIndexSectionType::DEVICE_RECORD:
            return sizeof(DeviceRecord);
        case DatasetIndexSectionType::COLUMN_NAME_INDEX:
            return sizeof(ColumnNameIndexRecord);
        case DatasetIndexSectionType::COLUMN_SCHEMA:
            return sizeof(ColumnSchemaRecord);
        case DatasetIndexSectionType::LOGICAL_SERIES:
            return sizeof(LogicalSeriesRecord);
        case DatasetIndexSectionType::TSFILE_RECORD:
            return sizeof(TsFileRecord);
        case DatasetIndexSectionType::TABLE_FILE_SPAN:
            return sizeof(TableFileSpanRecord);
        case DatasetIndexSectionType::DEVICE_FILE_SPAN:
            return sizeof(DeviceFileSpanRecord);
        case DatasetIndexSectionType::SERIES_FILE_SPAN:
            return sizeof(SeriesFileSpanRecord);
        case DatasetIndexSectionType::SERIES_LOCATOR:
            return sizeof(SeriesLocatorRecord);
    }
    return std::numeric_limits<uint32_t>::max();
}

std::string parent_directory(const std::string& path) {
    const std::string::size_type pos = path.find_last_of("/\\");
    if (pos == std::string::npos) {
        return ".";
    }
    if (pos == 0) {
        return path.substr(0, 1);
    }
    return path.substr(0, pos);
}

std::string system_error_message(const char* operation) {
    std::ostringstream stream;
    stream << operation << " failed: " << std::strerror(errno);
    return stream.str();
}

#ifdef _WIN32
typedef int NativeFile;
const NativeFile INVALID_NATIVE_FILE = -1;

NativeFile open_exclusive(const std::string& path) {
    return _open(path.c_str(), _O_BINARY | _O_CREAT | _O_EXCL | _O_WRONLY,
                 _S_IREAD | _S_IWRITE);
}

int native_write(NativeFile file, const void* data, size_t size) {
    const char* cursor = static_cast<const char*>(data);
    while (size > 0) {
        const unsigned int chunk = static_cast<unsigned int>(
            std::min<size_t>(size, std::numeric_limits<int>::max()));
        const int written = _write(file, cursor, chunk);
        if (written <= 0) {
            return -1;
        }
        cursor += written;
        size -= static_cast<size_t>(written);
    }
    return 0;
}

int native_sync(NativeFile file) { return _commit(file); }
void native_close(NativeFile file) { _close(file); }
int process_id() { return _getpid(); }
#else
typedef int NativeFile;
const NativeFile INVALID_NATIVE_FILE = -1;

NativeFile open_exclusive(const std::string& path) {
    return ::open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY, 0644);
}

int native_write(NativeFile file, const void* data, size_t size) {
    const uint8_t* cursor = static_cast<const uint8_t*>(data);
    while (size > 0) {
        const ssize_t written = ::write(file, cursor, size);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        if (written == 0) {
            errno = EIO;
            return -1;
        }
        cursor += written;
        size -= static_cast<size_t>(written);
    }
    return 0;
}

int native_sync(NativeFile file) { return ::fsync(file); }
void native_close(NativeFile file) { ::close(file); }
int process_id() { return static_cast<int>(::getpid()); }
#endif

int write_zero_padding(NativeFile file, uint64_t length) {
    static const uint8_t zeros[64] = {0};
    while (length > 0) {
        const size_t chunk =
            static_cast<size_t>(std::min<uint64_t>(length, sizeof(zeros)));
        if (native_write(file, zeros, chunk) != 0) {
            return -1;
        }
        length -= chunk;
    }
    return 0;
}

bool string_equals(const MappedDatasetIndex& index, uint32_t sid,
                   const std::string& expected) {
    DatasetIndexStringView value;
    return index.string(sid, value) == DatasetIndexStatus::OK &&
           value.length == expected.size() &&
           (value.length == 0 ||
            std::memcmp(value.data, expected.data(), value.length) == 0);
}

}  // namespace

const char* dataset_index_status_name(DatasetIndexStatus status) {
    switch (status) {
        case DatasetIndexStatus::OK:
            return "OK";
        case DatasetIndexStatus::INVALID_ARGUMENT:
            return "INVALID_ARGUMENT";
        case DatasetIndexStatus::IO_ERROR:
            return "IO_ERROR";
        case DatasetIndexStatus::OUT_OF_MEMORY:
            return "OUT_OF_MEMORY";
        case DatasetIndexStatus::BAD_MAGIC:
            return "BAD_MAGIC";
        case DatasetIndexStatus::UNSUPPORTED_VERSION:
            return "UNSUPPORTED_VERSION";
        case DatasetIndexStatus::BAD_HEADER:
            return "BAD_HEADER";
        case DatasetIndexStatus::BAD_DIRECTORY:
            return "BAD_DIRECTORY";
        case DatasetIndexStatus::BAD_SECTION:
            return "BAD_SECTION";
        case DatasetIndexStatus::BAD_CHECKSUM:
            return "BAD_CHECKSUM";
        case DatasetIndexStatus::BAD_REFERENCE:
            return "BAD_REFERENCE";
        case DatasetIndexStatus::NOT_FOUND:
            return "NOT_FOUND";
    }
    return "UNKNOWN";
}

uint32_t dataset_index_crc32c(const void* data, size_t length) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = ~static_cast<uint32_t>(0);
    for (size_t i = 0; i < length; ++i) {
        crc ^= bytes[i];
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask =
                static_cast<uint32_t>(-(static_cast<int32_t>(crc & 1)));
            crc = (crc >> 1) ^ (0x82F63B78U & mask);
        }
    }
    return ~crc;
}

uint64_t dataset_index_name_hash(const char* data, size_t length) {
    uint64_t hash = 1469598103934665603ULL;
    for (size_t i = 0; i < length; ++i) {
        hash ^= static_cast<uint8_t>(data[i]);
        hash *= 1099511628211ULL;
    }
    return hash;
}

DatasetIndexStatus DatasetIndexWriter::write_atomic(
    const std::string& output_path,
    const std::vector<DatasetIndexSectionData>& input_sections,
    std::string& error_message) {
    error_message.clear();
    if (output_path.empty() || !is_little_endian()) {
        error_message = "empty output path or unsupported host byte order";
        return DatasetIndexStatus::INVALID_ARGUMENT;
    }
    if (input_sections.size() != DATASET_INDEX_SECTION_COUNT) {
        error_message = "current v1 writer requires exactly 14 sections";
        return DatasetIndexStatus::INVALID_ARGUMENT;
    }

    std::vector<DatasetIndexSectionData> sections(input_sections);
    std::sort(sections.begin(), sections.end(),
              [](const DatasetIndexSectionData& left,
                 const DatasetIndexSectionData& right) {
                  return static_cast<uint32_t>(left.type) <
                         static_cast<uint32_t>(right.type);
              });

    DatasetIndexHeader header;
    std::memset(&header, 0, sizeof(header));
    std::memcpy(header.magic, DATASET_INDEX_MAGIC, sizeof(header.magic));
    header.version_major = DATASET_INDEX_VERSION_MAJOR;
    header.version_minor = DATASET_INDEX_VERSION_MINOR;
    header.header_size = DATASET_INDEX_HEADER_SIZE;
    header.directory_offset = DATASET_INDEX_HEADER_SIZE;
    header.section_count = DATASET_INDEX_SECTION_COUNT;
    header.directory_entry_size = DATASET_INDEX_DIRECTORY_ENTRY_SIZE;

    std::vector<DatasetIndexDirectoryEntry> directory(
        DATASET_INDEX_SECTION_COUNT);
    uint64_t next_offset = align64(header.directory_offset +
                                   static_cast<uint64_t>(directory.size()) *
                                       sizeof(DatasetIndexDirectoryEntry));
    if (next_offset == 0) {
        error_message = "directory size overflows";
        return DatasetIndexStatus::INVALID_ARGUMENT;
    }

    for (uint32_t i = 0; i < DATASET_INDEX_SECTION_COUNT; ++i) {
        const uint32_t expected_type = i + 1;
        const uint32_t actual_type = static_cast<uint32_t>(sections[i].type);
        const uint32_t expected_size = expected_record_size(sections[i].type);
        if (actual_type != expected_type ||
            sections[i].record_size != expected_size) {
            error_message = "section type or record size does not match v1";
            return DatasetIndexStatus::INVALID_ARGUMENT;
        }
        if (expected_size != 0 &&
            (multiply_overflows(sections[i].count, expected_size) ||
             static_cast<uint64_t>(sections[i].count) * expected_size !=
                 sections[i].bytes.size())) {
            error_message = "fixed-size section count does not match bytes";
            return DatasetIndexStatus::INVALID_ARGUMENT;
        }
        if (expected_size == 0 &&
            sections[i].count != sections[i].bytes.size()) {
            error_message = "blob section count must equal byte length";
            return DatasetIndexStatus::INVALID_ARGUMENT;
        }
        DatasetIndexDirectoryEntry& entry = directory[i];
        std::memset(&entry, 0, sizeof(entry));
        entry.section_type = actual_type;
        entry.record_size = expected_size;
        entry.offset = next_offset;
        entry.length = sections[i].bytes.size();
        entry.count = sections[i].count;
        entry.crc32c = dataset_index_crc32c(
            sections[i].bytes.empty() ? nullptr : sections[i].bytes.data(),
            sections[i].bytes.size());
        if (add_overflows(entry.offset, entry.length)) {
            error_message = "section file range overflows";
            return DatasetIndexStatus::INVALID_ARGUMENT;
        }
        next_offset = align64(entry.offset + entry.length);
        if (next_offset == 0 && i + 1 < DATASET_INDEX_SECTION_COUNT) {
            error_message = "section alignment overflows";
            return DatasetIndexStatus::INVALID_ARGUMENT;
        }
    }

    const DatasetIndexDirectoryEntry& last = directory.back();
    header.file_length = last.offset + last.length;
    header.header_crc32c = 0;
    header.header_crc32c = dataset_index_crc32c(&header, sizeof(header));

    std::ostringstream temp_name;
    temp_name << output_path << ".tmp." << process_id();
    const std::string temp_path = temp_name.str();
    NativeFile file = open_exclusive(temp_path);
    if (file == INVALID_NATIVE_FILE) {
        error_message = system_error_message("create temporary index");
        return DatasetIndexStatus::IO_ERROR;
    }

    DatasetIndexStatus status = DatasetIndexStatus::OK;
    uint64_t cursor = 0;
    if (native_write(file, &header, sizeof(header)) != 0 ||
        native_write(file, directory.data(),
                     directory.size() * sizeof(directory[0])) != 0) {
        error_message = system_error_message("write index header");
        status = DatasetIndexStatus::IO_ERROR;
    } else {
        cursor = header.directory_offset +
                 directory.size() * sizeof(DatasetIndexDirectoryEntry);
    }

    for (uint32_t i = 0;
         status == DatasetIndexStatus::OK && i < directory.size(); ++i) {
        if (directory[i].offset < cursor ||
            write_zero_padding(file, directory[i].offset - cursor) != 0 ||
            native_write(
                file,
                sections[i].bytes.empty() ? nullptr : sections[i].bytes.data(),
                sections[i].bytes.size()) != 0) {
            error_message = system_error_message("write index section");
            status = DatasetIndexStatus::IO_ERROR;
            break;
        }
        cursor = directory[i].offset + directory[i].length;
    }
    if (status == DatasetIndexStatus::OK && native_sync(file) != 0) {
        error_message = system_error_message("fsync index");
        status = DatasetIndexStatus::IO_ERROR;
    }
    native_close(file);

    if (status == DatasetIndexStatus::OK) {
#ifdef _WIN32
        if (!MoveFileExA(temp_path.c_str(), output_path.c_str(),
                         MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)) {
            error_message = "replace index failed";
            status = DatasetIndexStatus::IO_ERROR;
        }
#else
        if (::rename(temp_path.c_str(), output_path.c_str()) != 0) {
            error_message = system_error_message("replace index");
            status = DatasetIndexStatus::IO_ERROR;
        } else {
            const std::string directory_path = parent_directory(output_path);
            const int directory_fd = ::open(directory_path.c_str(), O_RDONLY);
            if (directory_fd >= 0) {
                if (::fsync(directory_fd) != 0) {
                    error_message =
                        system_error_message("fsync index directory");
                    status = DatasetIndexStatus::IO_ERROR;
                }
                ::close(directory_fd);
            }
        }
#endif
    }
    if (status != DatasetIndexStatus::OK) {
#ifdef _WIN32
        _unlink(temp_path.c_str());
#else
        ::unlink(temp_path.c_str());
#endif
    }
    return status;
}

MappedDatasetIndex::MappedDatasetIndex()
    : mapping_(nullptr),
      mapping_size_(0),
      header_(nullptr),
      directory_(nullptr)
#ifdef _WIN32
      ,
      file_handle_(INVALID_HANDLE_VALUE),
      mapping_handle_(nullptr)
#else
      ,
      fd_(-1)
#endif
{
}

MappedDatasetIndex::~MappedDatasetIndex() { close(); }

DatasetIndexStatus MappedDatasetIndex::fail(DatasetIndexStatus status,
                                            const std::string& message) {
    error_message_ = message;
    return status;
}

DatasetIndexStatus MappedDatasetIndex::open(const std::string& path) {
    close();
    path_ = path;
    if (path.empty() || !is_little_endian()) {
        return fail(DatasetIndexStatus::INVALID_ARGUMENT,
                    "empty path or unsupported host byte order");
    }
#ifdef _WIN32
    HANDLE file =
        CreateFileA(path.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr,
                    OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (file == INVALID_HANDLE_VALUE) {
        return fail(DatasetIndexStatus::IO_ERROR, "open index failed");
    }
    LARGE_INTEGER size;
    if (!GetFileSizeEx(file, &size) || size.QuadPart <= 0) {
        CloseHandle(file);
        return fail(DatasetIndexStatus::IO_ERROR, "stat index failed");
    }
    HANDLE mapping =
        CreateFileMappingA(file, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (mapping == nullptr) {
        CloseHandle(file);
        return fail(DatasetIndexStatus::IO_ERROR, "map index failed");
    }
    const void* address = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, 0);
    if (address == nullptr) {
        CloseHandle(mapping);
        CloseHandle(file);
        return fail(DatasetIndexStatus::IO_ERROR, "map view failed");
    }
    file_handle_ = file;
    mapping_handle_ = mapping;
    mapping_ = static_cast<const uint8_t*>(address);
    mapping_size_ = static_cast<uint64_t>(size.QuadPart);
#else
    fd_ = ::open(path.c_str(), O_RDONLY);
    if (fd_ < 0) {
        return fail(DatasetIndexStatus::IO_ERROR,
                    system_error_message("open index"));
    }
    struct stat stat_buffer;
    if (::fstat(fd_, &stat_buffer) != 0 || stat_buffer.st_size <= 0) {
        close();
        return fail(DatasetIndexStatus::IO_ERROR,
                    system_error_message("stat index"));
    }
    mapping_size_ = static_cast<uint64_t>(stat_buffer.st_size);
    void* address =
        ::mmap(nullptr, mapping_size_, PROT_READ, MAP_SHARED, fd_, 0);
    if (address == MAP_FAILED) {
        mapping_ = nullptr;
        close();
        return fail(DatasetIndexStatus::IO_ERROR,
                    system_error_message("mmap index"));
    }
    mapping_ = static_cast<const uint8_t*>(address);
#endif
    DatasetIndexStatus status = validate();
    if (status != DatasetIndexStatus::OK) {
        const std::string message = error_message_;
        close();
        error_message_ = message;
    }
    return status;
}

void MappedDatasetIndex::close() {
#ifdef _WIN32
    if (mapping_ != nullptr) {
        UnmapViewOfFile(mapping_);
    }
    if (mapping_handle_ != nullptr) {
        CloseHandle(static_cast<HANDLE>(mapping_handle_));
    }
    if (file_handle_ != INVALID_HANDLE_VALUE) {
        CloseHandle(static_cast<HANDLE>(file_handle_));
    }
    file_handle_ = INVALID_HANDLE_VALUE;
    mapping_handle_ = nullptr;
#else
    if (mapping_ != nullptr) {
        ::munmap(const_cast<uint8_t*>(mapping_), mapping_size_);
    }
    if (fd_ >= 0) {
        ::close(fd_);
    }
    fd_ = -1;
#endif
    mapping_ = nullptr;
    mapping_size_ = 0;
    header_ = nullptr;
    directory_ = nullptr;
    path_.clear();
}

DatasetIndexStatus MappedDatasetIndex::section(
    DatasetIndexSectionType type, DatasetIndexSectionView& result) const {
    result = DatasetIndexSectionView();
    if (directory_ == nullptr) {
        return DatasetIndexStatus::BAD_HEADER;
    }
    const uint32_t raw_type = static_cast<uint32_t>(type);
    if (raw_type == 0 || raw_type > header_->section_count) {
        return DatasetIndexStatus::NOT_FOUND;
    }
    const DatasetIndexDirectoryEntry& entry = directory_[raw_type - 1];
    if (entry.section_type != raw_type) {
        return DatasetIndexStatus::NOT_FOUND;
    }
    result.data = mapping_ + entry.offset;
    result.length = entry.length;
    result.record_size = entry.record_size;
    result.count = entry.count;
    return DatasetIndexStatus::OK;
}

DatasetIndexStatus MappedDatasetIndex::string(
    uint32_t sid, DatasetIndexStringView& result) const {
    result = DatasetIndexStringView();
    DatasetIndexSectionView offsets;
    DatasetIndexSectionView bytes;
    if (section(DatasetIndexSectionType::STRING_OFFSETS, offsets) !=
            DatasetIndexStatus::OK ||
        section(DatasetIndexSectionType::STRING_BYTES, bytes) !=
            DatasetIndexStatus::OK ||
        offsets.count == 0 || sid + 1 >= offsets.count) {
        return DatasetIndexStatus::NOT_FOUND;
    }
    const uint32_t* values = reinterpret_cast<const uint32_t*>(offsets.data);
    result.data = reinterpret_cast<const char*>(bytes.data + values[sid]);
    result.length = values[sid + 1] - values[sid];
    return DatasetIndexStatus::OK;
}

DatasetIndexStatus MappedDatasetIndex::validate() {
    if (mapping_size_ < sizeof(DatasetIndexHeader)) {
        return fail(DatasetIndexStatus::BAD_HEADER,
                    "index is shorter than header");
    }
    header_ = reinterpret_cast<const DatasetIndexHeader*>(mapping_);
    if (std::memcmp(header_->magic, DATASET_INDEX_MAGIC,
                    sizeof(header_->magic)) != 0) {
        return fail(DatasetIndexStatus::BAD_MAGIC, "bad dataset index magic");
    }
    if (header_->version_major != DATASET_INDEX_VERSION_MAJOR ||
        header_->version_minor != DATASET_INDEX_VERSION_MINOR) {
        return fail(DatasetIndexStatus::UNSUPPORTED_VERSION,
                    "unsupported dataset index version");
    }
    if (header_->header_size != sizeof(DatasetIndexHeader) ||
        header_->directory_entry_size != sizeof(DatasetIndexDirectoryEntry) ||
        header_->section_count != DATASET_INDEX_SECTION_COUNT ||
        header_->directory_offset < sizeof(DatasetIndexHeader) ||
        header_->file_length != mapping_size_) {
        return fail(DatasetIndexStatus::BAD_HEADER,
                    "header size, directory shape, or file length is invalid");
    }
    for (size_t i = 0; i < sizeof(header_->reserved); ++i) {
        if (header_->reserved[i] != 0) {
            return fail(DatasetIndexStatus::BAD_HEADER,
                        "header reserved bytes are not zero");
        }
    }
    DatasetIndexHeader header_copy = *header_;
    const uint32_t expected_header_crc = header_copy.header_crc32c;
    header_copy.header_crc32c = 0;
    if (dataset_index_crc32c(&header_copy, sizeof(header_copy)) !=
        expected_header_crc) {
        return fail(DatasetIndexStatus::BAD_CHECKSUM,
                    "header checksum does not match");
    }
    const uint64_t directory_length =
        static_cast<uint64_t>(header_->section_count) *
        header_->directory_entry_size;
    if (add_overflows(header_->directory_offset, directory_length) ||
        header_->directory_offset + directory_length > mapping_size_) {
        return fail(DatasetIndexStatus::BAD_DIRECTORY,
                    "section directory is outside the file");
    }
    directory_ = reinterpret_cast<const DatasetIndexDirectoryEntry*>(
        mapping_ + header_->directory_offset);
    uint64_t minimum_section_offset =
        align64(header_->directory_offset + directory_length);
    uint64_t previous_end = minimum_section_offset;
    for (uint32_t i = 0; i < header_->section_count; ++i) {
        const DatasetIndexDirectoryEntry& entry = directory_[i];
        const DatasetIndexSectionType type =
            static_cast<DatasetIndexSectionType>(i + 1);
        if (entry.section_type != i + 1 ||
            entry.record_size != expected_record_size(type)) {
            return fail(DatasetIndexStatus::BAD_DIRECTORY,
                        "section type order or record size is invalid");
        }
        if (entry.offset % DATASET_INDEX_ALIGNMENT != 0 ||
            entry.offset < minimum_section_offset ||
            entry.offset < previous_end ||
            add_overflows(entry.offset, entry.length) ||
            entry.offset + entry.length > mapping_size_) {
            return fail(
                DatasetIndexStatus::BAD_SECTION,
                "section range is unaligned, overlapping, or out of bounds");
        }
        if (entry.record_size == 0) {
            if (entry.count != entry.length) {
                return fail(DatasetIndexStatus::BAD_SECTION,
                            "blob count does not equal byte length");
            }
        } else if (multiply_overflows(entry.count, entry.record_size) ||
                   static_cast<uint64_t>(entry.count) * entry.record_size >
                       entry.length) {
            return fail(DatasetIndexStatus::BAD_SECTION,
                        "section record count exceeds section length");
        }
        const uint32_t actual_crc = dataset_index_crc32c(
            entry.length == 0 ? nullptr : mapping_ + entry.offset,
            static_cast<size_t>(entry.length));
        if (actual_crc != entry.crc32c) {
            return fail(DatasetIndexStatus::BAD_CHECKSUM,
                        "section checksum does not match");
        }
        previous_end = entry.offset + entry.length;
    }

    DatasetIndexSectionView string_offsets_view;
    DatasetIndexSectionView string_bytes_view;
    section(DatasetIndexSectionType::STRING_OFFSETS, string_offsets_view);
    section(DatasetIndexSectionType::STRING_BYTES, string_bytes_view);
    if (string_offsets_view.count == 0) {
        return fail(DatasetIndexStatus::BAD_REFERENCE,
                    "StringOffsets must contain the terminal offset");
    }
    const uint32_t* string_offsets =
        reinterpret_cast<const uint32_t*>(string_offsets_view.data);
    if (string_offsets[0] != 0) {
        return fail(DatasetIndexStatus::BAD_REFERENCE,
                    "StringOffsets must start at zero");
    }
    for (uint32_t i = 1; i < string_offsets_view.count; ++i) {
        if (string_offsets[i] < string_offsets[i - 1] ||
            string_offsets[i] > string_bytes_view.length) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "StringOffsets is not monotonic or is out of bounds");
        }
    }
    if (string_offsets[string_offsets_view.count - 1] !=
        string_bytes_view.length) {
        return fail(DatasetIndexStatus::BAD_REFERENCE,
                    "terminal string offset does not equal StringBytes length");
    }
    const uint32_t string_count = string_offsets_view.count - 1;

    DatasetIndexSectionView table_names;
    DatasetIndexSectionView tables;
    DatasetIndexSectionView device_names;
    DatasetIndexSectionView devices;
    DatasetIndexSectionView column_names;
    DatasetIndexSectionView columns;
    DatasetIndexSectionView series;
    DatasetIndexSectionView files;
    DatasetIndexSectionView table_spans;
    DatasetIndexSectionView device_spans;
    DatasetIndexSectionView series_spans;
    DatasetIndexSectionView locators;
    section(DatasetIndexSectionType::TABLE_NAME_INDEX, table_names);
    section(DatasetIndexSectionType::TABLE_RECORD, tables);
    section(DatasetIndexSectionType::DEVICE_NAME_INDEX, device_names);
    section(DatasetIndexSectionType::DEVICE_RECORD, devices);
    section(DatasetIndexSectionType::COLUMN_NAME_INDEX, column_names);
    section(DatasetIndexSectionType::COLUMN_SCHEMA, columns);
    section(DatasetIndexSectionType::LOGICAL_SERIES, series);
    section(DatasetIndexSectionType::TSFILE_RECORD, files);
    section(DatasetIndexSectionType::TABLE_FILE_SPAN, table_spans);
    section(DatasetIndexSectionType::DEVICE_FILE_SPAN, device_spans);
    section(DatasetIndexSectionType::SERIES_FILE_SPAN, series_spans);
    section(DatasetIndexSectionType::SERIES_LOCATOR, locators);

#define DATASET_RECORDS(view, type) reinterpret_cast<const type*>((view).data)
    const TableNameIndexRecord* table_name_records =
        DATASET_RECORDS(table_names, TableNameIndexRecord);
    const TableRecord* table_records = DATASET_RECORDS(tables, TableRecord);
    const DeviceNameIndexRecord* device_name_records =
        DATASET_RECORDS(device_names, DeviceNameIndexRecord);
    const DeviceRecord* device_records = DATASET_RECORDS(devices, DeviceRecord);
    const ColumnNameIndexRecord* column_name_records =
        DATASET_RECORDS(column_names, ColumnNameIndexRecord);
    const ColumnSchemaRecord* column_records =
        DATASET_RECORDS(columns, ColumnSchemaRecord);
    const LogicalSeriesRecord* series_records =
        DATASET_RECORDS(series, LogicalSeriesRecord);
    const TsFileRecord* file_records = DATASET_RECORDS(files, TsFileRecord);
    const TableFileSpanRecord* table_span_records =
        DATASET_RECORDS(table_spans, TableFileSpanRecord);
    const DeviceFileSpanRecord* device_span_records =
        DATASET_RECORDS(device_spans, DeviceFileSpanRecord);
    const SeriesFileSpanRecord* series_span_records =
        DATASET_RECORDS(series_spans, SeriesFileSpanRecord);
    const SeriesLocatorRecord* locator_records =
        DATASET_RECORDS(locators, SeriesLocatorRecord);
#undef DATASET_RECORDS

    for (uint32_t i = 0; i < table_names.count; ++i) {
        if (table_name_records[i].name_sid >= string_count ||
            table_name_records[i].table_id >= tables.count) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "TableNameIndex contains an invalid reference");
        }
    }
    for (uint32_t i = 0; i < tables.count; ++i) {
        const TableRecord& value = table_records[i];
        if (value.name_sid >= string_count || value.reserved0 != 0 ||
            value.reserved1 != 0 ||
            !range_valid(value.first_device_name_index, value.device_count,
                         device_names.count) ||
            !range_valid(value.first_column_name_index, value.column_count,
                         column_names.count) ||
            !range_valid(value.first_file_span, value.file_span_count,
                         table_spans.count)) {
            return fail(
                DatasetIndexStatus::BAD_REFERENCE,
                "TableRecord contains an invalid range or reserved value");
        }
    }
    for (uint32_t i = 0; i < device_names.count; ++i) {
        const DeviceNameIndexRecord& value = device_name_records[i];
        if (value.table_id >= tables.count ||
            value.device_id >= devices.count ||
            value.name_sid >= string_count || value.reserved != 0) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "DeviceNameIndex contains an invalid reference");
        }
    }
    for (uint32_t i = 0; i < devices.count; ++i) {
        const DeviceRecord& value = device_records[i];
        if (value.table_id >= tables.count || value.name_sid >= string_count ||
            value.reserved0 != 0 || value.reserved1 != 0 ||
            !range_valid(value.first_series_id, value.series_count,
                         series.count) ||
            !range_valid(value.first_file_span, value.file_span_count,
                         device_spans.count) ||
            (value.series_count != 0 && value.min_time > value.max_time)) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "DeviceRecord contains an invalid reference or range");
        }
    }
    for (uint32_t i = 0; i < column_names.count; ++i) {
        const ColumnNameIndexRecord& value = column_name_records[i];
        if (value.table_id >= tables.count ||
            value.column_id >= columns.count ||
            value.name_sid >= string_count || value.reserved != 0) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "ColumnNameIndex contains an invalid reference");
        }
    }
    for (uint32_t i = 0; i < columns.count; ++i) {
        const ColumnSchemaRecord& value = column_records[i];
        if (value.table_id >= tables.count || value.name_sid >= string_count ||
            value.nullable > 1 || value.reserved != 0) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "ColumnSchema contains an invalid reference or enum");
        }
    }
    for (uint32_t i = 0; i < series.count; ++i) {
        const LogicalSeriesRecord& value = series_records[i];
        if (value.device_id >= devices.count ||
            value.column_id >= columns.count ||
            !range_valid(value.first_file_span, value.file_span_count,
                         series_spans.count) ||
            (value.file_span_count != 0 && value.min_time > value.max_time)) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "LogicalSeries contains an invalid reference or range");
        }
    }
    for (uint32_t i = 0; i < files.count; ++i) {
        const TsFileRecord& value = file_records[i];
        if (value.path_sid >= string_count || value.reserved0 != 0 ||
            value.reserved1 != 0 || value.min_time > value.max_time) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "TsFileRecord contains an invalid reference or range");
        }
    }
    for (uint32_t i = 0; i < table_spans.count; ++i) {
        const TableFileSpanRecord& value = table_span_records[i];
        if (value.table_id >= tables.count || value.file_id >= files.count ||
            value.min_time > value.max_time) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "TableFileSpan contains an invalid reference or range");
        }
    }
    for (uint32_t i = 0; i < device_spans.count; ++i) {
        const DeviceFileSpanRecord& value = device_span_records[i];
        if (value.device_id >= devices.count || value.file_id >= files.count ||
            value.layout > 1 ||
            (value.flags & ~static_cast<uint16_t>(1)) != 0 ||
            value.min_time > value.max_time ||
            (value.layout == 0 &&
             (value.time_meta_offset != 0 || value.time_meta_length != 0)) ||
            (value.layout == 1 && value.time_meta_length == 0)) {
            return fail(
                DatasetIndexStatus::BAD_REFERENCE,
                "DeviceFileSpan contains an invalid reference or layout");
        }
        if (value.layout == 1 &&
            (add_overflows(value.time_meta_offset, value.time_meta_length) ||
             value.time_meta_offset + value.time_meta_length >
                 file_records[value.file_id].file_size)) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "aligned time metadata range is outside its TsFile");
        }
    }
    for (uint32_t i = 0; i < series_spans.count; ++i) {
        const SeriesFileSpanRecord& value = series_span_records[i];
        if (value.series_id >= series.count || value.file_id >= files.count ||
            value.locator_id >= locators.count || value.reserved != 0 ||
            value.min_time > value.max_time ||
            value.prefix_max_time < value.max_time) {
            return fail(
                DatasetIndexStatus::BAD_REFERENCE,
                "SeriesFileSpan contains an invalid reference or range");
        }
    }
    for (uint32_t i = 0; i < locators.count; ++i) {
        const SeriesLocatorRecord& value = locator_records[i];
        if (value.device_file_span_id >= device_spans.count ||
            value.locator_kind > 1 ||
            (value.flags & ~static_cast<uint16_t>(1)) != 0 ||
            value.timeseries_meta_length == 0) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "SeriesLocator contains an invalid reference or enum");
        }
        const DeviceFileSpanRecord& device_span =
            device_span_records[value.device_file_span_id];
        if (value.locator_kind != device_span.layout ||
            add_overflows(value.timeseries_meta_offset,
                          value.timeseries_meta_length) ||
            value.timeseries_meta_offset + value.timeseries_meta_length >
                file_records[device_span.file_id].file_size) {
            return fail(DatasetIndexStatus::BAD_REFERENCE,
                        "SeriesLocator metadata range is outside its TsFile");
        }
    }
    error_message_.clear();
    return DatasetIndexStatus::OK;
}

DatasetIndexStatus MappedDatasetIndex::find_table_ids(
    const std::string& name, std::vector<uint32_t>& table_ids) const {
    table_ids.clear();
    DatasetIndexSectionView view;
    if (section(DatasetIndexSectionType::TABLE_NAME_INDEX, view) !=
        DatasetIndexStatus::OK) {
        return DatasetIndexStatus::BAD_SECTION;
    }
    const TableNameIndexRecord* records =
        reinterpret_cast<const TableNameIndexRecord*>(view.data);
    const uint64_t hash = dataset_index_name_hash(name.data(), name.size());
    uint32_t low = 0;
    uint32_t high = view.count;
    while (low < high) {
        const uint32_t middle = low + (high - low) / 2;
        if (records[middle].name_hash < hash) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    for (uint32_t i = low; i < view.count && records[i].name_hash == hash;
         ++i) {
        if (string_equals(*this, records[i].name_sid, name)) {
            table_ids.push_back(records[i].table_id);
        }
    }
    return table_ids.empty() ? DatasetIndexStatus::NOT_FOUND
                             : DatasetIndexStatus::OK;
}

DatasetIndexStatus MappedDatasetIndex::find_device_id(
    uint32_t table_id, const std::string& name, uint32_t& device_id) const {
    const TableRecord* table = nullptr;
    if (record(DatasetIndexSectionType::TABLE_RECORD, table_id, table) !=
        DatasetIndexStatus::OK) {
        return DatasetIndexStatus::NOT_FOUND;
    }
    DatasetIndexSectionView view;
    section(DatasetIndexSectionType::DEVICE_NAME_INDEX, view);
    const DeviceNameIndexRecord* records =
        reinterpret_cast<const DeviceNameIndexRecord*>(view.data);
    const uint64_t hash = dataset_index_name_hash(name.data(), name.size());
    uint32_t low = table->first_device_name_index;
    uint32_t high = low + table->device_count;
    while (low < high) {
        const uint32_t middle = low + (high - low) / 2;
        if (records[middle].name_hash < hash) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    const uint32_t end = table->first_device_name_index + table->device_count;
    for (uint32_t i = low; i < end && records[i].name_hash == hash; ++i) {
        if (records[i].table_id == table_id &&
            string_equals(*this, records[i].name_sid, name)) {
            device_id = records[i].device_id;
            return DatasetIndexStatus::OK;
        }
    }
    return DatasetIndexStatus::NOT_FOUND;
}

DatasetIndexStatus MappedDatasetIndex::find_column_id(
    uint32_t table_id, const std::string& name, uint32_t& column_id) const {
    const TableRecord* table = nullptr;
    if (record(DatasetIndexSectionType::TABLE_RECORD, table_id, table) !=
        DatasetIndexStatus::OK) {
        return DatasetIndexStatus::NOT_FOUND;
    }
    DatasetIndexSectionView view;
    section(DatasetIndexSectionType::COLUMN_NAME_INDEX, view);
    const ColumnNameIndexRecord* records =
        reinterpret_cast<const ColumnNameIndexRecord*>(view.data);
    const uint64_t hash = dataset_index_name_hash(name.data(), name.size());
    uint32_t low = table->first_column_name_index;
    uint32_t high = low + table->column_count;
    while (low < high) {
        const uint32_t middle = low + (high - low) / 2;
        if (records[middle].name_hash < hash) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    const uint32_t end = table->first_column_name_index + table->column_count;
    for (uint32_t i = low; i < end && records[i].name_hash == hash; ++i) {
        if (records[i].table_id == table_id &&
            string_equals(*this, records[i].name_sid, name)) {
            column_id = records[i].column_id;
            return DatasetIndexStatus::OK;
        }
    }
    return DatasetIndexStatus::NOT_FOUND;
}

DatasetIndexStatus MappedDatasetIndex::find_series_id(
    uint32_t device_id, uint32_t column_id, uint32_t& series_id) const {
    const DeviceRecord* device = nullptr;
    if (record(DatasetIndexSectionType::DEVICE_RECORD, device_id, device) !=
        DatasetIndexStatus::OK) {
        return DatasetIndexStatus::NOT_FOUND;
    }
    DatasetIndexSectionView view;
    section(DatasetIndexSectionType::LOGICAL_SERIES, view);
    const LogicalSeriesRecord* records =
        reinterpret_cast<const LogicalSeriesRecord*>(view.data);
    uint32_t low = device->first_series_id;
    uint32_t high = low + device->series_count;
    while (low < high) {
        const uint32_t middle = low + (high - low) / 2;
        if (records[middle].column_id < column_id) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    if (low < device->first_series_id + device->series_count &&
        records[low].device_id == device_id &&
        records[low].column_id == column_id) {
        series_id = low;
        return DatasetIndexStatus::OK;
    }
    return DatasetIndexStatus::NOT_FOUND;
}

}  // namespace dataset
}  // namespace storage
