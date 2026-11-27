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
#include "reader/arrow/scan_page.h"

#include <algorithm>
#include <climits>
#include <cstring>

#include "common/container/bit_map.h"
#include "compress/compressor_factory.h"
#include "encoding/decoder_factory.h"

namespace storage {
using namespace common;
int ArrowInputRange::read(RandomAccessReadFile& file,
                          const ArrowInputBudget& usage, int64_t offset,
                          size_t length,
                          std::shared_ptr<ArrowInputRange>& input) {
    if (!usage || !length || length > MAX_QUERY_BYTES) return E_OK;
    size_t active = usage->load();
    while (active <= MAX_QUERY_BYTES - length) {
        if (!usage->compare_exchange_weak(active, active + length)) continue;
        std::shared_ptr<ArrowInputRange> buffer;
        try {
            buffer = std::make_shared<ArrowInputRange>(usage, offset, length);
        } catch (const std::bad_alloc&) {
            usage->fetch_sub(length);
            // Optional prefetch may fall back to the ordinary page reader.
            return E_OK;
        }
        int32_t actual = 0;
        int ret = file.read(offset, buffer->bytes.get(),
                            static_cast<int32_t>(length), actual);
        if (ret != E_OK) return ret;
        if (actual != static_cast<int32_t>(length)) return E_FILE_READ_ERR;
        input = std::move(buffer);
        break;
    }
    return E_OK;
}
int ArrowReadWindow::read(RandomAccessReadFile& file, int64_t offset,
                          char* destination, int32_t size, int64_t end) {
    if (offset < 0 || size < 0 || end < offset || end > file.file_size() ||
        size > end - offset)
        return E_TSFILE_CORRUPTED;
    if (size == 0) return E_OK;
    if (offset >= offset_ && uint64_t(offset - offset_) <= bytes_.size() &&
        uint64_t(size) <= bytes_.size() - (offset - offset_)) {
        std::memcpy(destination, bytes_.data() + offset - offset_, size);
        return E_OK;
    }
    int32_t actual = 0;
    if (size > 64 * 1024) {
        int ret = file.read(offset, destination, size, actual);
        return ret != E_OK ? ret : actual == size ? E_OK : E_FILE_READ_ERR;
    }
    offset_ = offset;
    bytes_.resize(std::min<int64_t>(64 * 1024, end - offset));
    int ret = file.read(offset, bytes_.data(), bytes_.size(), actual);
    if (ret != E_OK || actual != int64_t(bytes_.size())) {
        bytes_.clear();
        return ret != E_OK ? ret : E_FILE_READ_ERR;
    }
    std::memcpy(destination, bytes_.data(), size);
    return E_OK;
}
namespace {
struct DecoderDelete {
    void operator()(Decoder* p) const { DecoderFactory::free(p); }
};
struct CompressorDelete {
    void operator()(Compressor* p) const { CompressorFactory::free(p); }
};
using DecoderPtr = std::unique_ptr<Decoder, DecoderDelete>;
struct PageBytes {
    std::unique_ptr<Compressor, CompressorDelete> compressor;
    std::vector<char> compressed;
    char* data = nullptr;
    uint32_t size = 0;
    ~PageBytes() {
        if (data) compressor->after_uncompress(data);
    }
    int read(RandomAccessReadFile& file, const ArrowPageLocation& page,
             ArrowReadWindow* window = nullptr, int64_t read_end = 0) {
        if (page.compressed_size == 0 || page.compressed_size > INT32_MAX)
            return E_TSFILE_CORRUPTED;
        if (page.compression < UNCOMPRESSED || page.compression > LZMA2)
            return E_TSFILE_CORRUPTED;
        compressor.reset(CompressorFactory::alloc_compressor(page.compression));
        if (!compressor) return E_NOT_SUPPORT;
        int ret = compressor->reset(false);
        if (ret != E_OK) return ret;
        if (page.input) {
            const auto& block = *page.input;
            const int64_t rel = page.offset - block.offset;
            if (rel < 0 || uint64_t(rel) + page.compressed_size > block.length)
                return E_TSFILE_CORRUPTED;
            ret = compressor->uncompress(page.input->bytes.get() + rel,
                                         page.compressed_size, data, size);
            if (ret != E_OK) return ret;
            return size == page.uncompressed_size ? E_OK : E_TSFILE_CORRUPTED;
        }
        compressed.resize(page.compressed_size);
        int32_t actual = 0;
        if (window) {
            ret = window->read(file, page.offset, compressed.data(),
                               page.compressed_size, read_end);
            actual = page.compressed_size;
        } else {
            ret = file.read(page.offset, compressed.data(),
                            page.compressed_size, actual);
        }
        if (ret != E_OK) return ret;
        if (actual != int32_t(page.compressed_size)) return E_FILE_READ_ERR;
        ret = compressor->uncompress(compressed.data(), page.compressed_size,
                                     data, size);
        if (ret != E_OK) return ret;
        return size == page.uncompressed_size ? E_OK : E_TSFILE_CORRUPTED;
    }
};
template <typename Parse>
int read_header(RandomAccessReadFile& file, int64_t offset, int64_t end,
                Parse parse, uint32_t& consumed,
                ArrowReadWindow* window = nullptr) {
    if (offset < 0 || offset >= end || end > file.file_size())
        return E_TSFILE_CORRUPTED;
    uint64_t amount = std::min<int64_t>(256, end - offset);
    for (;;) {
        std::vector<char> bytes(amount);
        int ret;
        if (window) {
            ret = window->read(file, offset, bytes.data(), amount, end);
        } else {
            int32_t actual = 0;
            ret = file.read(offset, bytes.data(), amount, actual);
            if (ret == E_OK && actual != int64_t(amount)) ret = E_FILE_READ_ERR;
        }
        if (ret != E_OK) return ret;
        ByteStream in;
        in.wrap_from(bytes.data(), bytes.size());
        ret = parse(in);
        if (ret == E_OK) {
            consumed = in.read_pos();
            return E_OK;
        }
        if (!deserialize_buf_not_enough(ret) ||
            amount == uint64_t(end - offset) || amount >= INT32_MAX)
            return ret;
        amount = std::min<uint64_t>(std::min<uint64_t>(amount * 2, INT32_MAX),
                                    end - offset);
    }
}
int split_non_aligned(PageBytes& bytes, ByteStream& time, ByteStream& value) {
    uint32_t time_size = 0;
    int prefix = 0;
    int ret = SerializationUtil::read_var_uint(time_size, bytes.data,
                                               bytes.size, &prefix);
    if (ret != E_OK) return ret;
    if (uint64_t(prefix) + time_size > bytes.size) return E_TSFILE_CORRUPTED;
    time.wrap_from(bytes.data + prefix, time_size);
    value.wrap_from(bytes.data + prefix + time_size,
                    bytes.size - prefix - time_size);
    return E_OK;
}
int read_numeric(Decoder& decoder, TSDataType type, char* data, uint32_t count,
                 ByteStream& in) {
    switch (type) {
        case INT32:
        case DATE:
            return decoder.read_exact_int32(reinterpret_cast<int32_t*>(data),
                                            count, in);
        case INT64:
        case TIMESTAMP:
            return decoder.read_exact_int64(reinterpret_cast<int64_t*>(data),
                                            count, in);
        case FLOAT:
            return decoder.read_exact_float(reinterpret_cast<float*>(data),
                                            count, in);
        case DOUBLE:
            return decoder.read_exact_double(reinterpret_cast<double*>(data),
                                             count, in);
        default:
            return E_NOT_SUPPORT;
    }
}
}  // namespace
int plan_arrow_pages(RandomAccessReadFile& file, ChunkMeta& chunk,
                     Statistic* single_chunk_stat,
                     std::vector<ArrowPageLocation>& pages,
                     const ArrowInputBudget& usage,
                     const std::shared_ptr<ArrowInputRange>& device_input) {
    ChunkHeader header;
    uint32_t used = 0;
    int ret = E_OK;
    int64_t start = chunk.offset_of_chunk_header_;
    bool mapped = device_input && start >= device_input->offset &&
                  uint64_t(start - device_input->offset) < device_input->length;
    if (mapped) {
        int64_t rel = start - device_input->offset;
        ByteStream in;
        in.wrap_from(device_input->bytes.get() + rel,
                     device_input->length - rel);
        ret = header.deserialize_from(in);
        used = in.read_pos();
    } else
        ret = read_header(
            file, chunk.offset_of_chunk_header_, file.file_size(),
            [&header](ByteStream& in) {
                header = ChunkHeader();
                return header.deserialize_from(in);
            },
            used);
    if (ret != E_OK) return ret;
    uint8_t marker = static_cast<uint8_t>(header.chunk_type_) & 0x3f;
    if (marker != CHUNK_HEADER_MARKER &&
        marker != ONLY_ONE_PAGE_CHUNK_HEADER_MARKER)
        return E_TSFILE_CORRUPTED;
    if (header.encoding_type_ < PLAIN || header.encoding_type_ > ALP)
        return E_TSFILE_CORRUPTED;
    int64_t offset = chunk.offset_of_chunk_header_ + used;
    if (header.data_size_ > uint64_t(file.file_size() - offset))
        return E_TSFILE_CORRUPTED;
    int64_t end = offset + header.data_size_;
    if (end > file.file_size() || end < offset) return E_TSFILE_CORRUPTED;
    bool single = marker == ONLY_ONE_PAGE_CHUNK_HEADER_MARKER;
    ArrowReadWindow window;
    std::shared_ptr<ArrowInputRange> input;
    const size_t amount = end - offset;
    if (mapped &&
        uint64_t(end - device_input->offset) <= device_input->length) {
        input = device_input;
    } else if (usage && amount > 0 && amount <= 256 * 1024) {
        ret = ArrowInputRange::read(file, usage, offset, amount, input);
        if (ret != E_OK) return ret;
    }
    while (offset < end) {
        const int64_t header_offset = offset;
        PageHeader ph;
        if (input) {
            ByteStream in;
            in.wrap_from(input->bytes.get() + (offset - input->offset),
                         end - offset);
            ret = ph.deserialize_from(in, !single, header.data_type_);
            used = in.read_pos();
        } else
            ret = read_header(
                file, offset, end,
                [&ph, &header, single](ByteStream& in) {
                    ph.reset();
                    return ph.deserialize_from(in, !single, header.data_type_);
                },
                used, &window);
        if (ret != E_OK) return ret;
        offset += used;
        if (uint64_t(ph.compressed_size_) > uint64_t(end - offset))
            return E_TSFILE_CORRUPTED;
        ArrowPageLocation page;
        page.chunk_offset = chunk.offset_of_chunk_header_;
        page.header_offset = header_offset;
        page.input = input;
        page.offset = offset;
        page.compressed_size = ph.compressed_size_;
        page.uncompressed_size = ph.uncompressed_size_;
        page.type = header.data_type_;
        page.encoding = header.encoding_type_;
        page.compression = header.compression_type_;
        Statistic* stat = ph.statistic_;
        ph.statistic_ = nullptr;
        if (!stat && single && page.compressed_size) {
            Statistic* source =
                chunk.statistic_ ? chunk.statistic_ : single_chunk_stat;
            if (source) {
                stat = StatisticFactory::alloc_statistic(header.data_type_);
                if (!stat) return E_OOM;
                clone_statistic(source, stat, header.data_type_);
            }
        }
        page.statistic.reset(stat, [](Statistic* p) {
            if (p) StatisticFactory::free(p);
        });
        pages.push_back(std::move(page));
        offset += ph.compressed_size_;
        if (single && offset != end) return E_TSFILE_CORRUPTED;
    }
    return E_OK;
}
int decode_arrow_times(RandomAccessReadFile& file,
                       const ArrowPageLocation& page, bool aligned,
                       std::vector<int64_t>& times) {
    times.clear();
    PageBytes bytes;
    int ret = bytes.read(file, page);
    if (ret != E_OK) return ret;
    ByteStream in, values;
    if (aligned)
        in.wrap_from(bytes.data, bytes.size);
    else if ((ret = split_non_aligned(bytes, in, values)) != E_OK)
        return ret;
    DecoderPtr decoder(
        aligned ? DecoderFactory::alloc_value_decoder(page.encoding, INT64)
                : DecoderFactory::alloc_time_decoder());
    if (!decoder) return E_NOT_SUPPORT;
    int64_t buffer[1024];
    while (decoder->has_remaining(in)) {
        int actual = 0;
        ret = decoder->read_batch_int64(buffer, 1024, actual, in);
        if (ret != E_OK) return ret;
        if (actual <= 0 || actual > 1024) return E_TSFILE_CORRUPTED;
        times.insert(times.end(), buffer, buffer + actual);
        if (times.size() > INT32_MAX) return E_OVERFLOW;
    }
    return E_OK;
}
struct ArrowValueCursor::Impl {
    RandomAccessReadFile& file;
    ArrowPageLocation page;
    bool aligned;
    uint32_t page_rows;
    uint32_t source = 0;
    bool initialized = false;
    PageBytes bytes;
    ByteStream in;
    const uint8_t* bitmap = nullptr;
    DecoderPtr decoder;
    PageArena strings;
    Impl(RandomAccessReadFile& f, ArrowPageLocation p, bool a, uint32_t rows)
        : file(f), page(std::move(p)), aligned(a), page_rows(rows) {
        strings.init(512, MOD_TSFILE_READER);
    }
    int initialize(ArrowReadWindow* window, int64_t read_end) {
        int ret = bytes.read(file, page, window, read_end);
        if (ret != E_OK) return ret;
        ByteStream times;
        if (aligned) {
            if (bytes.size < 4) return E_TSFILE_CORRUPTED;
            uint32_t size = SerializationUtil::read_ui32(bytes.data);
            uint64_t bitmap_size = (uint64_t(size) + 7) / 8;
            if (size != page_rows || 4 + bitmap_size > bytes.size)
                return E_TSFILE_CORRUPTED;
            bitmap = reinterpret_cast<const uint8_t*>(bytes.data + 4);
            in.wrap_from(bytes.data + 4 + bitmap_size,
                         bytes.size - 4 - bitmap_size);
        } else if ((ret = split_non_aligned(bytes, times, in)) != E_OK)
            return ret;
        decoder.reset(
            DecoderFactory::alloc_value_decoder(page.encoding, page.type));
        if (!decoder) return E_NOT_SUPPORT;
        initialized = true;
        return E_OK;
    }
    int write(const std::vector<uint32_t>& rows, uint32_t begin, uint32_t count,
              ArrowColumn& output, uint32_t destination,
              ArrowReadWindow* window, int64_t read_end) {
        if (uint64_t(begin) + count > rows.size() ||
            uint64_t(destination) + count > output.capacity())
            return E_INVALID_ARG;
        if (page.compressed_size == 0 || count == 0) return E_OK;
        int ret = E_OK;
        if (!initialized && (ret = initialize(window, read_end)) != E_OK)
            return ret;
        auto valid = [this](uint32_t row) {
            return !bitmap || (bitmap[row / 8] & (0x80U >> (row % 8)));
        };
        uint64_t scratch[1024];
        // Selected rows are strictly increasing. Matching endpoints establish a
        // dense slice.
        if (aligned && page.type == DOUBLE && output.type() == DOUBLE &&
            count <= 1024 && !(source % 8) && !(destination % 8) &&
            !(count % 8) && rows[begin] == source &&
            uint64_t(source) + count <= page_rows &&
            rows[begin + count - 1] == source + count - 1) {
            const uint8_t* mask = bitmap + source / 8;
            const uint32_t non_null = bitops::count_valid_bits(mask, count);
            double* dest = reinterpret_cast<double*>(output.mutable_values()) +
                           destination;
            double compact[1024];
            if (non_null == count) {
                ret = decoder->read_exact_double(dest, count, in);
                if (ret != E_OK) return ret;
                output.set_valid_range(destination, count);
            } else {
                if (non_null && (ret = decoder->read_exact_double(
                                     compact, non_null, in)) != E_OK)
                    return ret;
                auto* valid_bits = output.mutable_validity() + destination / 8;
                unsigned j = 0, index = 0;
                while (j < count / 8) {
                    unsigned start = j;
                    while (j < count / 8 && mask[j] == 0xff) ++j;
                    if (j > start) {
                        const unsigned len = (j - start) * 8;
                        std::memcpy(dest + start * 8, compact + index,
                                    len * sizeof(double));
                        std::memset(valid_bits + start, 0xff, j - start);
                        index += len;
                    }
                    if (j == count / 8) break;
                    const uint8_t byte = mask[j];
                    valid_bits[j] = bitops::reverse_byte(byte);
                    for (unsigned bit = 0; bit < 8; ++bit)
                        if (byte & (0x80U >> bit))
                            dest[8 * j + bit] = compact[index++];
                    ++j;
                }
            }
            source += count;
            return E_OK;
        }
        uint32_t selected = 0;
        for (; source < page_rows && selected < count;) {
            bool keep = selected < count && rows[begin + selected] == source;
            if (selected < count && rows[begin + selected] < source)
                return E_INVALID_ARG;
            if (!valid(source)) {
                if (keep) {
                    ret = output.set_native(destination + selected, page.type,
                                            nullptr, 0);
                    if (ret != E_OK) return ret;
                    ++selected;
                }
                ++source;
                continue;
            }
            bool numeric = page.type == INT32 || page.type == INT64 ||
                           page.type == FLOAT || page.type == DOUBLE ||
                           page.type == TIMESTAMP || page.type == DATE;
            uint32_t run = 1;
            if (numeric) {
                while (source + run < page_rows && run < 1024 &&
                       valid(source + run)) {
                    bool next_keep =
                        keep ? selected + run < count &&
                                   rows[begin + selected + run] == source + run
                             : !(selected < count &&
                                 rows[begin + selected] == source + run);
                    if (!next_keep) break;
                    ++run;
                }
                bool direct =
                    keep && page.type == output.type() && page.type != DATE;
                char* dest = direct ? output.mutable_values() +
                                          uint64_t(destination + selected) *
                                              get_data_type_size(page.type)
                                    : reinterpret_cast<char*>(scratch);
                ret = read_numeric(*decoder, page.type, dest, run, in);
                if (ret != E_OK) return ret;
                if (keep) {
                    if (direct)
                        output.set_valid_range(destination + selected, run);
                    else
                        for (uint32_t k = 0; k < run; ++k) {
                            uint32_t width = get_data_type_size(page.type);
                            ret = output.set_native(destination + selected + k,
                                                    page.type, dest + k * width,
                                                    width);
                            if (ret != E_OK) return ret;
                        }
                    selected += run;
                }
            } else if (page.type == BOOLEAN) {
                bool value;
                ret = decoder->read_boolean(value, in);
                if (ret != E_OK) return ret;
                if (keep) {
                    ret = output.set_native(destination + selected++, page.type,
                                            reinterpret_cast<char*>(&value),
                                            sizeof(value));
                    if (ret != E_OK) return ret;
                }
            } else if (page.type == STRING || page.type == TEXT ||
                       page.type == BLOB) {
                String value;
                ret = decoder->read_String(value, strings, in);
                if (ret != E_OK) return ret;
                if (keep) {
                    ret = output.set_native(destination + selected++, page.type,
                                            value.len_ ? value.buf_ : "",
                                            value.len_);
                    if (ret != E_OK) return ret;
                }
                strings.reset();
            } else
                return E_NOT_SUPPORT;
            source += run;
        }
        return selected == count ? E_OK : E_TSFILE_CORRUPTED;
    }
};
ArrowValueCursor::ArrowValueCursor(RandomAccessReadFile& file,
                                   ArrowPageLocation page, bool aligned,
                                   uint32_t page_rows)
    : impl_(new Impl(file, std::move(page), aligned, page_rows)) {}
ArrowValueCursor::~ArrowValueCursor() = default;
int ArrowValueCursor::write(const std::vector<uint32_t>& rows, uint32_t begin,
                            uint32_t count, ArrowColumn& output,
                            uint32_t destination, ArrowReadWindow* window,
                            int64_t read_end) {
    return impl_->write(rows, begin, count, output, destination, window,
                        read_end);
}
int decode_arrow_values(RandomAccessReadFile& file,
                        const ArrowPageLocation& page, bool aligned,
                        uint32_t page_rows, const std::vector<uint32_t>& rows,
                        uint32_t begin, uint32_t count, ArrowColumn& output,
                        uint32_t destination) {
    ArrowValueCursor cursor(file, page, aligned, page_rows);
    return cursor.write(rows, begin, count, output, destination);
}
}  // namespace storage
