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

#include "value_page_writer.h"

#include "common/config/config.h"
#include "common/logger/elog.h"
#include "compress/compressor_factory.h"
#include "encoding/encoder_factory.h"

using namespace common;

namespace storage {

uint32_t ValuePageWriter::MASK = 1 << 7;

int ValuePageData::init(ByteStream &col_notnull_bitmap_bs, ByteStream &value_bs,
                        Compressor *compressor, uint32_t size) {
    int ret = E_OK;
    col_notnull_bitmap_buf_size_ = col_notnull_bitmap_bs.total_size();
    value_buf_size_ = value_bs.total_size();
    uncompressed_size_ =
        sizeof(size) + col_notnull_bitmap_buf_size_ + value_buf_size_;
    uncompressed_buf_ =
        (char *)mem_alloc(uncompressed_size_, MOD_PAGE_WRITER_OUTPUT_STREAM);
    compressor_ = compressor;
    if (IS_NULL(uncompressed_buf_)) {
        return E_OOM;
    }
    if (col_notnull_bitmap_buf_size_ == 0 || value_buf_size_ == 0) {
        return E_INVALID_ARG;
    }
    uncompressed_buf_[0] = (unsigned char)((size >> 24) & 0xFF);
    uncompressed_buf_[1] = (unsigned char)((size >> 16) & 0xFF);
    uncompressed_buf_[2] = (unsigned char)((size >> 8) & 0xFF);
    uncompressed_buf_[3] = (unsigned char)((size)&0xFF);

    if (RET_FAIL(common::copy_bs_to_buf(col_notnull_bitmap_bs,
                                        uncompressed_buf_ + sizeof(size),
                                        col_notnull_bitmap_buf_size_))) {
    } else if (RET_FAIL(common::copy_bs_to_buf(value_bs,
                                               uncompressed_buf_ +
                                                   sizeof(size) +
                                                   col_notnull_bitmap_buf_size_,
                                               value_buf_size_))) {
    } else {
        // TODO
        // NOTE: different compressor may have different compress API
        // Be carefull about the memory.
        if (RET_FAIL(compressor->reset(true))) {
        } else if (RET_FAIL(compressor->compress(
                       uncompressed_buf_, uncompressed_size_, compressed_buf_,
                       compressed_size_))) {
        }
    }
#if DEBUG_SE
    std::cout << "ValuePageData::init. col_notnull_bitmap_buf_size="
              << col_notnull_bitmap_buf_size_ << ", size_=" << size
              << ", value_buf_size=" << value_buf_size_
              << ", uncompressed_size=" << uncompressed_size_
              << ", compressed_size=" << compressed_size_ << std::endl;
    DEBUG_hex_dump_buf("uncompressed_buf=", uncompressed_buf_,
                       uncompressed_size_);
#endif
    return ret;
}

int ValuePageWriter::init(TSDataType data_type, TSEncoding encoding,
                          CompressionType compression) {
    int ret = E_OK;
    data_type_ = data_type;
    if (nullptr == (value_encoder_ = EncoderFactory::alloc_value_encoder(
                        encoding, data_type))) {
        ret = E_OOM;
    } else if (nullptr ==
               (statistic_ = StatisticFactory::alloc_statistic(data_type))) {
        ret = E_OOM;
    } else if (nullptr == (compressor_ = CompressorFactory::alloc_compressor(
                               compression))) {
        ret = E_OOM;
    }
    if (ret != E_OK) {
        if (value_encoder_ != nullptr) {
            EncoderFactory::free(value_encoder_);
        }
        if (statistic_ != nullptr) {
            StatisticFactory::free(statistic_);
        }
    }
    if (ret == E_OK) {
        is_inited_ = true;
    }
    return ret;
}

void ValuePageWriter::reset() {
    value_encoder_->reset();
    statistic_->reset();
    col_notnull_bitmap_out_stream_.reset();
    value_out_stream_.reset();
}

void ValuePageWriter::destroy() {
    if (is_inited_) {
        is_inited_ = false;
        value_encoder_->destroy();
        statistic_->destroy();

        EncoderFactory::free(value_encoder_);
        StatisticFactory::free(statistic_);
        CompressorFactory::free(compressor_);
    }
}

int ValuePageWriter::write_to_chunk(ByteStream &pages_data, bool write_header,
                                    bool write_statistic,
                                    bool write_data_to_chunk_data) {
#if DEBUG_SE
    std::cout << "ValuePageWriter::write_to_chunk at position "
              << pages_data.total_size() << " of chunk_data." << std::endl;
#endif
    int ret = E_OK;
    if (RET_FAIL(prepare_end_page())) {
        return ret;
    }
    if (RET_FAIL(cur_page_data_.init(col_notnull_bitmap_out_stream_,
                                     value_out_stream_, compressor_, size_))) {
    }
    col_notnull_bitmap_.clear();
    size_ = 0;
    col_notnull_bitmap_out_stream_.reset();

    if (IS_SUCC(ret) && write_header) {
        if (RET_FAIL(SerializationUtil::write_var_uint(
                cur_page_data_.uncompressed_size_, pages_data))) {
        } else if (RET_FAIL(SerializationUtil::write_var_uint(
                       cur_page_data_.compressed_size_, pages_data))) {
        }
    }
    // std::cout << "ValuePageWriter::write_to_chunk after write_header. pos="
    // << pages_data.total_size() << std::endl;
    if (IS_SUCC(ret) && write_statistic) {
        if (RET_FAIL(statistic_->serialize_to(pages_data))) {
        }
    }
    // std::cout << "ValuePageWriter::write_to_chunk after write_stat. pos=" <<
    // pages_data.total_size() << std::endl; DEBUG_print_byte_stream("In
    // ValuePageWriter::write_to_chunk, before writer page data: pages_data = ",
    // pages_data);
    if (IS_SUCC(ret) && write_data_to_chunk_data) {
        // DEBUG_hex_dump_buf("cur_page_data_.compressed_buf_ = ",
        // cur_page_data_.compressed_buf_, cur_page_data_.compressed_size_);
        if (RET_FAIL(pages_data.write_buf(cur_page_data_.compressed_buf_,
                                          cur_page_data_.compressed_size_))) {
        }
    }
    // DEBUG_print_byte_stream("In ValuePageWriter::write_to_chunk, after writer
    // page data: pages_data = ", pages_data); std::cout <<
    // "ValuePageWriter::write_to_chunk after write_data. pos=" <<
    // pages_data.total_size() << std::endl;
#if DEBUG_SE
    std::cout << "ValuePageWriter write_to_chunk: ret=" << ret
              << ", flags(HSD)=" << write_header << write_statistic
              << write_data_to_chunk_data
              << ", uncompressed_size=" << cur_page_data_.uncompressed_size_
              << ", compressed_size=" << cur_page_data_.compressed_size_
              << ", write_header=" << write_header
              << ", statistic_=" << statistic_->to_string() << std::endl;
#endif
    return ret;
}

}  // end namespace storage
