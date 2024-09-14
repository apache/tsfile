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

#include "chunk_writer.h"

#include "common/logger/elog.h"

using namespace common;

namespace storage {

int ChunkWriter::init(const ColumnDesc &col_desc) {
    return init(col_desc.column_name_, col_desc.type_, col_desc.encoding_,
                col_desc.compression_);
}

int ChunkWriter::init(const std::string &measurement_name, TSDataType data_type,
                      TSEncoding encoding, CompressionType compression_type) {
    int ret = E_OK;
    chunk_statistic_ = StatisticFactory::alloc_statistic(data_type);
    if (chunk_statistic_ == nullptr) {
        return E_OOM;
    } else if (RET_FAIL(
                   page_writer_.init(data_type, encoding, compression_type))) {
    } else if (IS_NULL((first_page_statistic_ =
                            StatisticFactory::alloc_statistic(data_type)))) {
        ret = E_OOM;
    } else {
        data_type_ = data_type;
        chunk_header_.measurement_name_ = measurement_name;
        chunk_header_.data_type_ = data_type;
        chunk_header_.compression_type_ = compression_type;
        chunk_header_.encoding_type_ = encoding;
    }
    return ret;
}

void ChunkWriter::destroy() {
    if (num_of_pages_ == 1) {
        free_first_writer_data();
    }
    page_writer_.destroy();
    if (chunk_statistic_ != nullptr) {
        StatisticFactory::free(chunk_statistic_);
        chunk_statistic_ = nullptr;
    }
    if (first_page_statistic_ != nullptr) {
        StatisticFactory::free(first_page_statistic_);
        first_page_statistic_ = nullptr;
    }
    chunk_data_.destroy();
    chunk_header_.reset();
    num_of_pages_ = 0;
}

int ChunkWriter::seal_cur_page(bool end_chunk) {
    int ret = E_OK;
    if (RET_FAIL(chunk_statistic_->merge_with(page_writer_.get_statistic()))) {
        return ret;
    }
    if (num_of_pages_ == 0) {
        if (end_chunk) {
            // this page is the only one page of this chunk
            ret = page_writer_.write_to_chunk(chunk_data_, /*header*/ true,
                                              /*stat*/ false, /*data*/ true);
            page_writer_.destroy_page_data();
            page_writer_.destroy();
        } else {
            /*
             * if the chunk has only one page, do not writer page statistic.
             * so we save the data of first page and see if the chunk has more
             * page later.
             */
            ret = page_writer_.write_to_chunk(chunk_data_, /*header*/ true,
                                              /*stat*/ false, /*data*/ false);
            if (IS_SUCC(ret)) {
                save_first_page_data(page_writer_);
                // page_writer_.destroy_page_data();
                page_writer_.reset();
            }
        }
    } else {
        if (num_of_pages_ == 1) {
            // the chunk has more than one page, writer first page now
            if (RET_FAIL(write_first_page_data(chunk_data_))) {
            }
            free_first_writer_data();
        }
        if (IS_SUCC(ret)) {
            if (RET_FAIL(page_writer_.write_to_chunk(
                    chunk_data_, /*header*/ true, /*stat*/ true,
                    /*data*/ true))) {
            }
            page_writer_.destroy_page_data();
            page_writer_.reset();
        }
    }
    num_of_pages_++;
#if DEBUG_SE
    std::cout << "seal_cur_page, num_of_pages_=" << num_of_pages_
              << ", end_chunk=" << end_chunk
              << ". After seal: chunk.get_point_numer()="
              << chunk_statistic_->count_
              << ", chunk_data.size=" << chunk_data_.total_size() << std::endl;
#endif
    return ret;
}

void ChunkWriter::save_first_page_data(PageWriter &first_page_writer) {
    first_page_data_ = first_page_writer.get_cur_page_data();
    first_page_statistic_->deep_copy_from(first_page_writer.get_statistic());
}

int ChunkWriter::write_first_page_data(ByteStream &pages_data) {
    int ret = E_OK;
    if (RET_FAIL(first_page_statistic_->serialize_to(pages_data))) {
    } else if (RET_FAIL(
                   pages_data.write_buf(first_page_data_.compressed_buf_,
                                        first_page_data_.compressed_size_))) {
    }
    return ret;
}

int ChunkWriter::end_encode_chunk() {
    int ret = E_OK;
    if (page_writer_.get_statistic()->count_ > 0) {
        ret = seal_cur_page(/*end_chunk*/ true);
        if (E_OK == ret) {
            chunk_header_.data_size_ = chunk_data_.total_size();
            chunk_header_.num_of_pages_ = num_of_pages_;
        }
    }
#if DEBUG_SE
    std::cout << "end_encode_chunk: num_of_pages_=" << num_of_pages_
              << ", chunk_header_.data_size_=" << chunk_header_.data_size_
              << ", page_writer.get_statistic()->count_="
              << page_writer_.get_statistic()->count_ << std::endl;
#endif
    return ret;
}

int64_t ChunkWriter::estimate_max_series_mem_size() {
    return chunk_data_.total_size() + page_writer_.estimate_max_mem_size() +
           PageHeader::estimat_max_page_header_size_without_statistics() +
           get_typed_statistic_sizeof(page_writer_.get_statistic()->get_type());
}

}  // end namespace storage
