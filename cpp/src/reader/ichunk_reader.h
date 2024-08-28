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

#ifndef READER_ICHUNK_READER_H
#define READER_ICHUNK_READER_H

#include "common/allocator/my_string.h"
#include "common/tsfile_common.h"
#include "compress/compressor.h"
#include "encoding/decoder.h"
#include "file/read_file.h"
#include "reader/filter/filter.h"

namespace storage {

class IChunkReader {
   public:
    IChunkReader() {}
    virtual int init(ReadFile *read_file, common::String m_name,
                     common::TSDataType data_type, Filter *time_filter) {
        return common::E_OK;
    }
    virtual void reset() {}
    virtual void destroy() {}

    virtual bool has_more_data() const { return false; }

    virtual int load_by_meta(ChunkMeta *meta) { return common::E_INVALID_ARG; }
    virtual int load_by_aligned_meta(ChunkMeta *time_meta,
                                     ChunkMeta *value_meta) {
        return common::E_INVALID_ARG;
    }

    virtual int get_next_page(common::TsBlock *tsblock,
                              Filter *oneshoot_filter) {
        return common::E_OK;
    }

    virtual ChunkHeader &get_chunk_header() { return chunk_header_; }

   protected:
    ChunkHeader chunk_header_;
};

}  // end namespace storage
#endif  // READER_ICHUNK_READER_H
