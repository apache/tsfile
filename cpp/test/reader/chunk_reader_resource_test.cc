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

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <new>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "reader/aligned_chunk_reader.h"

namespace storage {
namespace {

class TempTsFile {
   public:
    explicit TempTsFile(const char* path) : path_(path) {
        std::remove(path_.c_str());
        std::ofstream out(path_, std::ios::binary | std::ios::trunc);
        out.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
        out.put(VERSION_NUM_BYTE);
        out.write(MAGIC_STRING_TSFILE, MAGIC_STRING_TSFILE_LEN);
    }

    ~TempTsFile() { std::remove(path_.c_str()); }

    const std::string& path() const { return path_; }

   private:
    std::string path_;
};

AlignedChunkReader* allocate_reader(ReadFile* read_file) {
    void* memory =
        common::mem_alloc(sizeof(AlignedChunkReader), common::MOD_CHUNK_READER);
    if (memory == nullptr) {
        return nullptr;
    }
    auto* reader = new (memory) AlignedChunkReader();
    if (reader->init(read_file, common::String("s", 1), common::INT64,
                     nullptr) != common::E_OK) {
        reader->destroy();
        common::mem_free(reader);
        return nullptr;
    }
    return reader;
}

void free_reader(AlignedChunkReader* reader) {
    reader->destroy();
    common::mem_free(reader);
}

TEST(ChunkReaderResourceTest, AlignedInitialShortReadReleasesBuffer) {
    TempTsFile temp_file("aligned_initial_short_read.tsfile");
    ReadFile read_file;
    ASSERT_EQ(read_file.open(temp_file.path()), common::E_OK);
    AlignedChunkReader* reader = allocate_reader(&read_file);
    ASSERT_NE(reader, nullptr);
    ChunkMeta time_meta;
    time_meta.offset_of_chunk_header_ = read_file.file_size();
    ChunkMeta value_meta;
    int64_t memory_before =
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER);

    EXPECT_EQ(reader->load_by_aligned_meta(&time_meta, &value_meta),
              common::E_TSFILE_CORRUPTED);
    EXPECT_EQ(
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER),
        memory_before);

    free_reader(reader);
}

TEST(ChunkReaderResourceTest, MultiAlignedInitialShortReadReleasesBuffer) {
    TempTsFile temp_file("multi_aligned_initial_short_read.tsfile");
    ReadFile read_file;
    ASSERT_EQ(read_file.open(temp_file.path()), common::E_OK);
    AlignedChunkReader* reader = allocate_reader(&read_file);
    ASSERT_NE(reader, nullptr);
    ChunkMeta time_meta;
    time_meta.offset_of_chunk_header_ = read_file.file_size();
    ChunkMeta value_meta;
    std::vector<ChunkMeta*> value_metas{&value_meta};
    int64_t memory_before =
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER);

    EXPECT_EQ(reader->load_by_aligned_meta_multi(&time_meta, value_metas),
              common::E_TSFILE_CORRUPTED);
    EXPECT_EQ(
        common::ModStat::get_instance().get_stat(common::MOD_CHUNK_READER),
        memory_before);

    free_reader(reader);
}

}  // namespace
}  // namespace storage
