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

#ifndef COMPRESS_COMPRESSOR_FACTORY_H
#define COMPRESS_COMPRESSOR_FACTORY_H

#include "common/db_common.h"
#include "uncompressed_compressor.h"

#ifdef ENABLE_SNAPPY
#include "snappy_compressor.h"
#endif

#ifdef ENABLE_GZIP
#include "gzip_compressor.h"
#endif

#ifdef ENABLE_LZOKAY
#include "lzo_compressor.h"
#endif

#ifdef ENABLE_LZ4
#include "lz4_compressor.h"
#endif

#ifdef ENABLE_ZSTD
#include "zstd_compressor.h"
#endif

#ifdef ENABLE_LZMA2
#include "lzma2_compressor.h"
#endif

namespace storage {

#define ALLOC_AND_RETURN_COMPRESSPR(CompressorClass)               \
    do {                                                           \
        void* buf = common::mem_alloc(sizeof(CompressorClass),     \
                                      common::MOD_COMPRESSOR_OBJ); \
        if (buf != nullptr) {                                      \
            CompressorClass* c = new (buf) CompressorClass;        \
            return c;                                              \
        } else {                                                   \
            return nullptr;                                        \
        }                                                          \
    } while (false)

class CompressorFactory {
   public:
    static Compressor* alloc_compressor(common::CompressionType type) {
        if (type == common::UNCOMPRESSED) {
            ALLOC_AND_RETURN_COMPRESSPR(UncompressedCompressor);
        } else if (type == common::SNAPPY) {
#ifdef ENABLE_SNAPPY
            ALLOC_AND_RETURN_COMPRESSPR(SnappyCompressor);
#else
            return nullptr;
#endif
        } else if (type == common::GZIP) {
#ifdef ENABLE_GZIP
            ALLOC_AND_RETURN_COMPRESSPR(GZIPCompressor);
#else
            return nullptr;
#endif
        } else if (type == common::LZO) {
#ifdef ENABLE_LZOKAY
            ALLOC_AND_RETURN_COMPRESSPR(LZOCompressor);
#else
            return nullptr;
#endif
        } else if (type == common::LZ4) {
#ifdef ENABLE_LZ4
            ALLOC_AND_RETURN_COMPRESSPR(LZ4Compressor);
#else
            return nullptr;
#endif
        } else if (type == common::SDT) {
            return nullptr;
        } else if (type == common::PAA) {
            return nullptr;
        } else if (type == common::PLA) {
            return nullptr;
        } else if (type == common::ZSTD) {
#ifdef ENABLE_ZSTD
            ALLOC_AND_RETURN_COMPRESSPR(ZstdCompressor);
#else
            return nullptr;
#endif
        } else if (type == common::LZMA2) {
#ifdef ENABLE_LZMA2
            ALLOC_AND_RETURN_COMPRESSPR(LZMA2Compressor);
#else
            return nullptr;
#endif
        } else {
            ASSERT(false);
            return nullptr;
        }
    }

    // Compressors are placement-new'd into mem_alloc() storage, so freeing
    // that storage alone skips ~Compressor().  Any resource a compressor
    // retains outside its own object (e.g. LZ4's reused decompression
    // buffer) would leak; run the (virtual) destructor first.
    static void free(Compressor* c) {
        if (c == nullptr) {
            return;
        }
        c->~Compressor();
        common::mem_free(c);
    }
};

}  // end namespace storage

#endif  // COMPRESS_COMPRESSOR_FACTORY_H
