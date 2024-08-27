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

#include "gzip_compressor.h"
#include "lz4_compressor.h"
#include "uncompressed_compressor.h"

namespace storage {

#define ALLOC_AND_RETURN_COMPRESSPR(CompressorClass)               \
    do {                                                           \
        void *buf = common::mem_alloc(sizeof(CompressorClass),     \
                                      common::MOD_COMPRESSOR_OBJ); \
        if (buf != nullptr) {                                      \
            CompressorClass *c = new (buf) CompressorClass;        \
            return c;                                              \
        } else {                                                   \
            return nullptr;                                        \
        }                                                          \
    } while (false)

class CompressorFactory {
   public:
    static Compressor *alloc_compressor(common::CompressionType type) {
        if (type == common::UNCOMPRESSED) {
            ALLOC_AND_RETURN_COMPRESSPR(UncompressedCompressor);
        } else if (type == common::SNAPPY) {
            return nullptr;
        } else if (type == common::GZIP) {
            // ALLOC_AND_RETURN_COMPRESSPR(GZIPCompressor);
            return nullptr;
        } else if (type == common::LZO) {
            return nullptr;
        } else if (type == common::SDT) {
            return nullptr;
        } else if (type == common::PAA) {
            return nullptr;
        } else if (type == common::PLA) {
            return nullptr;
        } else if (type == common::LZ4) {
            ALLOC_AND_RETURN_COMPRESSPR(LZ4Compressor);
        } else {
            ASSERT(false);
            return nullptr;
        }
    }

    static void free(Compressor *c) { common::mem_free(c); }
};

}  // end namespace storage

#endif  // COMPRESS_COMPRESSOR_FACTORY_H
