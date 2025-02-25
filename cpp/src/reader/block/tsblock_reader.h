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
#ifndef READER_BLOCK_TSBLOCK_READER_H
#define READER_BLOCK_TSBLOCK_READER_H

#include "common/tsblock/tsblock.h"

namespace storage {
class TsBlockReader {
   public:
    virtual ~TsBlockReader() = default;
    virtual int has_next(bool &has_next) = 0;
    virtual int next(common::TsBlock *&ret_block) = 0;
    virtual void close() = 0;
};

class EmptyTsBlockReader : public TsBlockReader {
   public:
    EmptyTsBlockReader() = default;
    int has_next(bool &has_next) override {
        has_next = false;
        return common::E_OK;
    }

    int next(common::TsBlock *&ret_block) override { return common::E_OK; }
};
}  // namespace storage

#endif  // READER_BLOCK_TSBLOCK_READER_H