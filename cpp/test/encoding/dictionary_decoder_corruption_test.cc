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

#include "encoding/dictionary_decoder.h"

namespace storage {

class DictionaryDecoderCorruptionTest : public ::testing::Test {};

TEST_F(DictionaryDecoderCorruptionTest, NegativeEntryCountReturnsCorrupted) {
    // A dictionary page begins with a var-int entry count. A negative count
    // (e.g. -1) is corrupt: it must be rejected as E_TSFILE_CORRUPTED rather
    // than dereferencing an empty entry index (which used to SIGSEGV).
    common::ByteStream stream(64, common::MOD_DICENCODE_OBJ);
    common::SerializationUtil::write_var_int(-1, stream);
    // Trailing encoded-index data; never reached once the negative count is
    // rejected.
    common::SerializationUtil::write_var_int(0, stream);

    DictionaryDecoder decoder;
    decoder.init();

    common::PageArena pa;
    pa.init(64, common::MOD_DEFAULT);
    common::String out;
    ASSERT_EQ(decoder.read_String(out, pa, stream), common::E_TSFILE_CORRUPTED);
}

}  // namespace storage
