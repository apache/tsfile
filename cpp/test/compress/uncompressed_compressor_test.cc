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
#include "compress/uncompressed_compressor.h"

#include <gtest/gtest.h>

#include <cstring>

namespace storage {

// Regression: after_uncompress() used to free the cached uncompressed_buf_
// member regardless of which buffer the caller actually passed in.  Two
// successive uncompress() calls would cache only the second buffer; calling
// after_uncompress(first) then freed the still-live second buffer (UAF) and
// leaked the first.  The fix frees the parameter and only clears the
// member when it matches.  We can't directly observe UAF in a unit test,
// but we can verify the contract: a buffer the caller is releasing is no
// longer used after the call, and the second buffer's contents stay
// readable until its own after_uncompress() runs.
TEST(UncompressedCompressorTest, AfterUncompressFreesParamNotMember) {
    UncompressedCompressor c;

    const char src_a[] = "AAAA-payload-A";
    const char src_b[] = "BBBB-payload-B-longer";

    char* uA = nullptr;
    uint32_t lenA = 0;
    ASSERT_EQ(
        c.uncompress(const_cast<char*>(src_a), sizeof(src_a) - 1, uA, lenA),
        common::E_OK);
    ASSERT_NE(uA, nullptr);
    ASSERT_EQ(lenA, sizeof(src_a) - 1);
    EXPECT_EQ(memcmp(uA, src_a, lenA), 0);

    char* uB = nullptr;
    uint32_t lenB = 0;
    ASSERT_EQ(
        c.uncompress(const_cast<char*>(src_b), sizeof(src_b) - 1, uB, lenB),
        common::E_OK);
    ASSERT_NE(uB, nullptr);
    EXPECT_NE(uA, uB);
    EXPECT_EQ(memcmp(uB, src_b, lenB), 0);

    // Release the FIRST buffer.  Under the old bug this would free uB
    // (the member-cached pointer) and leak uA.  Under the fix it frees uA
    // and leaves uB intact for the next read.
    c.after_uncompress(uA);
    // uB must still be readable — if we had freed it above, the cached
    // member pointer would now point into freed memory and most
    // allocators would either return the byte back to the free list or
    // poison it.  Validate via the original content.
    EXPECT_EQ(memcmp(uB, src_b, lenB), 0);

    // Releasing uB should be a clean no-op-after on the member.
    c.after_uncompress(uB);
}

}  // namespace storage
