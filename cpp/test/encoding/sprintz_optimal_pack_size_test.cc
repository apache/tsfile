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

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "encoding/optimal/sprintz_optimal_pack_size.h"

using storage::optimal::SprintzOptimalPackSize;

namespace {

static int64_t cost_for_pack_size(const std::vector<int64_t> &vals, int p) {
    const int bits_overhead = 80;
    const int n = (int)vals.size();
    const int m = (n + p - 1) / p;
    int64_t cost = (int64_t)m * bits_overhead;
    for (int i = 0; i < m; i++) {
        int start = i * p;
        int end = std::min(start + p, n);
        int max_bw = 1;
        for (int j = start; j < end; j++) {
            uint64_t v = (uint64_t)std::max<int64_t>(1, vals[j]);
            int bw = 64 - __builtin_clzll(v);
            max_bw = std::max(max_bw, bw);
        }
        cost += (int64_t)(end - start) * (int64_t)max_bw;
    }
    return cost;
}

TEST(SprintzOptimalPackSizeTest, MatchesBruteforceOnSmallInputs) {
    std::vector<std::vector<int64_t>> cases = {
        {1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
        {1, 2, 3, 4, 5, 6, 7, 8, 9},
        {1, 255, 1, 255, 1, 255, 1, 255, 1, 255, 1, 255},
        {1, 1, 1, 1, 1024, 1024, 1024, 1024, 1, 1, 1, 1},
    };

    for (const auto &vals : cases) {
        int n = (int)vals.size();
        int best = SprintzOptimalPackSize::find_optimal_pack_size(vals.data(), n, nullptr);
        best = std::max(1, std::min(32, best));

        int max_p = std::min(32, n);
        int best_bf = 1;
        int64_t best_cost = INT64_MAX;
        for (int p = 1; p <= max_p; p++) {
            int64_t c = cost_for_pack_size(vals, p);
            if (c < best_cost) {
                best_cost = c;
                best_bf = p;
            }
        }

        ASSERT_EQ(best, best_bf);
    }
}

}  // namespace

