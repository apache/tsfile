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
#ifdef ENABLE_THREADS

#include "common/thread_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

// Regression: a zero-sized ThreadPool used to silently accept submit() but
// block wait_all() forever (no worker thread, so active_ never reaches 0).
// init_common() clamps thread_count_ to >= 1 before building the global pool,
// but the ctor normalizes zero to a single worker as a defensive backstop so
// any direct ThreadPool(0) still makes progress instead of hanging.
TEST(ThreadPoolTest, ZeroThreadPoolStillExecutesAndDrains) {
    common::ThreadPool pool(0);
    EXPECT_GE(pool.num_threads(), static_cast<size_t>(1));

    std::atomic<int> ran{0};
    pool.submit([&ran]() { ran.fetch_add(1); });
    auto fut = pool.submit([]() { return 42; });

    auto wait_with_timeout = [&pool]() {
        // wait_all has no timeout; run it in a helper thread we can join().
        std::promise<void> done;
        auto fut = done.get_future();
        std::thread t([&pool, &done]() {
            pool.wait_all();
            done.set_value();
        });
        auto status = fut.wait_for(std::chrono::seconds(2));
        if (status != std::future_status::ready) {
            // Detach so a hung pool doesn't terminate the test process.
            t.detach();
            return false;
        }
        t.join();
        return true;
    };

    ASSERT_TRUE(wait_with_timeout()) << "wait_all hung — zero-thread pool";
    EXPECT_EQ(ran.load(), 1);
    EXPECT_EQ(fut.get(), 42);
}

#endif  // ENABLE_THREADS
