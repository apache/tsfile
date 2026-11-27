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

#include "file/cached_random_access_read_file.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <utility>
#include <vector>

#include "utils/errno_define.h"

namespace {

char pattern_at(int64_t offset) {
    return static_cast<char>((offset * 31 + 7) & 0xFF);
}

class MockSource : public storage::RandomAccessReadFile {
   public:
    explicit MockSource(int64_t size, int delay_ms = 0)
        : size_(size),
          delay_ms_(delay_ms),
          opened_(true),
          requests_(0),
          active_(0),
          max_active_(0) {
        data_.resize(static_cast<size_t>(size));
        for (int64_t i = 0; i < size_; i++) {
            data_[static_cast<size_t>(i)] = pattern_at(i);
        }
    }

    bool is_opened() const override { return opened_.load(); }
    int64_t file_size() const override { return size_; }
    const std::string& file_path() const override { return path_; }
    int generation(uint64_t& size, uint64_t& fingerprint) const override {
        size = static_cast<uint64_t>(size_);
        fingerprint = 11;
        return common::E_OK;
    }

    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            requests_++;
            if (fail_requests_ > 0) {
                fail_requests_--;
                return common::E_FILE_READ_ERR;
            }
            offsets_.push_back(offset);
            sizes_.push_back(size);
        }
        if (delay_ms_ > 0) {
            const int active = ++active_;
            int observed = max_active_.load();
            while (active > observed &&
                   !max_active_.compare_exchange_weak(observed, active)) {
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms_));
            --active_;
        }
        read_size = 0;
        if (!opened_.load() || offset < 0 || size < 0 || offset >= size_) {
            return common::E_OK;
        }
        int64_t available = size_ - offset;
        int32_t want =
            available < size ? static_cast<int32_t>(available) : size;
        std::memcpy(buffer, data_.data() + offset, static_cast<size_t>(want));
        read_size = want;
        return common::E_OK;
    }

    void close() override { opened_.store(false); }

    void fail_next_requests(int count) {
        std::lock_guard<std::mutex> lock(mutex_);
        fail_requests_ = count;
    }
    int requests() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return requests_;
    }
    int max_active() const { return max_active_.load(); }
    std::vector<int32_t> sizes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return sizes_;
    }

   private:
    std::string path_ = "mock";
    int64_t size_;
    int delay_ms_;
    std::atomic<bool> opened_;
    mutable std::mutex mutex_;
    int requests_;
    std::atomic<int> active_;
    std::atomic<int> max_active_;
    int fail_requests_ = 0;
    std::vector<int64_t> offsets_;
    std::vector<int32_t> sizes_;
    std::vector<char> data_;
};

std::unique_ptr<storage::RandomAccessReadFile> wrap(
    MockSource** out_raw, int64_t size, int32_t block_size, int64_t capacity,
    int32_t readahead, int delay_ms = 0, int threads = 0) {
    MockSource* raw = new MockSource(size, delay_ms);
    *out_raw = raw;
    storage::ReadCacheOptions options;
    options.block_size = block_size;
    options.capacity_bytes = capacity;
    options.max_readahead_blocks = readahead;
    options.prefetch_threads = threads;
    return storage::make_cached_read_file(
        std::unique_ptr<storage::RandomAccessReadFile>(raw), options);
}

std::string read_range(storage::RandomAccessReadFile& file, int64_t offset,
                       int32_t size) {
    std::string out(static_cast<size_t>(size), '\0');
    int32_t got = 0;
    EXPECT_EQ(common::E_OK, file.read(offset, &out[0], size, got));
    EXPECT_EQ(size, got);
    out.resize(static_cast<size_t>(got));
    return out;
}

std::string expected_range(int64_t offset, int32_t size) {
    std::string out(static_cast<size_t>(size), '\0');
    for (int32_t i = 0; i < size; i++) {
        out[static_cast<size_t>(i)] = pattern_at(offset + i);
    }
    return out;
}

TEST(CachedRandomAccessReadFileTest, RepeatedReadServedFromCache) {
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, 64 * 1024, 8 * 1024, 1 << 20, 0);
    const std::string first = read_range(*file, 100, 200);
    const int after_first = raw->requests();
    const std::string second = read_range(*file, 100, 200);
    EXPECT_EQ(first, expected_range(100, 200));
    EXPECT_EQ(first, second);
    EXPECT_EQ(after_first, raw->requests()) << "second read must hit the cache";
}

TEST(CachedRandomAccessReadFileTest, SequentialScanCoalescesRequests) {
    const int64_t size = 64 * 1024;
    const int32_t block = 4 * 1024;
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, size, block, 1 << 20, 8);
    std::string whole;
    for (int64_t offset = 0; offset < size; offset += 1024) {
        whole += read_range(*file, offset, 1024);
    }
    EXPECT_EQ(whole, expected_range(0, static_cast<int32_t>(size)));
    // 16 blocks; read-ahead must bring the request count far below that.
    EXPECT_LT(raw->requests(), 8) << "sequential scan should coalesce";
    const std::vector<int32_t> sizes = raw->sizes();
    for (size_t i = 0; i < sizes.size(); i++) {
        EXPECT_GE(sizes[i], block) << "a coalesced request spans whole blocks";
    }
}

TEST(CachedRandomAccessReadFileTest, RandomReadsDoNotReadAhead) {
    const int64_t size = 256 * 1024;
    const int32_t block = 4 * 1024;
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, size, block, 1 << 20, 8);
    const int64_t offsets[] = {1000, 50 * 1024, 200 * 1024, 3 * 1024,
                               150 * 1024};
    std::set<int64_t> distinct_blocks;
    for (size_t i = 0; i < sizeof(offsets) / sizeof(offsets[0]); i++) {
        EXPECT_EQ(read_range(*file, offsets[i], 64),
                  expected_range(offsets[i], 64));
        distinct_blocks.insert(offsets[i] / block);
    }
    // One block per distinct block touched, no read-ahead on random access.
    EXPECT_EQ(static_cast<int>(distinct_blocks.size()), raw->requests());
    const std::vector<int32_t> sizes = raw->sizes();
    for (size_t i = 0; i < sizes.size(); i++) {
        EXPECT_EQ(block, sizes[i]);
    }
}

TEST(CachedRandomAccessReadFileTest, ReadSpanningBlocksIsConsistent) {
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, 40 * 1024, 4 * 1024, 1 << 20, 0);
    // Range starts mid-block and crosses three block boundaries.
    EXPECT_EQ(read_range(*file, 3000, 10000), expected_range(3000, 10000));
    EXPECT_EQ(read_range(*file, 5000, 17), expected_range(5000, 17));
}

TEST(CachedRandomAccessReadFileTest, ReadsClampAtEndOfFile) {
    const int64_t size = 10 * 1024 + 123;
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, size, 4096, 1 << 20, 0);
    // Last partial block.
    EXPECT_EQ(read_range(*file, size - 200, 200),
              expected_range(size - 200, 200));
    // A read past EOF returns a short read, like the underlying source.
    char buffer[16];
    int32_t got = -1;
    EXPECT_EQ(common::E_OK, file->read(size - 4, buffer, 16, got));
    EXPECT_EQ(4, got);
    EXPECT_EQ(expected_range(size - 4, 4),
              std::string(buffer, static_cast<size_t>(got)));
    got = -1;
    EXPECT_EQ(common::E_OK, file->read(size, buffer, 16, got));
    EXPECT_EQ(0, got);
}

TEST(CachedRandomAccessReadFileTest, ConcurrentMissesShareOneLoad) {
    const int32_t block = 4096;
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, 64 * 1024, block, 1 << 20, 0, 30 /* ms per request */);
    const int kThreads = 8;
    std::vector<std::thread> threads;
    std::atomic<int> failures(0);
    for (int t = 0; t < kThreads; t++) {
        threads.push_back(std::thread([&file, &failures, t]() {
            std::string out(1000, '\0');
            int32_t got = 0;
            if (file->read(100, &out[0], 1000, got) != common::E_OK ||
                got != 1000 || out != expected_range(100, 1000)) {
                failures++;
            }
            (void)t;
        }));
    }
    for (size_t i = 0; i < threads.size(); i++) {
        threads[i].join();
    }
    EXPECT_EQ(0, failures.load());
    EXPECT_EQ(1, raw->requests()) << "concurrent misses must be coalesced";
}

TEST(CachedRandomAccessReadFileTest, ParallelPrefetchOverlapsRequests) {
    const int64_t size = 64 * 4096;
    const int32_t block = 4096;
    MockSource* raw = nullptr;
    storage::ReadCacheOptions options;
    options.block_size = block;
    options.capacity_bytes = 1 << 20;
    options.max_readahead_blocks = 4;
    options.prefetch_threads = 4;
    raw = new MockSource(size, 20 /* ms per request */);
    std::unique_ptr<storage::RandomAccessReadFile> file =
        storage::make_cached_read_file(
            std::unique_ptr<storage::RandomAccessReadFile>(raw), options);
    std::string whole;
    for (int64_t offset = 0; offset < size; offset += 1024) {
        whole += read_range(*file, offset, 1024);
    }
    EXPECT_EQ(whole, expected_range(0, static_cast<int32_t>(size)));
    EXPECT_GE(raw->max_active(), 2)
        << "prefetch workers must keep several requests in flight";
    EXPECT_LT(raw->requests(), static_cast<int>(size / block))
        << "read-ahead must cover more than one block per request";
}

TEST(CachedRandomAccessReadFileTest, FailedLoadPropagatesAndIsNotCached) {
    for (int threads : {0, 4}) {
        SCOPED_TRACE(threads);
        MockSource* raw = nullptr;
        std::unique_ptr<storage::RandomAccessReadFile> file =
            wrap(&raw, 64 * 1024, 4096, 1 << 20, 0, 0, threads);
        raw->fail_next_requests(1);
        char buffer[64];
        int32_t got = 0;
        EXPECT_EQ(common::E_FILE_READ_ERR, file->read(0, buffer, 64, got));
        // The failed range is dropped, so the retry goes to the source again
        // and then serves the cached block.
        EXPECT_EQ(read_range(*file, 0, 64), expected_range(0, 64));
        const int after_retry = raw->requests();
        EXPECT_EQ(2, after_retry);
        EXPECT_EQ(read_range(*file, 0, 64), expected_range(0, 64));
        EXPECT_EQ(after_retry, raw->requests());
    }
}

TEST(CachedRandomAccessReadFileTest, CapacityEvictsLeastRecentlyUsed) {
    for (int threads : {0, 4}) {
        SCOPED_TRACE(threads);
        const int32_t block = 4096;
        MockSource* raw = nullptr;
        // Room for exactly two blocks.
        std::unique_ptr<storage::RandomAccessReadFile> file =
            wrap(&raw, 20 * 4096, block, 2 * block, 0, 0, threads);
        for (int64_t i = 0; i < 5; i++) {
            EXPECT_EQ(read_range(*file, i * block, 100),
                      expected_range(i * block, 100));
        }
        const int after_scan = raw->requests();
        EXPECT_EQ(5, after_scan);
        // Touching the last block again is a hit; the first block has been
        // evicted.
        EXPECT_EQ(read_range(*file, 4 * block, 100),
                  expected_range(4 * block, 100));
        EXPECT_EQ(after_scan, raw->requests());
        EXPECT_EQ(read_range(*file, 0, 100), expected_range(0, 100));
        EXPECT_EQ(after_scan + 1, raw->requests());
    }
}

TEST(CachedRandomAccessReadFileTest, DisabledOptionsKeepInnerSource) {
    MockSource* raw = nullptr;
    // Capacity smaller than one block disables the cache.
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, 64 * 1024, 4096, 1000, 0);
    EXPECT_EQ(read_range(*file, 10, 100), expected_range(10, 100));
    const std::vector<int32_t> sizes = raw->sizes();
    ASSERT_EQ(1u, sizes.size());
    EXPECT_EQ(100, sizes[0]) << "uncached source must see the original size";
}

TEST(CachedRandomAccessReadFileTest, CloseRejectsFurtherReads) {
    MockSource* raw = nullptr;
    std::unique_ptr<storage::RandomAccessReadFile> file =
        wrap(&raw, 64 * 1024, 4096, 1 << 20, 0);
    EXPECT_EQ(read_range(*file, 0, 16), expected_range(0, 16));
    file->close();
    char buffer[16];
    int32_t got = 0;
    EXPECT_EQ(common::E_FILE_READ_ERR, file->read(0, buffer, 16, got));
}

}  // namespace
