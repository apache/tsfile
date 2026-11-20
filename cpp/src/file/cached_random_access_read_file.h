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

#ifndef FILE_CACHED_RANDOM_ACCESS_READ_FILE_H
#define FILE_CACHED_RANDOM_ACCESS_READ_FILE_H

#include <stdint.h>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "file/random_access_read_file.h"

namespace storage {

/**
 * Tuning knobs for CachedRandomAccessReadFile.  All sizes are in bytes.
 *
 * The cache serves every consumer read() out of fixed-size blocks, so a remote
 * source sees one block transfer instead of the small page-header and
 * page-data reads the decoder issues.  While accesses proceed sequentially the
 * cache coalesces up to max_readahead_blocks extra blocks into the same
 * transfer, which collapses a full scan into roughly file_size / block_size
 * requests.
 */
struct ReadCacheOptions {
    int32_t block_size = 256 * 1024;
    int64_t capacity_bytes = 256ll * 1024 * 1024;
    int32_t max_readahead_blocks = 8;
    // When > 0, read-ahead blocks are fetched by this many background workers
    // (one block per request) instead of being coalesced into one large
    // synchronous transfer.  This is what lets a native object-storage source
    // keep several range requests in flight; it is left at 0 for sources whose
    // requests already serialize (the Python file-object bridge).
    int32_t prefetch_threads = 0;
};

/**
 * Wrap @p inner in a block cache, or return it unchanged when the options
 * describe a disabled or degenerate cache (non-positive block size, capacity
 * smaller than one block).
 */
std::unique_ptr<RandomAccessReadFile> make_cached_read_file(
    std::unique_ptr<RandomAccessReadFile> inner,
    const ReadCacheOptions& options);

/**
 * A block-caching, read-ahead decorator for a byte source whose reads are far
 * more expensive than memory copies (object storage reached through a Python
 * file object, or a future native S3/OSS backend).
 *
 * Concurrency: read() may be called from several threads.  A miss loads its
 * block outside the lock and publishes it through the shared condition
 * variable, so concurrent misses for different blocks overlap, while misses
 * for the same block are coalesced into a single load.
 */
class CachedRandomAccessReadFile : public RandomAccessReadFile {
   public:
    CachedRandomAccessReadFile(std::unique_ptr<RandomAccessReadFile> inner,
                               int32_t block_size, int64_t capacity_bytes,
                               int32_t max_readahead_blocks,
                               int32_t prefetch_threads);
    ~CachedRandomAccessReadFile() override;

    bool is_opened() const override;
    int64_t file_size() const override { return file_size_; }
    const std::string& file_path() const override;
    int generation(uint64_t& size, uint64_t& fingerprint) const override;
    int read(int64_t offset, char* buffer, int32_t size,
             int32_t& read_size) override;
    void close() override;

   private:
    struct Block {
        Block() : valid_bytes(0), loading(true), in_lru(false) {}
        std::vector<char> data;
        int32_t valid_bytes;
        bool loading;
        bool in_lru;
        std::list<int64_t>::iterator lru_it;
    };

    int64_t block_count() const;
    int32_t block_valid_bytes(int64_t index) const;
    // Serve block @p index, loading it (and possibly a read-ahead run) on a
    // miss.  @p lock is the caller's held lock; it is released while the inner
    // source is read and re-acquired before returning.
    int acquire_block_locked(std::unique_lock<std::mutex>& lock, int64_t index,
                             std::shared_ptr<Block>& block);
    // Read [first_block * block_size, end) from the inner source and publish
    // the given placeholder blocks.  Called without holding the lock.
    int load_blocks(const std::vector<int64_t>& indices,
                    const std::vector<std::shared_ptr<Block>>& blocks);
    void note_access_locked(int64_t index);
    void evict_locked();
    // Publish (or drop, on failure) a finished load and wake the waiters.
    void finish_load_locked(const std::vector<int64_t>& indices,
                            const std::vector<std::shared_ptr<Block>>& blocks,
                            int ret);

    struct PrefetchTask {
        std::vector<int64_t> indices;
        std::vector<std::shared_ptr<Block>> blocks;
    };
    void start_prefetch_workers();
    void stop_prefetch_workers();
    void prefetch_worker_loop();
    void submit_prefetch(const std::vector<int64_t>& indices,
                         const std::vector<std::shared_ptr<Block>>& blocks);

    std::unique_ptr<RandomAccessReadFile> inner_;
    const int64_t block_size_;
    const int64_t max_blocks_;
    const int32_t max_readahead_blocks_;
    const int64_t file_size_;

    mutable std::mutex mutex_;
    std::condition_variable condition_;
    std::unordered_map<int64_t, std::shared_ptr<Block>> blocks_;
    // Most recently used blocks at the front.
    std::list<int64_t> lru_;
    int64_t last_block_;
    int64_t sequential_streak_;
    std::atomic<bool> closed_;

    // Unique per live instance; guards the thread-local fast path against a
    // new cache reusing a destroyed one's address.
    const uint64_t instance_id_;
    const int32_t prefetch_threads_;
    std::vector<std::thread> workers_;
    std::deque<PrefetchTask> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    bool stopping_;
};

}  // end namespace storage
#endif  // FILE_CACHED_RANDOM_ACCESS_READ_FILE_H
