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

#include <algorithm>
#include <atomic>
#include <cstring>
#include <utility>

#include "utils/errno_define.h"
#include "utils/util_define.h"

namespace storage {

namespace {

// The reader never legitimately asks for more than a page or a chunk header
// region at once; a read-ahead run larger than this only wastes bandwidth.
const int64_t MAX_BYTES_PER_INNER_READ = 64ll * 1024 * 1024;

std::atomic<uint64_t> g_cache_instance_counter(0);

}  // namespace

std::unique_ptr<RandomAccessReadFile> make_cached_read_file(
    std::unique_ptr<RandomAccessReadFile> inner,
    const ReadCacheOptions& options) {
    if (!inner) {
        return inner;
    }
    int32_t block_size = options.block_size;
    if (block_size < 4096) {
        block_size = 4096;
    }
    if (block_size > 64 * 1024 * 1024) {
        block_size = 64 * 1024 * 1024;
    }
    if (options.capacity_bytes < block_size) {
        return inner;
    }
    int32_t readahead = options.max_readahead_blocks;
    if (readahead < 0) {
        readahead = 0;
    }
    if (readahead > 64) {
        readahead = 64;
    }
    int32_t prefetch_threads = options.prefetch_threads;
    if (prefetch_threads < 0) {
        prefetch_threads = 0;
    }
    if (prefetch_threads > 64) {
        prefetch_threads = 64;
    }
    return std::unique_ptr<RandomAccessReadFile>(new CachedRandomAccessReadFile(
        std::move(inner), block_size, options.capacity_bytes, readahead,
        prefetch_threads));
}

CachedRandomAccessReadFile::CachedRandomAccessReadFile(
    std::unique_ptr<RandomAccessReadFile> inner, int32_t block_size,
    int64_t capacity_bytes, int32_t max_readahead_blocks,
    int32_t prefetch_threads)
    : inner_(std::move(inner)),
      block_size_(block_size),
      max_blocks_(capacity_bytes / block_size > 0 ? capacity_bytes / block_size
                                                  : 1),
      max_readahead_blocks_(max_readahead_blocks),
      file_size_(inner_ != nullptr ? inner_->file_size() : 0),
      last_block_(-1),
      sequential_streak_(0),
      closed_(false),
      instance_id_(++g_cache_instance_counter),
      prefetch_threads_(prefetch_threads),
      stopping_(false) {
    if (prefetch_threads_ > 0) {
        start_prefetch_workers();
    }
}

CachedRandomAccessReadFile::~CachedRandomAccessReadFile() { close(); }

bool CachedRandomAccessReadFile::is_opened() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return !closed_ && inner_ != nullptr && inner_->is_opened();
}

const std::string& CachedRandomAccessReadFile::file_path() const {
    static const std::string kEmptyPath;
    // The path never changes for the lifetime of the source, so this is safe
    // without holding the lock.
    return inner_ != nullptr ? inner_->file_path() : kEmptyPath;
}

int CachedRandomAccessReadFile::generation(uint64_t& size,
                                           uint64_t& fingerprint) const {
    if (inner_ == nullptr) {
        return common::E_FILE_READ_ERR;
    }
    return inner_->generation(size, fingerprint);
}

void CachedRandomAccessReadFile::close() {
    stop_prefetch_workers();
    std::unique_ptr<RandomAccessReadFile> inner;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed_) {
            return;
        }
        closed_ = true;
        blocks_.clear();
        lru_.clear();
        inner.swap(inner_);
    }
    condition_.notify_all();
    if (inner != nullptr) {
        inner->close();
    }
}

int64_t CachedRandomAccessReadFile::block_count() const {
    return (file_size_ + block_size_ - 1) / block_size_;
}

int32_t CachedRandomAccessReadFile::block_valid_bytes(int64_t index) const {
    int64_t remaining = file_size_ - index * block_size_;
    if (remaining <= 0) {
        return 0;
    }
    return remaining < block_size_ ? static_cast<int32_t>(remaining)
                                   : static_cast<int32_t>(block_size_);
}

void CachedRandomAccessReadFile::note_access_locked(int64_t index) {
    // Repeated reads of the same block (a page split into several decoder
    // reads) must not reset the sequential run.
    if (index != last_block_) {
        if (last_block_ >= 0 && index == last_block_ + 1) {
            ++sequential_streak_;
        } else {
            sequential_streak_ = 0;
        }
        last_block_ = index;
    }
    std::unordered_map<int64_t, std::shared_ptr<Block>>::iterator it =
        blocks_.find(index);
    if (it == blocks_.end() || !it->second->in_lru) {
        return;
    }
    lru_.erase(it->second->lru_it);
    lru_.push_front(index);
    it->second->lru_it = lru_.begin();
}

void CachedRandomAccessReadFile::evict_locked() {
    while (static_cast<int64_t>(blocks_.size()) > max_blocks_ &&
           !lru_.empty()) {
        const int64_t victim = lru_.back();
        lru_.pop_back();
        std::unordered_map<int64_t, std::shared_ptr<Block>>::iterator it =
            blocks_.find(victim);
        if (it == blocks_.end()) {
            continue;
        }
        it->second->in_lru = false;
        blocks_.erase(it);
    }
}

int CachedRandomAccessReadFile::load_blocks(
    const std::vector<int64_t>& indices,
    const std::vector<std::shared_ptr<Block>>& blocks) {
    const int64_t start = indices.front() * block_size_;
    int64_t end = (indices.back() + 1) * block_size_;
    if (end > file_size_) {
        end = file_size_;
    }
    const int64_t length = end - start;
    if (length <= 0) {
        return common::E_FILE_READ_ERR;
    }
    std::vector<char> buffer(static_cast<size_t>(length));
    int32_t read_len = 0;
    int ret = inner_->read(start, buffer.data(), static_cast<int32_t>(length),
                           read_len);
    if (ret != common::E_OK) {
        return ret;
    }
    // The RandomAccessReadFile contract requires a full fill unless EOF was
    // hit, and the range was clamped to the file size above.
    if (read_len != length) {
        return common::E_FILE_READ_ERR;
    }
    for (size_t k = 0; k < indices.size(); k++) {
        const int64_t block_start = indices[k] * block_size_;
        const int32_t valid = block_valid_bytes(indices[k]);
        blocks[k]->data.resize(static_cast<size_t>(valid));
        if (valid > 0) {
            std::memcpy(blocks[k]->data.data(),
                        buffer.data() + (block_start - start),
                        static_cast<size_t>(valid));
        }
        blocks[k]->valid_bytes = valid;
    }
    return common::E_OK;
}

int CachedRandomAccessReadFile::acquire_block_locked(
    std::unique_lock<std::mutex>& lock, int64_t index,
    std::shared_ptr<Block>& block) {
    while (true) {
        if (closed_ || inner_ == nullptr) {
            return common::E_FILE_READ_ERR;
        }
        std::unordered_map<int64_t, std::shared_ptr<Block>>::iterator it =
            blocks_.find(index);
        if (it != blocks_.end()) {
            if (!it->second->loading) {
                note_access_locked(index);
                block = it->second;
                return common::E_OK;
            }
            // Another thread is loading this block; wait for it to publish
            // instead of issuing a duplicate request.
            condition_.wait(lock);
            continue;
        }

        // Miss.  Size the read-ahead run from how long the access stream has
        // been sequential.  The first miss of a random stream reads exactly
        // one block; once a sequential run is observed the extra blocks grow
        // 1, 2, 4, ... up to the configured maximum.
        // Background prefetch keeps several requests in flight, so it can
        // use a wider window than the single-transfer read-ahead; otherwise
        // the consumer catches up with the workers between runs.
        const int64_t readahead_cap = prefetch_threads_ > 0
                                          ? max_readahead_blocks_ * 4
                                          : max_readahead_blocks_;
        int64_t run = 1;
        if (readahead_cap > 0 && sequential_streak_ > 0) {
            const int64_t shift =
                sequential_streak_ < 5 ? sequential_streak_ : 5;
            int64_t extra = 1LL << (shift - 1);
            if (extra > readahead_cap) {
                extra = readahead_cap;
            }
            run += extra;
        }
        const int64_t total = block_count();
        if (run > total - index) {
            run = total - index;
        }
        if (run > max_blocks_) {
            run = max_blocks_;
        }
        if (run * block_size_ > MAX_BYTES_PER_INNER_READ) {
            run = MAX_BYTES_PER_INNER_READ / block_size_;
        }
        if (run < 1) {
            run = 1;
        }

        // Publish placeholders for the contiguous run of absent blocks.  An
        // already-present block ends the run: it was either loaded by another
        // thread or is being loaded, in which case waiting (next loop) is
        // cheaper than re-fetching it.
        std::vector<int64_t> indices;
        indices.reserve(static_cast<size_t>(run));
        for (int64_t i = index; i < index + run; i++) {
            if (blocks_.find(i) != blocks_.end()) {
                break;
            }
            indices.push_back(i);
        }
        std::vector<std::shared_ptr<Block>> placeholders;
        placeholders.reserve(indices.size());
        for (size_t k = 0; k < indices.size(); k++) {
            std::shared_ptr<Block> fresh(new Block());
            blocks_[indices[k]] = fresh;
            placeholders.push_back(fresh);
        }

        // With background prefetch workers the whole run (the requested block
        // included) is handed to the workers so every request overlaps; this
        // thread only waits for its own block.  Without workers the run is
        // coalesced into one large synchronous transfer.
        const bool parallel_prefetch =
            prefetch_threads_ > 0 && !indices.empty();
        if (parallel_prefetch) {
            // Each worker request covers a contiguous group of blocks: one
            // large transfer per request keeps per-request throughput, and
            // several groups in flight across the workers hide the latency.
            const size_t group = static_cast<size_t>(
                max_readahead_blocks_ > 0 ? max_readahead_blocks_ : 1);
            for (size_t first = 0; first < indices.size(); first += group) {
                const size_t last = std::min(indices.size(), first + group);
                std::vector<int64_t> task_indices(indices.begin() + first,
                                                  indices.begin() + last);
                std::vector<std::shared_ptr<Block>> task_blocks(
                    placeholders.begin() + first, placeholders.begin() + last);
                submit_prefetch(task_indices, task_blocks);
            }
            while (true) {
                if (closed_ || inner_ == nullptr) {
                    return common::E_FILE_READ_ERR;
                }
                std::unordered_map<int64_t, std::shared_ptr<Block>>::iterator
                    current = blocks_.find(index);
                if (current == blocks_.end()) {
                    // A prefetch load for this block failed; report it so the
                    // caller does not treat the data as successfully read.
                    return common::E_FILE_READ_ERR;
                }
                if (!current->second->loading) {
                    note_access_locked(index);
                    block = current->second;
                    return common::E_OK;
                }
                condition_.wait(lock);
            }
        }

        lock.unlock();
        const int ret = load_blocks(indices, placeholders);
        lock.lock();

        finish_load_locked(indices, placeholders, ret);
        if (ret != common::E_OK || closed_) {
            condition_.notify_all();
            return ret != common::E_OK ? ret : common::E_FILE_READ_ERR;
        }
        // Record the access before eviction so the requested block is at the
        // front of the LRU list and cannot be evicted before it is served.
        note_access_locked(index);
        evict_locked();
        condition_.notify_all();
    }
}

void CachedRandomAccessReadFile::finish_load_locked(
    const std::vector<int64_t>& indices,
    const std::vector<std::shared_ptr<Block>>& blocks, int ret) {
    if (ret != common::E_OK || closed_) {
        for (size_t k = 0; k < indices.size(); k++) {
            std::unordered_map<int64_t, std::shared_ptr<Block>>::iterator
                inserted = blocks_.find(indices[k]);
            if (inserted != blocks_.end() && inserted->second == blocks[k]) {
                blocks_.erase(inserted);
            }
        }
        condition_.notify_all();
        return;
    }
    for (size_t k = 0; k < indices.size(); k++) {
        std::unordered_map<int64_t, std::shared_ptr<Block>>::iterator inserted =
            blocks_.find(indices[k]);
        if (inserted == blocks_.end() || inserted->second != blocks[k]) {
            continue;
        }
        blocks[k]->loading = false;
        blocks[k]->in_lru = true;
        lru_.push_front(indices[k]);
        blocks[k]->lru_it = lru_.begin();
    }
    condition_.notify_all();
}

void CachedRandomAccessReadFile::start_prefetch_workers() {
    for (int32_t i = 0; i < prefetch_threads_; i++) {
        workers_.push_back(std::thread(
            &CachedRandomAccessReadFile::prefetch_worker_loop, this));
    }
}

void CachedRandomAccessReadFile::stop_prefetch_workers() {
    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        if (stopping_) {
            return;
        }
        stopping_ = true;
        tasks_.clear();
    }
    queue_cv_.notify_all();
    for (size_t i = 0; i < workers_.size(); i++) {
        if (workers_[i].joinable()) {
            workers_[i].join();
        }
    }
    workers_.clear();
}

void CachedRandomAccessReadFile::submit_prefetch(
    const std::vector<int64_t>& indices,
    const std::vector<std::shared_ptr<Block>>& blocks) {
    {
        std::lock_guard<std::mutex> queue_lock(queue_mutex_);
        if (stopping_) {
            return;
        }
        PrefetchTask task;
        task.indices = indices;
        task.blocks = blocks;
        tasks_.push_back(task);
    }
    queue_cv_.notify_one();
}

void CachedRandomAccessReadFile::prefetch_worker_loop() {
    while (true) {
        PrefetchTask task;
        {
            std::unique_lock<std::mutex> queue_lock(queue_mutex_);
            while (!stopping_ && tasks_.empty()) {
                queue_cv_.wait(queue_lock);
            }
            if (tasks_.empty()) {
                return;  // stopping_ and the queue is drained
            }
            task = tasks_.front();
            tasks_.pop_front();
        }
        const int ret = load_blocks(task.indices, task.blocks);
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finish_load_locked(task.indices, task.blocks, ret);
        }
    }
}

int CachedRandomAccessReadFile::read(int64_t offset, char* buffer, int32_t size,
                                     int32_t& read_size) {
    read_size = 0;
    if (offset < 0 || size < 0 || (buffer == nullptr && size > 0)) {
        return common::E_INVALID_ARG;
    }
    if (size == 0) {
        return common::E_OK;
    }
    // Pages are far smaller than a block, so consecutive reads by one thread
    // usually hit the block it just fetched.  Keep the last block per thread
    // and serve such reads without taking the cache lock at all; published
    // blocks are immutable and eviction only drops the map entry, so an
    // evicted block stays valid for this reference.
    struct ThreadLocalBlock {
        ThreadLocalBlock() : owner_id(0), index(-1) {}
        uint64_t owner_id;
        int64_t index;
        std::shared_ptr<Block> block;
    };
    static thread_local ThreadLocalBlock tls_block;
    if (offset < file_size_) {
        int64_t fast_end = offset + static_cast<int64_t>(size);
        if (fast_end > file_size_) {
            fast_end = file_size_;
        }
        const int64_t fast_first = offset / block_size_;
        if (fast_first == (fast_end - 1) / block_size_ &&
            LIKELY(!closed_.load(std::memory_order_relaxed) &&
                   tls_block.owner_id == instance_id_ &&
                   tls_block.index == fast_first && tls_block.block)) {
            const int64_t block_start = fast_first * block_size_;
            int64_t copy_start = offset > block_start ? offset : block_start;
            const int64_t block_end =
                block_start + tls_block.block->valid_bytes;
            int64_t copy_end = fast_end < block_end ? fast_end : block_end;
            if (copy_end > copy_start) {
                std::memcpy(
                    buffer + (copy_start - offset),
                    tls_block.block->data.data() + (copy_start - block_start),
                    static_cast<size_t>(copy_end - copy_start));
                read_size = static_cast<int32_t>(copy_end - copy_start);
            }
            return common::E_OK;
        }
    }

    std::unique_lock<std::mutex> lock(mutex_);
    if (closed_ || inner_ == nullptr) {
        return common::E_FILE_READ_ERR;
    }
    if (offset >= file_size_) {
        return common::E_OK;
    }
    int64_t end = offset + static_cast<int64_t>(size);
    if (end > file_size_) {
        end = file_size_;
    }
    const int64_t first = offset / block_size_;
    const int64_t last = (end - 1) / block_size_;
    if (first == last) {
        std::shared_ptr<Block> block;
        const int ret = acquire_block_locked(lock, first, block);
        if (ret != common::E_OK) {
            return ret;
        }
        const int64_t block_start = first * block_size_;
        int64_t copy_start = offset > block_start ? offset : block_start;
        const int64_t block_end = block_start + block->valid_bytes;
        int64_t copy_end = end < block_end ? end : block_end;
        lock.unlock();
        if (copy_end > copy_start) {
            std::memcpy(buffer + (copy_start - offset),
                        block->data.data() + (copy_start - block_start),
                        static_cast<size_t>(copy_end - copy_start));
            read_size = static_cast<int32_t>(copy_end - copy_start);
        }
        lock.lock();
        tls_block.owner_id = instance_id_;
        tls_block.index = first;
        tls_block.block = block;
        return common::E_OK;
    }
    int64_t copied = 0;
    for (int64_t index = first; index <= last; index++) {
        std::shared_ptr<Block> block;
        const int ret = acquire_block_locked(lock, index, block);
        if (ret != common::E_OK) {
            return ret;
        }
        // Block contents are immutable once published and eviction only drops
        // the map entry, so the copy can run without the cache lock: several
        // decode threads would otherwise serialize their page copies on it.
        const int64_t block_start = index * block_size_;
        const int64_t valid_bytes = block->valid_bytes;
        lock.unlock();
        int64_t copy_start = offset > block_start ? offset : block_start;
        const int64_t block_end = block_start + valid_bytes;
        int64_t copy_end = end < block_end ? end : block_end;
        if (copy_end > copy_start) {
            std::memcpy(buffer + (copy_start - offset),
                        block->data.data() + (copy_start - block_start),
                        static_cast<size_t>(copy_end - copy_start));
            copied += copy_end - copy_start;
        }
        lock.lock();
    }
    read_size = static_cast<int32_t>(copied);
    return common::E_OK;
}

}  // end namespace storage
