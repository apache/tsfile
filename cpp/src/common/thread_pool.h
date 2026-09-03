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
#ifndef COMMON_THREAD_POOL_H
#define COMMON_THREAD_POOL_H

#ifdef ENABLE_THREADS

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "common/logger/elog.h"

namespace common {

// Unified fixed-size thread pool supporting both fire-and-forget tasks
// (submit void + wait_all) and future-returning tasks (submit<F>).
// Used by both write path (column-parallel encoding) and read path
// (column-parallel decoding).
class ThreadPool {
   public:
    explicit ThreadPool(size_t num_threads)
        // A zero-thread pool would silently accept submit() but wait_all()
        // would block forever because active_ never reaches 0.  init_common()
        // already clamps the configured size to >= 1 before building the
        // global pool; this normalization is a defensive backstop so any
        // direct ThreadPool(0) still makes progress.
        : num_threads_(num_threads == 0 ? 1 : num_threads),
          stop_(false),
          active_(0) {
        for (size_t i = 0; i < num_threads_; i++) {
            workers_.emplace_back([this, i] { worker_loop(i); });
        }
    }

    // Returns this worker's index in [0, num_threads).  Returns SIZE_MAX when
    // called from a non-pool thread.  Used by callers that want per-worker
    // state (e.g., per-worker decoders/compressors).
    static size_t current_worker_id() { return tl_worker_id_(); }

    size_t num_threads() const { return num_threads_; }

    ~ThreadPool() {
        {
            std::lock_guard<std::mutex> lk(mu_);
            stop_ = true;
        }
        cv_work_.notify_all();
        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
    }

    // Submit a fire-and-forget task (no return value).
    void submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lk(mu_);
            tasks_.push(std::move(task));
            active_++;
        }
        cv_work_.notify_one();
    }

    // Submit a task that returns a value via std::future.
    template <typename F>
    std::future<typename std::result_of<F()>::type> submit(F&& f) {
        using RetType = typename std::result_of<F()>::type;
        auto task =
            std::make_shared<std::packaged_task<RetType()>>(std::forward<F>(f));
        std::future<RetType> result = task->get_future();
        {
            std::lock_guard<std::mutex> lk(mu_);
            tasks_.emplace([task]() { (*task)(); });
            active_++;
        }
        cv_work_.notify_one();
        return result;
    }

    // Block until all submitted tasks have completed.
    void wait_all() {
        std::unique_lock<std::mutex> lk(mu_);
        cv_done_.wait(lk, [this] { return active_ == 0 && tasks_.empty(); });
    }

   private:
    void worker_loop(size_t id) {
        tl_worker_id_() = id;
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_work_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            // Without the try/catch, a task that throws would:
            //   (1) skip the active_-- below → wait_all() blocks forever
            //       because active_ never drops to zero, and
            //   (2) propagate the exception out of the std::thread function
            //       → std::terminate() takes down the whole process.
            // Swallowing the exception is unfortunate but it matches the
            // contract of the public submit(std::function<void()>) overload
            // which has no way to surface the failure back to the caller.
            // submit<F>() callers receive their error via the std::future
            // wrapper installed by std::packaged_task — that path never
            // reaches here, so this catch only fires for fire-and-forget
            // tasks where the alternative is termination.
            try {
                task();
            } catch (const std::exception& e) {
                // Suppressed to keep the worker alive and wait_all() unblocked
                // (see comment above); logged so the failure is not silent.
                LOGE("ThreadPool worker: task threw std::exception: "
                     << e.what());
            } catch (...) {
                LOGE("ThreadPool worker: task threw a non-standard exception");
            }
            {
                std::lock_guard<std::mutex> lk(mu_);
                active_--;
            }
            cv_done_.notify_one();
        }
    }

    // Wrapped in a function so static-initialization order is well-defined
    // (function-local static is zero-initialized to a sentinel).
    static size_t& tl_worker_id_() {
        static thread_local size_t id = static_cast<size_t>(-1);
        return id;
    }

    size_t num_threads_;
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mu_;
    std::condition_variable cv_work_;
    std::condition_variable cv_done_;
    bool stop_;
    int active_;
};

}  // namespace common

#endif  // ENABLE_THREADS

#endif  // COMMON_THREAD_POOL_H
