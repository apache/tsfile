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
#include <type_traits>
#include <vector>

namespace common {

// Unified fixed-size thread pool supporting both fire-and-forget tasks
// (submit void + wait_all) and future-returning tasks (submit<F>).
// Used by both write path (column-parallel encoding) and read path
// (column-parallel decoding).
class ThreadPool {
   public:
    explicit ThreadPool(size_t num_threads) : stop_(false), active_(0) {
        for (size_t i = 0; i < num_threads; i++) {
            workers_.emplace_back([this] { worker_loop(); });
        }
    }

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
    void worker_loop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lk(mu_);
                cv_work_.wait(lk, [this] { return stop_ || !tasks_.empty(); });
                if (stop_ && tasks_.empty()) return;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            task();
            {
                std::lock_guard<std::mutex> lk(mu_);
                active_--;
            }
            cv_done_.notify_one();
        }
    }

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
