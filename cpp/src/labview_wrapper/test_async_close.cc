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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <memory>
#include <mutex>
#include <thread>

#include "async_close.h"

#define CHECK(condition)                                              \
    do {                                                              \
        if (!(condition)) {                                           \
            std::fprintf(stderr, "FAIL: %s at line %d\n", #condition, \
                         __LINE__);                                   \
            return false;                                             \
        }                                                             \
    } while (0)

namespace {

class Gate {
   public:
    void Open() {
        std::lock_guard<std::mutex> lock(mutex_);
        open_ = true;
        condition_.notify_all();
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        condition_.wait(lock, [this] { return open_; });
    }

   private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool open_ = false;
};

void update_max(std::atomic<int>* maximum, int value) {
    int observed = maximum->load();
    while (observed < value &&
           !maximum->compare_exchange_weak(observed, value)) {
    }
}

bool test_submissions_are_serialized() {
    labview::AsyncCloseCoordinator coordinator;
    std::shared_ptr<labview::AsyncCloseTask> first;
    std::shared_ptr<labview::AsyncCloseTask> second;
    Gate first_started;
    Gate release_first;
    Gate second_submit_started;
    std::atomic<int> running(0);
    std::atomic<int> max_running(0);
    std::atomic<bool> second_submit_done(false);
    LV_Status second_submit_status = -1;

    CHECK(coordinator.Submit(
              [&] {
                  const int active = ++running;
                  update_max(&max_running, active);
                  first_started.Open();
                  release_first.Wait();
                  --running;
                  return static_cast<LV_Status>(11);
              },
              &first) == 0);
    first_started.Wait();

    std::thread submitter([&] {
        second_submit_started.Open();
        second_submit_status = coordinator.Submit(
            [&] {
                const int active = ++running;
                update_max(&max_running, active);
                --running;
                return static_cast<LV_Status>(22);
            },
            &second);
        second_submit_done.store(true);
    });

    second_submit_started.Wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(!second_submit_done.load());
    release_first.Open();
    submitter.join();

    CHECK(second_submit_status == 0);
    CHECK(first != nullptr);
    CHECK(second != nullptr);
    CHECK(first->Wait() == 11);
    CHECK(second->Wait() == 22);
    CHECK(max_running.load() == 1);
    return true;
}

bool test_destructor_joins_last_worker() {
    std::unique_ptr<labview::AsyncCloseCoordinator> coordinator(
        new labview::AsyncCloseCoordinator());
    std::shared_ptr<labview::AsyncCloseTask> task;
    Gate action_started;
    Gate release_action;
    Gate destructor_started;
    std::atomic<bool> destructor_done(false);

    CHECK(coordinator->Submit(
              [&] {
                  action_started.Open();
                  release_action.Wait();
                  return static_cast<LV_Status>(33);
              },
              &task) == 0);
    action_started.Wait();

    std::thread destroyer([&] {
        destructor_started.Open();
        coordinator.reset();
        destructor_done.store(true);
    });
    destructor_started.Wait();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    CHECK(!destructor_done.load());
    release_action.Open();
    destroyer.join();

    CHECK(destructor_done.load());
    CHECK(task->Wait() == 33);
    return true;
}

}  // namespace

int main() {
    if (!test_submissions_are_serialized()) {
        return 1;
    }
    if (!test_destructor_joins_last_worker()) {
        return 1;
    }
    return 0;
}
