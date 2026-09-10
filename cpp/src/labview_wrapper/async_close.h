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

#ifndef SRC_LABVIEW_WRAPPER_ASYNC_CLOSE_H_
#define SRC_LABVIEW_WRAPPER_ASYNC_CLOSE_H_

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include "tsfile_labview.h"

namespace labview {

class AsyncCloseTask {
   public:
    LV_Status Wait();

   private:
    friend class AsyncCloseCoordinator;

    void Complete(LV_Status status);

    std::mutex mutex_;
    std::condition_variable completed_;
    bool done_ = false;
    LV_Status status_ = 0;
};

class AsyncCloseCoordinator {
   public:
    using CloseAction = std::function<LV_Status()>;

    AsyncCloseCoordinator() = default;
    ~AsyncCloseCoordinator();

    AsyncCloseCoordinator(const AsyncCloseCoordinator&) = delete;
    AsyncCloseCoordinator& operator=(const AsyncCloseCoordinator&) = delete;

    LV_Status Submit(CloseAction action,
                     std::shared_ptr<AsyncCloseTask>* out_task);

   private:
    void JoinWorker();

    std::mutex submission_mutex_;
    std::thread worker_;
};

}  // namespace labview

#endif  // SRC_LABVIEW_WRAPPER_ASYNC_CLOSE_H_
