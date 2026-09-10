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

#include "async_close.h"

#include <new>
#include <utility>

#include "cwrapper/errno_define_c.h"

namespace labview {
namespace {

LV_Status RunAction(const AsyncCloseCoordinator::CloseAction& action) {
    try {
        return action();
    } catch (const std::bad_alloc&) {
        return RET_OOM;
    } catch (...) {
        return RET_FILE_CLOSE_ERR;
    }
}

}  // namespace

LV_Status AsyncCloseTask::Wait() {
    std::unique_lock<std::mutex> lock(mutex_);
    completed_.wait(lock, [this] { return done_; });
    return status_;
}

void AsyncCloseTask::Complete(LV_Status status) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        status_ = status;
        done_ = true;
    }
    completed_.notify_all();
}

AsyncCloseCoordinator::~AsyncCloseCoordinator() {
    std::lock_guard<std::mutex> lock(submission_mutex_);
    JoinWorker();
}

LV_Status AsyncCloseCoordinator::Submit(
    CloseAction action, std::shared_ptr<AsyncCloseTask>* out_task) {
    if (!action || out_task == nullptr) {
        return RET_INVALID_ARG;
    }
    out_task->reset();

    std::lock_guard<std::mutex> lock(submission_mutex_);
    JoinWorker();

    std::shared_ptr<AsyncCloseTask> task;
    try {
        task = std::make_shared<AsyncCloseTask>();
        worker_ =
            std::thread([task, action] { task->Complete(RunAction(action)); });
    } catch (...) {
        return RunAction(action);
    }

    *out_task = std::move(task);
    return RET_OK;
}

void AsyncCloseCoordinator::JoinWorker() {
    if (worker_.joinable()) {
        worker_.join();
    }
}

}  // namespace labview
