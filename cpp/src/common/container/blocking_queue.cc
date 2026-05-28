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

#include "blocking_queue.h"

namespace common {

BlockingQueue::BlockingQueue() : queue_(), mutex_(), cond_() {}

BlockingQueue::~BlockingQueue() {}

void BlockingQueue::push(void* data) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(data);
    }
    cond_.notify_one();
}

void* BlockingQueue::pop() {
    std::unique_lock<std::mutex> lock(mutex_);
    while (queue_.empty()) {
        cond_.wait(lock);
    }
    void* ret_data = queue_.front();
    queue_.pop();
    return ret_data;
}

}  // end namespace common
