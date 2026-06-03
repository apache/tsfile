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

#ifndef COMMON_MUTEX_MUTEX_H
#define COMMON_MUTEX_MUTEX_H

#include <mutex>

#include "utils/util_define.h"

namespace common {

// Thin wrapper over std::mutex. Implemented with the C++11 standard library
// (instead of pthreads directly) so it builds on every platform, including
// MSVC where pthreads is not available.
class Mutex {
   public:
    Mutex() {}
    ~Mutex() {}

    void lock() { mutex_.lock(); }

    void unlock() { mutex_.unlock(); }

    bool try_lock() { return mutex_.try_lock(); }

   private:
    std::mutex mutex_;
};

class MutexGuard {
   public:
    MutexGuard(Mutex& m) : m_(m) { m_.lock(); }
    ~MutexGuard() { m_.unlock(); }

   private:
    Mutex& m_;
};

}  // end namespace common
#endif  // COMMON_MUTEX_MUTEX_H
