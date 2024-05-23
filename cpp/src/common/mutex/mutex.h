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

#include <errno.h>
#include <pthread.h>

#include "utils/util_define.h"

namespace common {

class Mutex {
   public:
    Mutex() : mutex_() { pthread_mutex_init(&mutex_, NULL); }
    ~Mutex() { pthread_mutex_destroy(&mutex_); }

    void lock() {
        int ret = EBUSY;
        do {
            ret = pthread_mutex_lock(&mutex_);
        } while (UNLIKELY(ret == EBUSY || ret == EAGAIN));
        ASSERT(ret == 0);
    }

    void unlock() {
        int ret = pthread_mutex_unlock(&mutex_);
        ASSERT(ret == 0);
    }

    bool try_lock() {
        int ret = pthread_mutex_trylock(&mutex_);
        if (ret == 0) {
            return true;
        } else if (ret == EBUSY || ret == EAGAIN) {
            return false;
        } else {
            ASSERT(false);
            return false;
        }
    }

   private:
    pthread_mutex_t mutex_;
};

class MutexGuard {
   public:
    MutexGuard(Mutex &m) : m_(m) { m_.lock(); }
    ~MutexGuard() { m_.unlock(); }

   private:
    Mutex &m_;
};

}  // end namespace common
#endif  // COMMON_MUTEX_MUTEX_H
