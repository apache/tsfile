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
#include "page_arena.h"

#include <stdio.h>

#include <new>

namespace common {

char *PageArena::alloc(uint32_t size) {
    if (LIKELY(size <= page_size_)) {
        Page *cur_page = dummy_head_.next_;
        if (LIKELY(cur_page != nullptr)) {
            char *ret_ptr = cur_page->alloc(size);
            if (LIKELY(ret_ptr != nullptr)) {
                return ret_ptr;
            }
        }

        /*
         * cur_page is null OR can not alloc @size from cur_page,
         * then alloc new page
         */
        void *ptr = base_allocator_.alloc(page_size_ + sizeof(Page), mid_);
        if (UNLIKELY(ptr == nullptr)) {
            return nullptr;
        }
        Page *new_page = new (ptr) Page(page_size_, cur_page);
        dummy_head_.next_ = new_page;
        return new_page->alloc(size);
    } else {
        void *ptr = base_allocator_.alloc(size + sizeof(Page), mid_);
        Page *new_page = new (ptr) Page(dummy_head_.next_);
        dummy_head_.next_ = new_page;
        return reinterpret_cast<char *>(new_page) + sizeof(Page);
    }
}

void PageArena::reset() {
    Page *p = dummy_head_.next_;
    while (p) {
        dummy_head_.next_ = p->next_;
        base_allocator_.free(p);
        p = dummy_head_.next_;
    }
}

}  // end namespace common