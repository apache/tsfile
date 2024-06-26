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
// #include "seq_tvlist.h"
#include <stdio.h>
#include <string.h>
#include <iostream>
#include "common/mutex/mutex.h"
#include "common/logger/elog.h"


namespace storage
{

template<typename Type>
int SeqTVList<Type>::init(int32_t primary_array_size,
                          int32_t max_count,
                          bool use_page_arena)
{
  if (primary_array_size > max_count) {
    //common:://log_err("TVList init error, primary_array_size=%u, max_count=%u", primary_array_size, max_count);
    return common::E_INVALID_ARG;
  }
  use_page_arena_ = use_page_arena;

  primary_array_size_ = primary_array_size;
  list_size_ = (max_count / primary_array_size_) +
               (max_count % primary_array_size_ == 0 ? 0 : 1);

  int32_t alloc_size = sizeof(TV) * list_size_;
  tv_array_list_ = (TV**)alloc(alloc_size);
  if (tv_array_list_ == nullptr) {
    return common::E_OOM;
  }
  memset(tv_array_list_, 0, alloc_size);
  write_count_ = 0;
  if (use_page_arena_) {
    // TODO make it configurable
    page_arena_.init(sizeof(TV) * primary_array_size_ * 4, common::MOD_TVLIST_OBJ);
  }
  return common::E_OK;
}

template<typename Type>
int SeqTVList<Type>::push(int64_t time, Type value)
{
  common::MutexGuard mg(mutex_);
  return push_without_lock(time, value);
};

template<typename Type>
int SeqTVList<Type>::push_without_lock(int64_t time, Type value)
{
  if (UNLIKELY(time <= last_time_)) {
    return common::E_OUT_OF_ORDER;
  }
  if (UNLIKELY(write_count_ >= list_size_ * primary_array_size_)) {
    return common::E_OVERFLOW;
  }

  int32_t list_idx = write_count_ / primary_array_size_;
  int32_t list_offset = write_count_ % primary_array_size_;
  if (UNLIKELY(list_offset == 0)) {
    ASSERT(tv_array_list_[list_idx] == nullptr);
    tv_array_list_[list_idx] = static_cast<TV*>(alloc(sizeof(TV) * primary_array_size_));
    if (UNLIKELY(tv_array_list_[list_idx] == nullptr)) {
      return common::E_OOM;
    }
  }

  TV insert_tv;
  insert_tv.time_ = time;
  insert_tv.value_ = value;
#if STORAGE_ENGINE_DEBUG
  std::cout << "tvlist[" << list_idx << "][" << list_offset << "] = (" << time << ", " << value << ")" << std::endl;
#endif
  tv_array_list_[list_idx][list_offset] = insert_tv;
  write_count_++;
  last_time_ = time;
  return common::E_OK;
};

template<typename Type>
void SeqTVList<Type>::destroy()
{
  if (use_page_arena_) {
    page_arena_.destroy();
  } else {
    int32_t list_size = write_count_ / primary_array_size_
                        + (write_count_ % primary_array_size_ == 0 ? 0 : 1);
    for (int i = 0; i < list_size; i++) {
      common::mem_free(tv_array_list_[i]);
    }
    common::mem_free(tv_array_list_);
  }
}

template<typename Type>
typename SeqTVList<Type>::Iterator SeqTVList<Type>::scan_without_lock(int64_t start_time, int64_t end_time)
{
  ASSERT(start_time < end_time);
  int32_t start_idx = binary_search_lower(start_time);
  int32_t end_idx = binary_search_upper(end_time);
  ASSERT(start_idx <= end_time + 1);
  SeqTVList::Iterator iter;
  iter.init(this, start_idx, end_idx);
  return iter;
}

template<typename Type>
typename SeqTVList<Type>::Iterator SeqTVList<Type>::scan_without_lock()
{
  SeqTVList::Iterator iter;
  iter.init(this, 0, write_count_);
  return iter;
}

// return the first tv which is larger or equal to @time
template<typename Type>
int32_t SeqTVList<Type>::binary_search_lower(int64_t time)
{
  int32_t start = -1;
  int32_t end = write_count_;

  // arr[start] < time <= arr[end]
  while (start + 1 != end) {
    int mid = (start + end) / 2;
    int64_t mid_time = time_at(mid);
    if (mid_time < time) {
      start = mid;
    } else {
      end = mid;
    }
  }
  return end;
}

// return the last tv which is less or equal to @time
template<typename Type>
int32_t SeqTVList<Type>::binary_search_upper(int64_t time)
{
  int32_t start = 0;
  int32_t end = write_count_;

  // arr[start] <= time < arr[end]
  while (start + 1 != end) {
    int mid = (start + end) / 2;
    int64_t mid_time = time_at(mid);
    if (mid_time <= time) {
      start = mid;
    } else {
      end = mid;
    }
  }
  return start;
}

} // namepsace storage

