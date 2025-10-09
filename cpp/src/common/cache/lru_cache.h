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

#ifndef COMMON_CACHE_LRU_CACHE_H
#define COMMON_CACHE_LRU_CACHE_H

#include <algorithm>
#include <list>
#include <unordered_map>

#include "utils/errno_define.h"

namespace common {

template <typename K, typename V>
struct KeyValuePair {
   public:
    K key;
    V value;

    KeyValuePair(K k, V v) : key(std::move(k)), value(std::move(v)) {}
};

/**
 *	The LRU Cache class templated by
 *		Key - key type
 *		Value - value type
 *		MapType - an associative container like std::unordered_map
 */
template <class Key, class Value,
          class Map = std::unordered_map<
              Key, typename std::list<KeyValuePair<Key, Value>>::iterator>>
class Cache {
   public:
    typedef KeyValuePair<Key, Value> node_type;
    typedef std::list<KeyValuePair<Key, Value>> list_type;
    typedef Map map_type;
    /**
     * the maxSize is the soft limit of entries and (maxSize + elasticity) is
     * the hard limit the cache is allowed to grow till (maxSize + elasticity)
     * and is pruned back to maxSize entries set maxSize = 0 for an unbounded
     * cache (but in that case, you're better off using a std::unordered_map
     * directly anyway! :)
     */
    explicit Cache(size_t maxSize = 64, size_t elasticity = 10)
        : maxSize_(maxSize), elasticity_(elasticity) {}
    virtual ~Cache() = default;
    size_t size() const { return cache_.size(); }
    bool empty() const { return cache_.empty(); }
    void clear() {
        cache_.clear();
        entries_.clear();
    }
    void insert(const Key& k, Value v) {
        const auto iter = cache_.find(k);
        if (iter != cache_.end()) {
            iter->second->value = v;
            entries_.splice(entries_.begin(), entries_, iter->second);
            return;
        }

        entries_.emplace_front(k, std::move(v));
        cache_[k] = entries_.begin();
        prune();
    }
    /**
      for backward compatibity. redirects to tryGetCopy()
     */
    bool tryGet(const Key& kIn, Value& vOut) { return tryGetCopy(kIn, vOut); }

    bool tryGetCopy(const Key& kIn, Value& vOut) {
        Value tmp;
        if (!tryGetRef_nolock(kIn, tmp)) {
            return false;
        }
        vOut = tmp;
        return true;
    }

    bool tryGetRef(const Key& kIn, Value& vOut) {
        return tryGetRef_nolock(kIn, vOut);
    }
    /**
     *	The const reference returned here is only
     *    guaranteed to be valid till the next insert/delete
     *  in multi-threaded apps use getCopy() to be threadsafe
     */
    const Value& getRef(const Key& k) { return get_nolock(k); }

    /**
        added for backward compatibility
     */
    Value get(const Key& k) { return getCopy(k); }
    /**
     * returns a copy of the stored object (if found)
     * safe to use/recommended in multi-threaded apps
     */
    Value getCopy(const Key& k) { return get_nolock(k); }

    bool remove(const Key& k) {
        auto iter = cache_.find(k);
        if (iter == cache_.end()) {
            return false;
        }
        entries_.erase(iter->second);
        cache_.erase(iter);
        return true;
    }
    bool contains(const Key& k) const { return cache_.find(k) != cache_.end(); }

    size_t getMaxSize() const { return maxSize_; }
    size_t getElasticity() const { return elasticity_; }
    size_t getMaxAllowedSize() const { return maxSize_ + elasticity_; }
    template <typename F>
    void cwalk(F& f) const {
        std::for_each(entries_.begin(), entries_.end(), f);
    }

   protected:
    bool tryGetRef_nolock(const Key& kIn, Value& vOut) {
        const auto iter = cache_.find(kIn);
        if (iter == cache_.end()) {
            return false;
        }
        entries_.splice(entries_.begin(), entries_, iter->second);
        vOut = iter->second->value;
        return true;
    }
    size_t prune() {
        size_t maxAllowed = maxSize_ + elasticity_;
        if (maxSize_ == 0 || cache_.size() < maxAllowed) {
            return 0;
        }
        size_t count = 0;
        while (cache_.size() > maxSize_) {
            cache_.erase(entries_.back().key);
            entries_.pop_back();
            ++count;
        }
        return count;
    }

   private:
    // Disallow copying.
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    Map cache_;
    list_type entries_;
    size_t maxSize_;
    size_t elasticity_;
};
}  // namespace common

#endif  // COMMON_CACHE_LRU_CACHE_H