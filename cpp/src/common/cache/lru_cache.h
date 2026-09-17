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
#include <limits>
#include <list>
#include <stdexcept>
#include <unordered_map>
#include <utility>

namespace common {

template <typename K, typename V>
struct KeyValuePair {
   public:
    K key;
    V value;

    KeyValuePair(K k, V v) : key(std::move(k)), value(std::move(v)) {}
};

/**
 * Least-recently-used cache.
 *
 * The cache is not internally synchronized. A caller that shares one cache
 * across threads must serialize every operation, including the full lifetime
 * of a pointer/reference returned by getPtr(), getRef(), or tryGetRef().
 *
 * maxSize is the normal retained-entry limit. elasticity allows the cache to
 * grow temporarily to maxSize + elasticity; the insertion that would exceed
 * that hard limit prunes least-recently-used entries back to maxSize. A
 * maxSize of 0 means unbounded and elasticity is ignored.
 *
 * Pointers/references returned by non-copying lookups remain valid until that
 * entry is updated, removed, or evicted, or until clear()/destruction. Any
 * insertion can evict an entry when the cache is bounded.
 */
template <class Key, class Value,
          class Map = std::unordered_map<
              Key, typename std::list<KeyValuePair<Key, Value>>::iterator>>
class Cache {
   public:
    typedef KeyValuePair<Key, Value> node_type;
    typedef std::list<KeyValuePair<Key, Value>> list_type;
    typedef Map map_type;

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
            iter->second->value = std::move(v);
            entries_.splice(entries_.begin(), entries_, iter->second);
            return;
        }

        entries_.emplace_front(k, std::move(v));
        cache_[k] = entries_.begin();
        prune();
    }

    /** Backward-compatible copying lookup. */
    bool tryGet(const Key& k, Value& vOut) { return tryGetCopy(k, vOut); }

    bool tryGetCopy(const Key& k, Value& vOut) {
        const Value* value = getPtr(k);
        if (value == nullptr) {
            return false;
        }
        vOut = *value;
        return true;
    }

    /**
     * Non-copying lookup. On success vOut points at the cached value and the
     * entry is promoted to most-recently-used.
     */
    bool tryGetRef(const Key& k, const Value*& vOut) {
        vOut = getPtr(k);
        return vOut != nullptr;
    }

    /**
     * Legacy overload retained for source compatibility. Despite its historic
     * name it copies; new code should use the pointer overload or getPtr().
     */
    bool tryGetRef(const Key& k, Value& vOut) { return tryGetCopy(k, vOut); }

    /** Returns nullptr on miss and promotes a hit without copying the value. */
    const Value* getPtr(const Key& k) {
        const auto iter = cache_.find(k);
        if (iter == cache_.end()) {
            return nullptr;
        }
        entries_.splice(entries_.begin(), entries_, iter->second);
        return &iter->second->value;
    }

    const Value& getRef(const Key& k) {
        const Value* value = getPtr(k);
        if (value == nullptr) {
            throw std::out_of_range("LRU cache key not found");
        }
        return *value;
    }

    /** Backward-compatible copying lookup. */
    Value get(const Key& k) { return getCopy(k); }

    Value getCopy(const Key& k) { return getRef(k); }

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
    size_t getMaxAllowedSize() const {
        if (maxSize_ == 0) {
            return 0;
        }
        const size_t max = std::numeric_limits<size_t>::max();
        if (elasticity_ > max - maxSize_) {
            return max;
        }
        return maxSize_ + elasticity_;
    }

    template <typename F>
    void cwalk(F& f) const {
        std::for_each(entries_.begin(), entries_.end(), f);
    }

   protected:
    size_t prune() {
        if (maxSize_ == 0 || cache_.size() <= getMaxAllowedSize()) {
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
    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    Map cache_;
    list_type entries_;
    size_t maxSize_;
    size_t elasticity_;
};
}  // namespace common

#endif  // COMMON_CACHE_LRU_CACHE_H
