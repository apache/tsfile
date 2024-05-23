#ifndef COMMON_CONTAINER_HASH_TABLE_H
#define COMMON_CONTAINER_HASH_TABLE_H

#include "common/allocator/alloc_base.h"
#include "common/container/array.h"
#include "common/container/hash_func.h"
#include "common/container/hash_node.h"
#include "common/container/hash_segm.h"

#define TABLE_CAPACITY 100
#define TABLE_GROWTH 2

namespace common {

template <typename KeyType, typename ValueType, typename F>
class HashTable {
   public:
    HashTable() : table_(TABLE_CAPACITY) {
        new_table_capacity_ = TABLE_CAPACITY;
        old_table_capacity_ = TABLE_CAPACITY;
        size_ = 0;
        is_inited_ = false;
        hash_version_ = 0;
        is_extending_ = 0;
    }

    HashTable(size_t total_capacity)
        : table_(total_capacity / SEGMENT_CAPACITY) {
        new_table_capacity_ = total_capacity / SEGMENT_CAPACITY;
        old_table_capacity_ = new_table_capacity_;
        size_ = 0;
        is_inited_ = false;
        hash_version_ = 0;
        is_extending_ = 0;
    }
    ~HashTable() {
        new_table_capacity_ = 0;
        size_ = 0;
        is_inited_ = false;
        hash_version_ = 0;
        is_extending_ = 0;
    }

    int init() {
        if (is_inited_) {
            // log_err("init repeated.");
            ASSERT(!is_inited_);
        }

        int ret = E_OK;
        ret = table_.init();
        if (ret != E_OK) {
            return E_OOM;
        }
        for (size_t i = 0; i < new_table_capacity_; i++) {
            void* buf =
                mem_alloc(sizeof(HashSegm<KeyType, ValueType>), MOD_HASH_TABLE);
            if (UNLIKELY(nullptr == buf)) {
                // log_err("malloc() failed.");
                return E_OOM;
            }
            HashSegm<KeyType, ValueType>* tmp_segm =
                new (buf) HashSegm<KeyType, ValueType>;
            table_.append(tmp_segm);
        }
        is_inited_ = true;
        return E_OK;
    }

    void destroy() {
        if (is_inited_) {
            for (size_t i = 0; i < new_table_capacity_; i++) {
                for (size_t j = 0; j < SEGMENT_CAPACITY; j++) {
                    if ((*table_[i])[j].get_valid()) {
                        HashNode<KeyType, ValueType>* prev = nullptr;
                        HashNode<KeyType, ValueType>* entry =
                            (*table_[i])[j].get_next();
                        while (entry != nullptr) {
                            prev = entry;
                            entry = entry->get_next();
                            prev->~HashNode();
                            mem_free(prev);
                            prev = nullptr;
                        }
                        (*table_[i])[j].set_valid(false);
                    }
                }
                table_[i]->~HashSegm();
                mem_free(table_[i]);
                table_[i] = nullptr;
            }
            table_.destroy();
        }
        is_inited_ = false;
    }

    int table_extend() {
        int ret = table_.extend();
        if (ret != E_OK) {
            return ret;
        }
        for (size_t i = ATOMIC_LOAD(&new_table_capacity_);
             i < table_.capacity(); i++) {
            void* buf =
                mem_alloc(sizeof(HashSegm<KeyType, ValueType>), MOD_HASH_TABLE);
            if (UNLIKELY(nullptr == buf)) {
                // log_err("malloc() failed.");
                return E_OOM;
            }
            HashSegm<KeyType, ValueType>* tmp_segm =
                new (buf) HashSegm<KeyType, ValueType>;
            table_.append(tmp_segm);
        }
        return E_OK;
    }

    int move_bucket_head(size_t from_segment_idx, size_t from_bucket_idx) {
        HashNode<KeyType, ValueType>* prev =
            &((*table_[from_segment_idx])[from_bucket_idx]);
        HashNode<KeyType, ValueType>* entry =
            ((*table_[from_segment_idx])[from_bucket_idx]).get_next();

        while (
            (*table_[from_segment_idx])[from_bucket_idx].get_rehashid() !=
            ATOMIC_LOAD(&hash_version_)) {  // if the hashnode hasn't rehashed
            uint32_t hash_value = hashfunc_(
                (*table_[from_segment_idx])[from_bucket_idx].get_key());
            size_t to_segment_idx =
                hash_value % ATOMIC_LOAD(&new_table_capacity_);
            size_t to_bucket_idx = hash_value % SEGMENT_CAPACITY;
            if (from_segment_idx == to_segment_idx &&
                from_bucket_idx == to_bucket_idx) {
                ((*table_[to_segment_idx])[to_bucket_idx])
                    .set_rehashid(
                        ATOMIC_LOAD(&hash_version_));  // only update rehashid
            } else {
                // put
                table_[to_segment_idx]->mutexes_[to_bucket_idx].lock();
                int ret = put_nolock(
                    (*table_[from_segment_idx])[from_bucket_idx].get_key(),
                    (*table_[from_segment_idx])[from_bucket_idx].get_value(),
                    to_segment_idx, to_bucket_idx);
                table_[to_segment_idx]->mutexes_[to_bucket_idx].unlock();
                if (ret != E_OK) {
                    return ret;
                }
                // remove
                if (entry != nullptr) {
                    ((*table_[from_segment_idx])[from_bucket_idx])
                        .set_key(entry->get_key());
                    ((*table_[from_segment_idx])[from_bucket_idx])
                        .set_value(entry->get_value());
                    ((*table_[from_segment_idx])[from_bucket_idx])
                        .set_next(entry->get_next());
                    ((*table_[from_segment_idx])[from_bucket_idx])
                        .set_valid(true);
                    ((*table_[from_segment_idx])[from_bucket_idx])
                        .set_rehashid(entry->get_rehashid());
                    ATOMIC_AAF(&size_, -1);
                    entry->~HashNode();
                    mem_free(entry);
                    entry = prev->get_next();
                } else {
                    ((*table_[from_segment_idx])[from_bucket_idx])
                        .set_valid(false);
                    ATOMIC_AAF(&size_, -1);
                    break;
                }
            }
        }

        return E_OK;
    }

    int move_bucket_chain(size_t from_segment_idx, size_t from_bucket_idx) {
        HashNode<KeyType, ValueType>* prev =
            &((*table_[from_segment_idx])[from_bucket_idx]);
        HashNode<KeyType, ValueType>* entry =
            ((*table_[from_segment_idx])[from_bucket_idx]).get_next();

        while (entry != nullptr) {
            if (entry->get_rehashid() != ATOMIC_LOAD(&hash_version_)) {
                uint32_t hash_value = hashfunc_(entry->get_key());
                size_t to_segment_idx =
                    hash_value % ATOMIC_LOAD(&new_table_capacity_);
                size_t to_bucket_idx = hash_value % SEGMENT_CAPACITY;
                if (from_segment_idx == to_segment_idx &&
                    from_bucket_idx == to_bucket_idx) {
                    entry->set_rehashid(ATOMIC_LOAD(&hash_version_));
                    prev = entry;
                    entry = entry->get_next();
                } else {
                    table_[to_segment_idx]->mutexes_[to_bucket_idx].lock();
                    if ((*table_[to_segment_idx])[to_bucket_idx].get_valid() ==
                        false) {
                        ((*table_[to_segment_idx])[to_bucket_idx])
                            .set_key(entry->get_key());
                        ((*table_[to_segment_idx])[to_bucket_idx])
                            .set_value(entry->get_value());
                        ((*table_[to_segment_idx])[to_bucket_idx])
                            .set_next(nullptr);
                        ((*table_[to_segment_idx])[to_bucket_idx])
                            .set_valid(true);
                        ((*table_[to_segment_idx])[to_bucket_idx])
                            .set_rehashid(ATOMIC_LOAD(&hash_version_));
                        prev->set_next(entry->get_next());
                        entry->~HashNode();
                        mem_free(entry);
                        entry = prev->get_next();
                        // entry = entry->get_next();
                    } else {
                        entry->set_rehashid(ATOMIC_LOAD(&hash_version_));
                        prev->set_next(entry->get_next());
                        entry->set_next((*table_[to_segment_idx])[to_bucket_idx]
                                            .get_next());
                        (*table_[to_segment_idx])[to_bucket_idx].set_next(
                            entry);
                        entry = prev->get_next();
                    }
                    table_[to_segment_idx]->mutexes_[to_bucket_idx].unlock();
                }
            } else {
                prev = prev->get_next();
                entry = entry->get_next();
            }
        }
        return E_OK;
    }

    int extend() {
        int ret = table_extend();
        if (ret != E_OK) {
            return ret;
        }
        ATOMIC_STORE(&old_table_capacity_, ATOMIC_LOAD(&new_table_capacity_));
        ATOMIC_STORE(&new_table_capacity_, table_.capacity());
        // ATOMIC_STORE(&hash_total_capacity_, table_.capacity() *
        // SEGMENT_CAPACITY);

        int64_t next_hash_version =
            ATOMIC_LOAD(&hash_version_) +
            1;  // the highest position of next_hash_version is 0
        ATOMIC_STORE(&hash_version_,
                     next_hash_version |
                         (1UL << 63));  // set the highest position to 1
                                        // which means 'extend' is beginning.

        // rehash
        for (size_t from_segment_idx = 0;
             from_segment_idx < ATOMIC_LOAD(&old_table_capacity_);
             from_segment_idx++) {
            for (size_t from_bucket_idx = 0; from_bucket_idx < SEGMENT_CAPACITY;
                 from_bucket_idx++) {
                table_[from_segment_idx]->mutexes_[from_bucket_idx].lock();
                if ((*table_[from_segment_idx])[from_bucket_idx].get_valid()) {
                    ret = move_bucket_head(from_segment_idx, from_bucket_idx);
                    if (ret != E_OK) {
                        table_[from_segment_idx]
                            ->mutexes_[from_bucket_idx]
                            .unlock();
                        return ret;
                    }
                    ret = move_bucket_chain(from_segment_idx, from_bucket_idx);
                    if (ret != E_OK) {
                        table_[from_segment_idx]
                            ->mutexes_[from_bucket_idx]
                            .unlock();
                        return ret;
                    }
                }
                table_[from_segment_idx]->mutexes_[from_bucket_idx].unlock();
            }
        }

        ATOMIC_STORE(
            &hash_version_,
            next_hash_version & (~(1UL << 63)));  // set extending flag is false

        return E_OK;
    }

    /*
     * when the key already exists, just update old value to new value;
     */
    int put(const KeyType& key, const ValueType& value) {
        int ret;
        uint32_t hash_value = hashfunc_(key);

        while (true) {
            uint8_t cur_table_capacity = ATOMIC_LOAD(&new_table_capacity_);
            size_t segment_idx = hash_value % cur_table_capacity;
            size_t bucket_idx = hash_value % SEGMENT_CAPACITY;

            table_[segment_idx]->mutexes_[bucket_idx].lock();
            ret = put_nolock(key, value, segment_idx, bucket_idx);
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            if (ret != E_OK) {
                return ret;
            }

            if (UNLIKELY(
                    cur_table_capacity !=
                    ATOMIC_LOAD(
                        &new_table_capacity_))) {  // resolve the problem of
                                                   // insert into old position
                ret = remove_from_wrong_bucket(key, segment_idx, bucket_idx);
                // if (ret != E_OK) {
                //   return ret;
                // }
                continue;
            } else {
                break;
            }
        }

        if (ATOMIC_LOAD(&size_) > ATOMIC_LOAD(&new_table_capacity_) *
                                      SEGMENT_CAPACITY * TABLE_GROWTH) {
            int tmp = 0;
            if (ATOMIC_CAS(&is_extending_, &tmp, 1)) {
                ret = extend();
                if (ret != E_OK) {
                    ATOMIC_AAF(&is_extending_, -1);
                    return ret;
                }
                ATOMIC_AAF(&is_extending_, -1);
            }
        }

        return ret;
    }

    /*
     * when the key already exists, just update old value to new value;
     */
    int put_nolock(const KeyType& key, const ValueType& value,
                   size_t segment_idx, size_t bucket_idx) {
        if (((*table_[segment_idx])[bucket_idx]).get_valid() ==
            false) {  // if current segment has no valid node
            ((*table_[segment_idx])[bucket_idx]).set_key(key);
            ((*table_[segment_idx])[bucket_idx]).set_value(value);
            ((*table_[segment_idx])[bucket_idx]).set_next(nullptr);
            ((*table_[segment_idx])[bucket_idx]).set_valid(true);
            ((*table_[segment_idx])[bucket_idx])
                .set_rehashid(ATOMIC_LOAD(&hash_version_));
            ATOMIC_AAF(&size_, 1);
        } else if (((*table_[segment_idx])[bucket_idx]).get_key() ==
                   key) {  // if current segment's first node's key is same as
                           // the target key
            ((*table_[segment_idx])[bucket_idx])
                .set_value(value);  // update value of existing node
            ((*table_[segment_idx])[bucket_idx])
                .set_rehashid(ATOMIC_LOAD(&hash_version_));
        } else {  // follow the link list
            HashNode<KeyType, ValueType>* prev =
                &((*table_[segment_idx])[bucket_idx]);
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            while (entry != nullptr && entry->get_key() != key) {
                prev = entry;
                entry = entry->get_next();
            }
            if (LIKELY(entry == nullptr)) {
                void* buf = mem_alloc(sizeof(HashNode<KeyType, ValueType>),
                                      MOD_HASH_TABLE);
                if (UNLIKELY(nullptr == buf)) {
                    // log_err("malloc() failed.");
                    return E_OOM;
                }
                entry = new (buf) HashNode<KeyType, ValueType>;
                entry->set_key(key);
                entry->set_value(value);
                entry->set_valid(true);
                entry->set_rehashid(ATOMIC_LOAD(&hash_version_));
                prev->set_next(entry);
                ATOMIC_AAF(&size_, 1);
            } else {
                entry->set_value(value);  // update value of existing node
                entry->set_rehashid(ATOMIC_LOAD(&hash_version_));
            }
        }
        return E_OK;
    }

    /*
     * when the key already exists, then remain the old value and return the old
     * value and set 'exist' to true
     */
    int put_no_update(const KeyType& key, ValueType& value, bool& exist) {
        int ret;
        uint32_t hash_value = hashfunc_(key);

        while (true) {
            uint8_t cur_table_capacity = ATOMIC_LOAD(&new_table_capacity_);
            size_t segment_idx = hash_value % cur_table_capacity;
            size_t bucket_idx = hash_value % SEGMENT_CAPACITY;

            table_[segment_idx]->mutexes_[bucket_idx].lock();
            ret = put_nolock_no_update(key, value, segment_idx, bucket_idx,
                                       exist);
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            if (ret != E_OK) {
                return ret;
            }

            if (UNLIKELY(
                    cur_table_capacity !=
                    ATOMIC_LOAD(
                        &new_table_capacity_))) {  // resolve the problem of
                                                   // insert into old position
                ret = remove_from_wrong_bucket(key, segment_idx, bucket_idx);
                // if (ret != E_OK) {
                //   return ret;
                // }
                continue;
            } else {
                break;
            }
        }

        if (ATOMIC_LOAD(&size_) > ATOMIC_LOAD(&new_table_capacity_) *
                                      SEGMENT_CAPACITY * TABLE_GROWTH) {
            int tmp = 0;
            if (ATOMIC_CAS(&is_extending_, &tmp, 1)) {
                ret = extend();
                if (ret != E_OK) {
                    return ret;
                }
                ATOMIC_AAF(&is_extending_, -1);
            }
        }

        return ret;
    }

    /*
     * when the key already exists, then remain the old value and return the old
     * value and set 'exist' to true
     */
    int put_nolock_no_update(const KeyType& key, ValueType& value,
                             size_t segment_idx, size_t bucket_idx,
                             bool& exist) {
        if (((*table_[segment_idx])[bucket_idx]).get_valid() ==
            false) {  // if current segment has no valid node
            ((*table_[segment_idx])[bucket_idx]).set_key(key);
            ((*table_[segment_idx])[bucket_idx]).set_value(value);
            ((*table_[segment_idx])[bucket_idx]).set_next(nullptr);
            ((*table_[segment_idx])[bucket_idx]).set_valid(true);
            ((*table_[segment_idx])[bucket_idx])
                .set_rehashid(ATOMIC_LOAD(&hash_version_));
            ATOMIC_AAF(&size_, 1);
        } else if (((*table_[segment_idx])[bucket_idx]).get_key() ==
                   key) {  // if current segment's first node's key is same as
                           // the target key
            exist = true;
            value = ((*table_[segment_idx])[bucket_idx]).get_value();
            ((*table_[segment_idx])[bucket_idx])
                .set_rehashid(ATOMIC_LOAD(&hash_version_));
        } else {  // follow the link list
            HashNode<KeyType, ValueType>* prev =
                &((*table_[segment_idx])[bucket_idx]);
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            while (entry != nullptr && entry->get_key() != key) {
                prev = entry;
                entry = entry->get_next();
            }
            if (LIKELY(entry == nullptr)) {
                void* buf = mem_alloc(sizeof(HashNode<KeyType, ValueType>),
                                      MOD_HASH_TABLE);
                if (UNLIKELY(nullptr == buf)) {
                    // log_err("malloc() failed.");
                    return E_OOM;
                }
                entry = new (buf) HashNode<KeyType, ValueType>;
                entry->set_key(key);
                entry->set_value(value);
                entry->set_valid(true);
                entry->set_rehashid(ATOMIC_LOAD(&hash_version_));
                prev->set_next(entry);
                ATOMIC_AAF(&size_, 1);
            } else {
                exist = true;
                value = entry->get_value();
                entry->set_rehashid(ATOMIC_LOAD(&hash_version_));
            }
        }
        return E_OK;
    }

    int get(const KeyType& key, ValueType& result_value) {
        if (!ATOMIC_LOAD(&size_)) {
            return E_NOT_EXIST;
        }
        int ret;
        ValueType result1;
        ValueType result2;
        ValueType result3;
        while (true) {
            uint64_t hv1 = ATOMIC_LOAD(&hash_version_);

            if ((hv1 & (1UL << 63))) {
                ret = get_by_bucket(key, result1,
                                    ATOMIC_LOAD(&old_table_capacity_));
                if (ret == E_OK) {
                    result_value = result1;
                    return ret;
                }
                ret = get_by_bucket(key, result2,
                                    ATOMIC_LOAD(&new_table_capacity_));
                if (ret == E_OK) {
                    result_value = result2;
                    return ret;
                }
            } else {  //
                ret = get_by_bucket(key, result3,
                                    ATOMIC_LOAD(&new_table_capacity_));
                if (ret == E_OK) {
                    result_value = result3;
                    return ret;
                }
            }

            uint64_t hv2 = ATOMIC_LOAD(&hash_version_);
            if ((hv1 & (~(1UL << 63))) != (hv2 & (~(1UL << 63)))) {
                continue;
            } else if (ATOMIC_LOAD(&is_extending_)) {
                continue;
            } else {
                return E_NOT_EXIST;
            }
        }
    }

    int get_by_bucket(const KeyType& key, ValueType& result_value,
                      size_t table_capacity) {
        uint32_t hash_value = hashfunc_(key);
        size_t segment_idx = hash_value % table_capacity;
        size_t bucket_idx = hash_value % SEGMENT_CAPACITY;

        table_[segment_idx]->mutexes_[bucket_idx].lock();

        if (!((*table_[segment_idx])[bucket_idx])
                 .get_valid()) {  // if current segment has no node
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_NOT_EXIST;
        } else if (((*table_[segment_idx])[bucket_idx]).get_key() == key) {
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            result_value = ((*table_[segment_idx])[bucket_idx]).get_value();
            return E_OK;
        } else {
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            while (entry != nullptr) {
                if (entry->get_key() == key) {
                    table_[segment_idx]->mutexes_[bucket_idx].unlock();
                    result_value = entry->get_value();
                    return E_OK;
                }
                entry = entry->get_next();
            }
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_NOT_EXIST;
        }
    }

    int remove(const KeyType& key) {
        if (!ATOMIC_LOAD(&size_)) {
            return E_NOT_EXIST;
        }
        int ret;
        while (true) {
            uint64_t hv1 = ATOMIC_LOAD(&hash_version_);

            if ((hv1 & (1UL << 63))) {
                ret = remove_by_bucket(key, ATOMIC_LOAD(&old_table_capacity_));
                if (ret == E_OK) {
                    return ret;
                }
                ret = remove_by_bucket(key, ATOMIC_LOAD(&new_table_capacity_));
                if (ret == E_OK) {
                    return ret;
                }
            } else {
                ret = remove_by_bucket(key, ATOMIC_LOAD(&new_table_capacity_));
                if (ret == E_OK) {
                    return ret;
                }
            }

            uint64_t hv2 = ATOMIC_LOAD(&hash_version_);
            if ((hv1 & (~(1UL << 63))) != (hv2 & (~(1UL << 63)))) {
                continue;
            } else if (ATOMIC_LOAD(&is_extending_)) {
                continue;
            } else {
                return E_NOT_EXIST;
            }
        }
    }

    int remove_by_bucket(const KeyType& key, size_t table_capacity) {
        uint32_t hash_value = hashfunc_(key);
        size_t segment_idx = hash_value % table_capacity;
        size_t bucket_idx = hash_value % SEGMENT_CAPACITY;

        table_[segment_idx]->mutexes_[bucket_idx].lock();

        if (!((*table_[segment_idx])[bucket_idx])
                 .get_valid()) {  // if current segment has no node
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_NOT_EXIST;
        } else if (((*table_[segment_idx])[bucket_idx]).get_key() == key) {
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            if (entry != nullptr) {
                ((*table_[segment_idx])[bucket_idx]).set_key(entry->get_key());
                ((*table_[segment_idx])[bucket_idx])
                    .set_value(entry->get_value());
                ((*table_[segment_idx])[bucket_idx])
                    .set_next(entry->get_next());
                ((*table_[segment_idx])[bucket_idx]).set_valid(true);
                ((*table_[segment_idx])[bucket_idx])
                    .set_rehashid(entry->get_rehashid());
                entry->~HashNode();
                mem_free(entry);
                entry = nullptr;
            } else {
                ((*table_[segment_idx])[bucket_idx]).set_valid(false);
            }
            ATOMIC_AAF(&size_, -1);
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_OK;
        } else {
            HashNode<KeyType, ValueType>* prev =
                &((*table_[segment_idx])[bucket_idx]);
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            while (entry != nullptr) {
                if (entry->get_key() == key) {
                    prev->set_next(
                        entry->get_next());  // remove node and reconnect
                    entry->~HashNode();
                    mem_free(entry);
                    entry = nullptr;
                    ATOMIC_AAF(&size_, -1);
                    table_[segment_idx]->mutexes_[bucket_idx].unlock();
                    return E_OK;
                }
                prev = entry;
                entry = entry->get_next();
            }
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_NOT_EXIST;
        }
    }

    int remove_from_wrong_bucket(const KeyType& key, size_t segment_idx,
                                 size_t bucket_idx) {
        if (!ATOMIC_LOAD(&size_)) {
            return E_NO_MORE_DATA;
        }

        table_[segment_idx]->mutexes_[bucket_idx].lock();

        if (!((*table_[segment_idx])[bucket_idx])
                 .get_valid()) {  // if current segment has no node
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_NOT_EXIST;
        } else if (((*table_[segment_idx])[bucket_idx]).get_key() == key) {
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            if (entry != nullptr) {
                ((*table_[segment_idx])[bucket_idx]).set_key(entry->get_key());
                ((*table_[segment_idx])[bucket_idx])
                    .set_value(entry->get_value());
                ((*table_[segment_idx])[bucket_idx])
                    .set_next(entry->get_next());
                ((*table_[segment_idx])[bucket_idx]).set_valid(true);
                ((*table_[segment_idx])[bucket_idx])
                    .set_rehashid(entry->get_rehashid());
                entry->~HashNode();
                mem_free(entry);
                entry = nullptr;
            } else {
                ((*table_[segment_idx])[bucket_idx]).set_valid(false);
            }
            ATOMIC_AAF(&size_, -1);
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_OK;
        } else {
            HashNode<KeyType, ValueType>* prev =
                &((*table_[segment_idx])[bucket_idx]);
            HashNode<KeyType, ValueType>* entry =
                ((*table_[segment_idx])[bucket_idx]).get_next();
            while (entry != nullptr) {
                if (entry->get_key() == key) {
                    prev->set_next(
                        entry->get_next());  // remove node and reconnect
                    entry->~HashNode();
                    mem_free(entry);
                    entry = nullptr;
                    ATOMIC_AAF(&size_, -1);
                    table_[segment_idx]->mutexes_[bucket_idx].unlock();
                    return E_OK;
                }
                prev = entry;
                entry = entry->get_next();
            }
            table_[segment_idx]->mutexes_[bucket_idx].unlock();
            return E_NOT_EXIST;
        }
    }

#ifndef NDEBUG
    void print() {
        printf("<< The Hash Table >>\n");
        if (!ATOMIC_AAF(&size_, 0)) {
            printf("  Empty now!\n");
            return;
        }
        size_t bucket_num = 0;
        for (size_t i = 0; i < ATOMIC_AAF(&new_table_capacity_, 0); i++) {
            for (size_t j = 0; j < SEGMENT_CAPACITY; j++) {
                // printf("----(%ld)segment id is %ld, bucket id is %ld----\n",
                // bucket_num++, i, j);
                if ((*table_[i])[j].get_valid()) {
                    printf("----(%ld)segment id is %ld, bucket id is %ld----\n",
                           bucket_num++, i, j);
                    std::cout << "|      "
                              << "key is " << (*table_[i])[j].get_key()
                              << ", value is " << (*table_[i])[j].get_value()
                              << ", rehashid is "
                              << (int)(*table_[i])[j].get_rehashid()
                              << std::endl;
                    HashNode<KeyType, ValueType>* entry =
                        ((*table_[i])[j]).get_next();
                    while (entry != nullptr) {
                        std::cout << "|      "
                                  << "key is " << entry->get_key()
                                  << ", value is " << entry->get_value()
                                  << ", rehashid is "
                                  << (int)entry->get_rehashid() << std::endl;
                        entry = entry->get_next();
                    }
                } else {
                    bucket_num++;
                }
            }
        }
    }
#endif  // #ifndef NDEBUG

    FORCE_INLINE bool empty() const { return ATOMIC_LOAD(&size_) == 0; }

    FORCE_INLINE size_t size() { return ATOMIC_LOAD(&size_); }

    FORCE_INLINE size_t total_capacity() {
        return ATOMIC_LOAD(&new_table_capacity_) * SEGMENT_CAPACITY;
    }

   private:
    size_t new_table_capacity_;
    size_t old_table_capacity_;
    size_t size_;
    uint64_t hash_version_;
    Array<HashSegm<KeyType, ValueType>*>
        table_;  // the element in Array is pointer.
    bool is_inited_;
    uint8_t is_extending_;
    F hashfunc_;
};

}  // end namespace common
#endif  // COMMON_CONTAINER_HASH_TABLE_H
