
#include "bloom_filter.h"
#if USE_SYS_MATH
#include <math.h>
#endif

using namespace common;

namespace storage {

#if USE_SYS_MATH
// Returns the natural logarithm (base e) of a double value
double math_log(double in) { return math.log(in); }
#else
// Returns the natural logarithm (base e) of a double value
// To avoid use libm.so (however libstdc++ use libm.so sigh..)
double math_log(double in) {
    // result table: 0.90, 0.91, ... , 0.99
    const double in_arr[10] = {
        0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10,
    };
    const double res_arr[10] = {
        /* ln(0.01) = */ -4.60517018599,
        /* ln(0.02) = */ -3.91202300543,
        /* ln(0.03) = */ -3.50655789732,
        /* ln(0.04) = */ -3.21887582487,
        /* ln(0.05) = */ -2.99573227355,
        /* ln(0.06) = */ -2.81341071676,
        /* ln(0.07) = */ -2.65926003693,
        /* ln(0.08) = */ -2.52572864431,
        /* ln(0.09) = */ -2.40794560865,
        /* ln(0.10) = */ -2.30258509299,
    };

    if (in <= 0.01) {
        return res_arr[0];
    } else if (in >= 0.10) {
        return res_arr[9];
    }
    int i = 0;
    for (; i < 10; i++) {
        if (in < in_arr[i]) {
            break;
        }
    }
    if (i > 0) {  // float number is not precise
        return res_arr[i - 1];
    } else {
        return res_arr[0];
    }
}
#endif

/* ================ BitSet ================ */
void BitSet::to_bytes(uint8_t *&ret_bytes, int32_t &ret_len) const {
    int32_t words_in_use = get_words_in_use();
    if (words_in_use == 0) {
        return;
    }

    int len = 8 * (words_in_use - 1);
    uint64_t x = words_[words_in_use - 1];
    while (x != 0) {
        len++;
        x = x >> 8;
    }

    uint8_t *res =
        (uint8_t *)mem_alloc(sizeof(uint8_t) * len, MOD_BLOOM_FILTER);
    int32_t res_pos = 0;
    for (int32_t w = 0; w < words_in_use - 1; w++) {
        uint64_t word = words_[w];
        for (int b = 0; b < 8; b++) {
            *(res + res_pos) = (uint8_t)(word & 0xFF);
            word = word >> 8;
            res_pos++;
        }
    }
    uint64_t last_word = words_[words_in_use - 1];
    for (; res_pos < len; res_pos++) {
        *(res + res_pos) = (uint8_t)(last_word & 0xFF);
        last_word = last_word >> 8;
    }

    ASSERT(res_pos == len);
    ret_bytes = res;
    ret_len = len;
}

// TODO byte-wise unittest
int BitSet::from_bytes(uint8_t *filter_data, uint32_t filter_data_bytes_len) {
    int ret = E_OK;

    word_count_ =
        (filter_data_bytes_len / 8) + (filter_data_bytes_len % 8 == 0 ? 0 : 1);
    words_ =
        (uint64_t *)mem_alloc(word_count_ * sizeof(uint64_t), MOD_BLOOM_FILTER);
    if (IS_NULL(words_)) {
        return E_OOM;
    }

    uint32_t word_idx = 0;
    for (; word_idx < (filter_data_bytes_len / 8); word_idx += 1) {
        uint64_t cur_word = 0;
        uint8_t *cur_word_start_byte = filter_data + (word_idx * 8);
        cur_word |= *(cur_word_start_byte + 0);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 1);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 2);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 3);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 4);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 5);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 6);
        cur_word = cur_word << 8;
        cur_word |= *(cur_word_start_byte + 7);
        cur_word = cur_word << 8;
        *(words_ + word_idx) = cur_word;
    }

    if (filter_data_bytes_len - word_idx * 8 > 0) {
        uint64_t cur_word = 0;
        uint8_t *cur_word_start_byte = filter_data + (word_idx * 8);
        for (uint32_t r = 0; r < filter_data_bytes_len - word_idx * 8; r++) {
            cur_word |= *(cur_word_start_byte + r);
            cur_word = cur_word << 8;
        }
        *(words_ + word_idx) = cur_word;
    }
    return ret;
}

/* ================ Bloom Filter ================ */
const double BloomFilter::MIN_BF_ERROR_RATE = 0.01;
const double BloomFilter::MAX_BF_ERROR_RATE = 0.1;
const int32_t BloomFilter::SEEDS[MAX_HASH_FUNC_COUNT] = {5,  7,  11, 19,
                                                         31, 37, 43, 59};

int BloomFilter::init(double error_percent, int entry_count) {
    if (error_percent < MIN_BF_ERROR_RATE) {
        error_percent = MIN_BF_ERROR_RATE;
    } else if (error_percent > MAX_BF_ERROR_RATE) {
        error_percent = MAX_BF_ERROR_RATE;
    }

    double ln2 = 0.6931471805599453;
    double math_log_err_percent = math_log(error_percent);
    int32_t size =
        (-1) * ((int32_t)(entry_count * math_log_err_percent / ln2 / ln2)) + 1;
    int32_t hash_func_count = (-1) * (math_log_err_percent / ln2) + 1;
    size_ = UTIL_MAX(size, MIN_SIZE);
    hash_func_count_ = UTIL_MIN(hash_func_count, MAX_HASH_FUNC_COUNT);

    for (uint32_t i = 0; i < hash_func_count_; i++) {
        hash_func_arr_[i].init(size_, SEEDS[i]);
    }
    int ret = bitset_.init(size_);
    return ret;
}

String BloomFilter::get_entry_string(const String &device_name,
                                     const String &measurement_name) {
    String ret_str;
    int len = device_name.len_ + measurement_name.len_ + 2;  // '.' and '\0'
    char *path_buf = (char *)mem_alloc(len, MOD_BLOOM_FILTER);
    if (IS_NULL(path_buf)) {
        return ret_str;
    }
    memcpy(path_buf, device_name.buf_, device_name.len_);
    *(path_buf + device_name.len_) = '.';
    memcpy(path_buf + device_name.len_ + 1, measurement_name.buf_,
           measurement_name.len_);
    *(path_buf + device_name.len_ + measurement_name.len_ + 1) = '\0';
    ret_str.buf_ = path_buf;
    ret_str.len_ = len;
    return ret_str;
}

int BloomFilter::add_path_entry(const String &device_name,
                                const String &measurement_name) {
    if (device_name.is_null() || measurement_name.is_null()) {
        return E_INVALID_ARG;
    }

    String entry = get_entry_string(device_name, measurement_name);
    if (IS_NULL(entry.buf_)) {
        return E_OOM;
    }

    for (uint32_t i = 0; i < hash_func_count_; i++) {
        HashFunction &hf = hash_func_arr_[i];
        int32_t hv = hf.hash(entry);
        bitset_.set(hv);
    }
    free_entry_buf(entry.buf_);
    return E_OK;
}

int BloomFilter::serialize_to(ByteStream &out) {
    int ret = E_OK;
    uint8_t *filter_data_bytes = nullptr;
    int32_t filter_data_bytes_len = 0;
    bitset_.to_bytes(filter_data_bytes, filter_data_bytes_len);
    ASSERT(filter_data_bytes_len > 0);

    if (RET_FAIL(
            SerializationUtil::write_var_uint(filter_data_bytes_len, out))) {
    } else if (RET_FAIL(
                   out.write_buf(filter_data_bytes, filter_data_bytes_len))) {
    } else if (RET_FAIL(SerializationUtil::write_var_uint(size_, out))) {
    } else if (RET_FAIL(
                   SerializationUtil::write_var_uint(hash_func_count_, out))) {
    }
    bitset_.revert_bytes(filter_data_bytes);
    return ret;
}

int BloomFilter::deserialize_from(ByteStream &in) {
    int ret = E_OK;
    uint32_t filter_data_bytes_len = 0;
    uint32_t ret_read_len = 0;
    uint8_t *filter_data = nullptr;
    if (RET_FAIL(SerializationUtil::read_var_uint(filter_data_bytes_len, in))) {
    } else if (UNLIKELY(nullptr ==
                        (filter_data = (uint8_t *)mem_alloc(
                             filter_data_bytes_len, MOD_BLOOM_FILTER)))) {
        ret = E_OOM;
    } else if (RET_FAIL(in.read_buf(filter_data, filter_data_bytes_len,
                                    ret_read_len))) {
    } else if (RET_FAIL(
                   bitset_.from_bytes(filter_data, filter_data_bytes_len))) {
    } else if (RET_FAIL(SerializationUtil::read_var_uint(size_, in))) {
    } else if (RET_FAIL(
                   SerializationUtil::read_var_uint(hash_func_count_, in))) {
    } else {
        for (uint32_t i = 0; i < hash_func_count_; i++) {
            hash_func_arr_[i].init(size_, SEEDS[i]);
        }
    }
    if (filter_data != nullptr) {
        mem_free(filter_data);
    }
    return ret;
}

}  // namespace storage
