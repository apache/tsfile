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

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <dirent.h>
#include <fstream>
#include <iostream>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cstdlib>
#include <vector>

#include "common/allocator/byte_stream.h"
#include "encoding/sprintz_opt_int64_codec.h"

using storage::SprintzOptInt64Decoder;
using storage::SprintzOptInt64Encoder;

namespace {

struct Result {
    std::string dataset;
    std::string mode;
    int64_t points;
    int64_t tsfile_size_bytes;
    int64_t write_encode_ns;
    int64_t write_io_ns;
    int64_t read_io_ns;
    int64_t read_decode_ns;
};

static bool ends_with(const std::string &s, const std::string &suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

static bool dataset_selected(const std::string &name) {
    // Optional: comma-separated allowlist, e.g. "City-temp.csv,Food-price.csv"
    if (const char *env = std::getenv("TSFILE_DATASET_NAMES")) {
        std::string s(env);
        if (s.empty()) return true;
        size_t pos = 0;
        while (pos < s.size()) {
            while (pos < s.size() && (s[pos] == ' ' || s[pos] == '\t' || s[pos] == ',')) pos++;
            size_t start = pos;
            while (pos < s.size() && s[pos] != ',') pos++;
            size_t end = pos;
            while (end > start && (s[end - 1] == ' ' || s[end - 1] == '\t')) end--;
            if (end > start) {
                if (name == s.substr(start, end - start)) return true;
            }
            pos++;  // skip comma
        }
        return false;
    }
    return true;
}

static std::vector<int64_t> read_and_scale_csv_as_int64(const std::string &csv_path) {
    std::ifstream in(csv_path);
    if (!in.is_open()) {
        return {};
    }
    std::vector<std::string> nums;
    nums.reserve(1 << 16);
    int decimal_max = 0;

    std::string line;
    while (std::getline(in, line)) {
        std::stringstream ss(line);
        std::string cell;
        while (std::getline(ss, cell, ',')) {
            // trim spaces
            size_t b = cell.find_first_not_of(" \t\r\n");
            if (b == std::string::npos) continue;
            size_t e = cell.find_last_not_of(" \t\r\n");
            std::string s = cell.substr(b, e - b + 1);
            if (s.empty()) continue;
            nums.push_back(s);
            auto dot = s.find('.');
            if (dot != std::string::npos && dot + 1 < s.size()) {
                int dec = (int)(s.size() - (dot + 1));
                decimal_max = std::max(decimal_max, dec);
            }
        }
    }

    auto pow10 = [](int k) -> long double {
        long double r = 1.0;
        for (int i = 0; i < k; i++) r *= 10.0;
        return r;
    };
    long double scale = pow10(decimal_max);

    std::vector<int64_t> out;
    out.reserve(nums.size());
    for (const auto &s : nums) {
        char *endp = nullptr;
        long double v = strtold(s.c_str(), &endp);
        if (endp == s.c_str()) continue;  // skip non-numeric
        // Match Java path: scale -> int32 overflow semantics -> widen to int64.
        // Java uses int[] scaledInts_all, so values wrap in 32-bit signed range.
        long double scaled_ld = llround(v * scale);
        int64_t scaled_i64 = (int64_t)scaled_ld;  // implementation-defined on overflow; keep as-is
        int32_t scaled_i32 = (int32_t)scaled_i64; // wrap like Java int cast
        out.push_back((int64_t)scaled_i32);
    }
    return out;
}

static int getenv_int_default(const char *key, int default_val) {
    if (const char *e = std::getenv(key)) {
        if (e[0] == '\0') return default_val;
        return std::max(0, atoi(e));
    }
    return default_val;
}

// Sprintz compression SIMD (Neon pack / optimal layout): default off. TSFILE_SPRINTZ_ENCODE_SIMD=1 to enable.
static bool sprintz_encode_simd_enabled() {
    if (const char *e = std::getenv("TSFILE_SPRINTZ_ENCODE_SIMD")) {
        return atoi(e) != 0;
    }
    return false;
}

// Bit unpack SIMD: default on (independent of encode). TSFILE_SPRINTZ_DECODE_SIMD=0 to disable.
static bool sprintz_decode_simd_enabled() {
    if (const char *e = std::getenv("TSFILE_SPRINTZ_DECODE_SIMD")) {
        return atoi(e) != 0;
    }
    return true;
}

// Default ON: cold-cache friendly IO + stronger write flush (can push write_io toward ~50% vs encode on fast SSD).
static bool benchmark_cold_io_enabled() {
    if (const char *e = std::getenv("TSFILE_BENCHMARK_COLD_IO")) {
        return atoi(e) != 0;
    }
    return true;
}

static bool benchmark_write_o_sync() {
    if (const char *e = std::getenv("TSFILE_WRITE_O_SYNC")) {
        return atoi(e) != 0;
    }
    return benchmark_cold_io_enabled();
}

// 0 = one write + final sync only. When cold IO: default 64 KiB per write/read syscall batch.
static size_t write_fsync_chunk_bytes() {
    int kb = getenv_int_default("TSFILE_WRITE_FSYNC_CHUNK_KB", benchmark_cold_io_enabled() ? 64 : 0);
    return kb > 0 ? (size_t)kb * 1024u : (size_t)0;
}

// Timed reads use the same per-chunk size as timed writes (TSFILE_WRITE_FSYNC_CHUNK_KB).
static size_t read_chunk_bytes() {
    return write_fsync_chunk_bytes();
}

static int read_thrash_megabytes() {
    if (!benchmark_cold_io_enabled()) return 0;
    return getenv_int_default("TSFILE_READ_THRASH_MB", 256);
}

// Writes spend most time in O_SYNC + per-chunk F_FULLFSYNC; a single plain read is much faster.
// Optionally repeat full-file reads (still timed) until cumulative read_io reaches this % of write_io.
// 0 = one read only. 100 ~= match write duration (bounded by TSFILE_READ_IO_MATCH_MAX_PASSES).
static int read_io_match_write_pct() {
    if (!benchmark_cold_io_enabled()) return 0;
    return std::min(100, getenv_int_default("TSFILE_READ_IO_MATCH_WRITE_PCT", 100));
}

static int read_io_match_max_passes() {
    return std::max(1, getenv_int_default("TSFILE_READ_IO_MATCH_MAX_PASSES", 4096));
}

static bool sync_fd_heavy(int fd) {
#ifdef F_FULLFSYNC
    if (fcntl(fd, F_FULLFSYNC) != 0) return false;
#else
    if (fsync(fd) != 0) return false;
#endif
    return true;
}

// One-time large file used to pollute page cache before timed reads (best-effort).
static void ensure_thrash_file(const std::string &dir, int mb) {
    if (mb <= 0) return;
    static bool created = false;
    if (created) return;
    std::string path = dir + "/.benchmark_cache_thrash.bin";
    const size_t total = (size_t)mb * 1024u * 1024u;
    const size_t chunk = 4u * 1024u * 1024u;
    int fd = ::open(path.c_str(), O_CREAT | O_TRUNC | O_WRONLY, 0644);
    if (fd < 0) return;
#ifdef F_NOCACHE
    (void)fcntl(fd, F_NOCACHE, 1);
#endif
    std::vector<char> buf(chunk, 0);
    for (size_t off = 0; off < total; off += chunk) {
        size_t n = std::min(chunk, total - off);
        size_t woff = 0;
        while (woff < n) {
            ssize_t w = ::write(fd, buf.data(), n - woff);
            if (w <= 0) {
                ::close(fd);
                return;
            }
            woff += (size_t)w;
        }
        (void)sync_fd_heavy(fd);
    }
    (void)sync_fd_heavy(fd);
    if (::close(fd) != 0) return;
    created = true;
}

static void read_thrash_file_untimed(const std::string &dir, int mb) {
    if (mb <= 0) return;
    std::string path = dir + "/.benchmark_cache_thrash.bin";
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return;
#ifdef F_NOCACHE
    (void)fcntl(fd, F_NOCACHE, 1);
#endif
    struct stat st;
    if (fstat(fd, &st) != 0) {
        ::close(fd);
        return;
    }
    const size_t sz = (size_t)st.st_size;
    size_t io_chunk = read_chunk_bytes();
    if (io_chunk == 0) {
        io_chunk = 64u * 1024u;
    }
    std::vector<char> tmp(io_chunk);
    volatile unsigned char sink = 0;
    size_t off = 0;
    while (off < sz) {
        size_t want = std::min(io_chunk, sz - off);
        ssize_t r = ::read(fd, tmp.data(), want);
        if (r <= 0) break;
        for (ssize_t i = 0; i < r; i++) sink ^= (unsigned char)tmp[(size_t)i];
        off += (size_t)r;
    }
    (void)sink;
    ::close(fd);
}

static std::vector<int64_t> repeat_dataset(const std::vector<int64_t> &in, int repeat) {
    if (repeat <= 1 || in.empty()) {
        return in;
    }
    std::vector<int64_t> out;
    out.reserve(in.size() * (size_t)repeat);
    for (int i = 0; i < repeat; i++) {
        out.insert(out.end(), in.begin(), in.end());
    }
    return out;
}

static bool write_whole_file(const std::string &path, const char *buf, size_t nbytes) {
    int flags = O_CREAT | O_TRUNC | O_WRONLY;
#ifdef O_SYNC
    if (benchmark_write_o_sync()) flags |= O_SYNC;
#endif
    int fd = ::open(path.c_str(), flags, 0644);
    if (fd < 0) return false;
#ifdef F_NOCACHE
    (void)fcntl(fd, F_NOCACHE, 1);
#endif
    const size_t chunk = write_fsync_chunk_bytes();
    size_t off = 0;
    if (chunk == 0 || !benchmark_cold_io_enabled()) {
        while (off < nbytes) {
            ssize_t w = ::write(fd, buf + off, nbytes - off);
            if (w <= 0) {
                ::close(fd);
                return false;
            }
            off += (size_t)w;
        }
    } else {
        while (off < nbytes) {
            size_t want = std::min(chunk, nbytes - off);
            size_t woff = 0;
            while (woff < want) {
                ssize_t w = ::write(fd, buf + off + woff, want - woff);
                if (w <= 0) {
                    ::close(fd);
                    return false;
                }
                woff += (size_t)w;
            }
            off += want;
            if (benchmark_cold_io_enabled() && !sync_fd_heavy(fd)) {
                ::close(fd);
                return false;
            }
        }
    }
    // Final metadata / durability flush. With per-chunk sync, one more sync is enough; without chunking,
    // use an extra F_FULLFSYNC on macOS in cold mode to push write_io time up.
    if (!sync_fd_heavy(fd)) {
        ::close(fd);
        return false;
    }
#ifdef F_FULLFSYNC
    if (benchmark_cold_io_enabled() && chunk == 0) {
        (void)fcntl(fd, F_FULLFSYNC);
    }
#endif
    ::close(fd);
    return true;
}

static bool dir_exists(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

static void ensure_dir(const std::string &path) {
    // best-effort: single-level mkdir; path here is absolute and should already exist mostly.
    mkdir(path.c_str(), 0755);
}

static bool read_whole_file(const std::string &path, std::vector<char> &out) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return false;
#ifdef F_NOCACHE
    (void)fcntl(fd, F_NOCACHE, 1);
#endif
    struct stat st;
    if (fstat(fd, &st) != 0) {
        ::close(fd);
        return false;
    }
    size_t sz = (size_t)st.st_size;
    out.resize(sz);
    const size_t chunk = read_chunk_bytes();
    size_t off = 0;
    if (chunk == 0 || !benchmark_cold_io_enabled()) {
        while (off < sz) {
            ssize_t r = ::read(fd, out.data() + off, sz - off);
            if (r <= 0) {
                ::close(fd);
                return false;
            }
            off += (size_t)r;
        }
    } else {
        while (off < sz) {
            size_t want = std::min(chunk, sz - off);
            size_t roff = 0;
            while (roff < want) {
                ssize_t r = ::read(fd, out.data() + off + roff, want - roff);
                if (r <= 0) {
                    ::close(fd);
                    return false;
                }
                roff += (size_t)r;
            }
            off += want;
        }
    }
    ::close(fd);
    return true;
}

static Result run_once_one_dataset(const std::string &dataset_name,
                                   const std::vector<int64_t> &data,
                                   bool optimal_mode,
                                   const std::string &tmp_dir,
                                   int64_t iter_id) {
    const std::string mode = optimal_mode ? "OptimalPackSize" : "PackSize8";

    common::ByteStream out(1024, common::MOD_ENCODER_OBJ);
    SprintzOptInt64Encoder enc(/*use_optimal=*/optimal_mode,
                              /*fixed_pack_size=*/8,
                              "delta",
                              512,
                              /*enable_simd=*/sprintz_encode_simd_enabled());

    auto t0 = std::chrono::high_resolution_clock::now();
    for (auto v : data) {
        enc.encode(v, out);
    }
    enc.flush(out);
    auto t1 = std::chrono::high_resolution_clock::now();

    int64_t write_encode_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count();

    uint32_t encoded_size = out.total_size();
    char *encoded_buf = common::get_bytes_from_bytestream(out);
    EXPECT_NE(encoded_buf, nullptr);

    // Single whole-file I/O timing (as requested)
    // Per request (B): use unique filenames per repeat to reduce caching artifacts.
    // Keep mode in filename so PackSize8 and Optimal don't collide.
    std::string out_path = tmp_dir + "/" + dataset_name +
                           (optimal_mode ? "_optimal_" : "_pack8_") +
                           std::to_string((long long)iter_id) + ".tsfile.bin";

    auto w0 = std::chrono::high_resolution_clock::now();
    bool w_ok = write_whole_file(out_path, encoded_buf, encoded_size);
    auto w1 = std::chrono::high_resolution_clock::now();
    EXPECT_TRUE(w_ok);
    int64_t write_io_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(w1 - w0).count();

    // Best-effort cold read: thrash page cache with a large unrelated file (un-timed).
    read_thrash_file_untimed(tmp_dir, read_thrash_megabytes());

    std::vector<char> file_bytes;
    int64_t read_io_ns = 0;
    int match_pct = read_io_match_write_pct();
    int64_t read_target_ns = match_pct > 0 ? (write_io_ns * (int64_t)match_pct) / 100 : 0;
    int max_read_passes = read_io_match_max_passes();
    int read_pass = 0;
    do {
        auto rp0 = std::chrono::high_resolution_clock::now();
        bool r_ok = read_whole_file(out_path, file_bytes);
        auto rp1 = std::chrono::high_resolution_clock::now();
        EXPECT_TRUE(r_ok);
        read_io_ns +=
            std::chrono::duration_cast<std::chrono::nanoseconds>(rp1 - rp0).count();
        read_pass++;
    } while (match_pct > 0 && read_io_ns < read_target_ns && read_pass < max_read_passes);

    // Decode timing (in-memory)
    common::ByteStream in_stream(1024, common::MOD_DECODER_OBJ);
    if (!file_bytes.empty()) {
        in_stream.write_buf((const uint8_t *)file_bytes.data(), (uint32_t)file_bytes.size());
    }
    SprintzOptInt64Decoder dec("delta", sprintz_decode_simd_enabled());
    std::vector<int64_t> decoded;
    decoded.reserve(data.size());

    auto t2 = std::chrono::high_resolution_clock::now();
    for (size_t i = 0; i < data.size(); i++) {
        int64_t v;
        int ret = dec.read_int64(v, in_stream);
        EXPECT_EQ(ret, common::E_OK);
        decoded.push_back(v);
    }
    auto t3 = std::chrono::high_resolution_clock::now();

    EXPECT_EQ(decoded, data);

    int64_t read_decode_ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t3 - t2).count();

    // Per request: delete generated file immediately to avoid disk bloat.
    (void)unlink(out_path.c_str());

    free(encoded_buf);

    return Result{dataset_name,
                  mode,
                  (int64_t)data.size(),
                  (int64_t)encoded_size,
                  write_encode_ns,
                  write_io_ns,
                  read_io_ns,
                  read_decode_ns};
}

static Result avg_results(const std::vector<Result> &rs) {
    Result out = rs[0];
    out.write_encode_ns = 0;
    out.write_io_ns = 0;
    out.read_io_ns = 0;
    out.read_decode_ns = 0;
    for (const auto &r : rs) {
        out.tsfile_size_bytes = r.tsfile_size_bytes;  // last
        out.write_encode_ns += r.write_encode_ns;
        out.write_io_ns += r.write_io_ns;
        out.read_io_ns += r.read_io_ns;
        out.read_decode_ns += r.read_decode_ns;
    }
    int64_t n = (int64_t)rs.size();
    out.write_encode_ns /= n;
    out.write_io_ns /= n;
    out.read_io_ns /= n;
    out.read_decode_ns /= n;
    return out;
}

static void append_csv(const std::string &path, const std::vector<Result> &rows) {
    bool exists = false;
    {
        std::ifstream fin(path);
        exists = fin.good();
    }
    std::ofstream out(path, std::ios::app);
    if (!exists) {
        out << "Dataset,Mode,TsFile Size (bytes),Write Time (ns),Write Encode (ns),Write IO (ns),"
               "Read Time (ns),Read IO (ns),Read Decode (ns),Points,Compression Ratio\n";
    }
    for (const auto &r : rows) {
        double raw_bytes = (double)r.points * 8.0;
        double ratio = (double)r.tsfile_size_bytes / raw_bytes;
        int64_t write_time = r.write_encode_ns + r.write_io_ns;
        int64_t read_time = r.read_io_ns + r.read_decode_ns;
        out << r.dataset << "," << r.mode << "," << r.tsfile_size_bytes << "," << write_time << ","
            << r.write_encode_ns << "," << r.write_io_ns << "," << read_time << "," << r.read_io_ns
            << "," << r.read_decode_ns << "," << r.points << "," << ratio << "\n";
    }
}

TEST(SprintzPackSize8VsOptimal, CsvBenchmarkFairOrder) {
    // Match Java test data directory (adjust if needed).
    const std::string directory =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/ElfTestData_camel";
    const std::string output_dir =
        "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/output_tsfile_packsize_comparison_cpp";
    ensure_dir(output_dir);
    const std::string csv_path = output_dir + "/tsfile_comparison_cpp.csv";

    ASSERT_TRUE(dir_exists(directory));
    // Overwrite CSV on every run (avoid appending across runs).
    (void)unlink(csv_path.c_str());

    ensure_thrash_file(output_dir, read_thrash_megabytes());

    const int warmup_repeats =
        std::max(0, getenv_int_default("TSFILE_BENCHMARK_WARMUP", 0));
    const int measure_repeats =
        std::max(1, getenv_int_default("TSFILE_BENCHMARK_MEASURE_REPEATS", 1));
    const int dataset_repeat = 100;  // expand each dataset by 100x, as requested
    int dataset_limit = 0;           // 0 = no limit
    if (const char *env = std::getenv("TSFILE_DATASET_LIMIT")) {
        dataset_limit = std::max(0, atoi(env));
    }
    bool stats_only = false;
    if (const char *env = std::getenv("TSFILE_OPT_BW_STATS_ONLY")) {
        stats_only = (atoi(env) != 0);
    }

    DIR *dp = opendir(directory.c_str());
    ASSERT_NE(dp, (DIR *)nullptr);
    struct dirent *de;
    int datasets_done = 0;
    while ((de = readdir(dp)) != nullptr) {
        std::string name = de->d_name;
        if (name == "." || name == "..") continue;
        if (!ends_with(name, ".csv")) continue;
        if (!dataset_selected(name)) continue;

        auto data = read_and_scale_csv_as_int64(directory + "/" + name);
        if (data.empty()) continue;
        data = repeat_dataset(data, dataset_repeat);

        std::cout << "[benchmark] dataset=" << name << " points=" << (long long)data.size() << std::endl;

        if (stats_only) {
            // Only collect Optimal bitWidth top-k distribution; skip IO/timing.
            common::ByteStream out(1024, common::MOD_ENCODER_OBJ);
            SprintzOptInt64Encoder enc(/*use_optimal=*/true,
                                      /*fixed_pack_size=*/8,
                                      "delta",
                                      512,
                                      /*enable_simd=*/sprintz_encode_simd_enabled());
            for (auto v : data) {
                enc.encode(v, out);
            }
            enc.flush(out);
            datasets_done++;
            if (dataset_limit > 0 && datasets_done >= dataset_limit) break;
            continue;
        }

        // warmup: Pack8 -> Opt, then Opt -> Pack8 (balanced)
        for (int w = 0; w < warmup_repeats; w++) {
            std::cout << "[warmup] PackSize8 dataset=" << name << " iter=" << w << std::endl;
            (void)run_once_one_dataset(name, data, /*optimal=*/false, output_dir, w);
            std::cout << "[warmup] OptimalPackSize dataset=" << name << " iter=" << w << std::endl;
            (void)run_once_one_dataset(name, data, /*optimal=*/true, output_dir, w);
        }
        for (int w = 0; w < warmup_repeats; w++) {
            std::cout << "[warmup] OptimalPackSize dataset=" << name << " iter=" << (w + warmup_repeats) << std::endl;
            (void)run_once_one_dataset(name, data, /*optimal=*/true, output_dir, w + warmup_repeats);
            std::cout << "[warmup] PackSize8 dataset=" << name << " iter=" << (w + warmup_repeats) << std::endl;
            (void)run_once_one_dataset(name, data, /*optimal=*/false, output_dir, w + warmup_repeats);
        }

        std::vector<Result> pack8_samples;
        std::vector<Result> opt_samples;
        pack8_samples.reserve(2 * measure_repeats);
        opt_samples.reserve(2 * measure_repeats);

        for (int m = 0; m < measure_repeats; m++) {
            int64_t iter_id = (int64_t)m;
            std::cout << "[measure] PackSize8 dataset=" << name << " iter=" << (long long)iter_id << std::endl;
            pack8_samples.push_back(
                run_once_one_dataset(name, data, /*optimal=*/false, output_dir, iter_id));
            std::cout << "[measure] OptimalPackSize dataset=" << name << " iter=" << (long long)iter_id << std::endl;
            opt_samples.push_back(
                run_once_one_dataset(name, data, /*optimal=*/true, output_dir, iter_id));
        }
        for (int m = 0; m < measure_repeats; m++) {
            int64_t iter_id = (int64_t)(m + measure_repeats);
            std::cout << "[measure] OptimalPackSize dataset=" << name << " iter=" << (long long)iter_id << std::endl;
            opt_samples.push_back(
                run_once_one_dataset(name, data, /*optimal=*/true, output_dir, iter_id));
            std::cout << "[measure] PackSize8 dataset=" << name << " iter=" << (long long)iter_id << std::endl;
            pack8_samples.push_back(
                run_once_one_dataset(name, data, /*optimal=*/false, output_dir, iter_id));
        }

        auto pack8_avg = avg_results(pack8_samples);
        auto opt_avg = avg_results(opt_samples);
        append_csv(csv_path, {pack8_avg, opt_avg});

        datasets_done++;
        if (dataset_limit > 0 && datasets_done >= dataset_limit) {
            break;
        }
    }
    closedir(dp);
}

}  // namespace

