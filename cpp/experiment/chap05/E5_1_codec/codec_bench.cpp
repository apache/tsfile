/*
 * E5-1: Codec Throughput with SIMD ON/OFF
 *
 * Direct timing of ChunkWriter (encoding) and decoder (decoding) to measure
 * SIMD speedup. Bypasses the complete read/write path to eliminate I/O
 * interference. Single-threaded execution.
 *
 * Tests: INT32, INT64, FLOAT, DOUBLE
 * Encoding: TS_2DIFF (integer types), GORILLA (float types)
 * Compression: SNAPPY
 *
 * Output:
 *   encode_results.csv: dtype, throughput_mrows_s, time_s
 *   decode_results.csv: dtype, throughput_mrows_s, time_s
 *
 * Build:
 *   cmake -DENABLE_SIMD=OFF -DBUILD_TEST=OFF ..   # C1 scalar
 *   cmake -DENABLE_SIMD=ON  -DBUILD_TEST=OFF ..   # C3 SIMD
 *   cmake --build . --target codec_bench
 *
 * Run:
 *   ./codec_bench [total_rows] [csv_dir]
 */

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "common/global.h"
#include "common/schema.h"
#include "encoding/ts2diff_encoder.h"
#include "encoding/ts2diff_decoder.h"
#include "encoding/gorilla_encoder.h"
#include "encoding/gorilla_decoder.h"
#include "writer/chunk_writer.h"
#include "writer/tsfile_writer.h"

using Clock = std::chrono::high_resolution_clock;

// ─── Configuration ──────────────────────────────────────────────────────────

static int64_t gTotalRows = 20000000;   // 20M default (per dtype)
static std::string gCsvDir = ".";
static const int kWarmupRounds = 1;
static const int kBenchRounds = 3;

// ─── Helpers ────────────────────────────────────────────────────────────────

static std::string simd_label() {
#ifdef ENABLE_SIMD
    return "ON";
#else
    return "OFF";
#endif
}

static void print_build_config() {
    std::cout << "=== E5-1: Codec Throughput Benchmark ===\n"
              << "  SIMD:       " << simd_label() << "\n"
              << "  total_rows: " << gTotalRows << " (per dtype)\n"
              << "  warmup:     " << kWarmupRounds << " round(s)\n"
              << "  bench:      " << kBenchRounds << " round(s)\n"
              << "  csv_dir:    " << gCsvDir << "\n\n";
}

struct BenchRecord {
    std::string dtype;
    std::string operation;  // "encode" or "decode"
    double throughput_mrows;
    double time_s;
};

static std::vector<BenchRecord> gRecords;

static void record(const std::string& dtype, const std::string& op,
                   double time_s, int64_t rows) {
    double throughput = rows / time_s / 1e6;
    gRecords.push_back({dtype, op, throughput, time_s});
    std::cout << "  " << std::left << std::setw(10) << dtype
              << std::setw(8) << op
              << std::fixed << std::setprecision(3) << time_s << " s  "
              << std::setprecision(2) << throughput << " M rows/s\n";
}

// ─── W0 data generation (matches no_flush_bench / write_memory) ─────────────
// Seed: 42, noise: ni(-100,100), nf(-5,5), nd(-0.5,0.5)
// INT32: ts%100000+ni, INT64: ts+ni, FLOAT: ts%10000+nf, DOUBLE: ts*1.1+nd

static void generate_int32_data(std::vector<int64_t>& timestamps,
                                std::vector<int32_t>& values, int64_t n,
                                std::mt19937_64& rng) {
    timestamps.resize(n);
    values.resize(n);
    std::uniform_int_distribution<int> ni(-100, 100);
    for (int64_t i = 0; i < n; i++) {
        timestamps[i] = 1000 + i;
        values[i] = static_cast<int32_t>((1000 + i) % 100000) + ni(rng);
    }
}

static void generate_int64_data(std::vector<int64_t>& timestamps,
                                std::vector<int64_t>& values, int64_t n,
                                std::mt19937_64& rng) {
    timestamps.resize(n);
    values.resize(n);
    std::uniform_int_distribution<int> ni(-100, 100);
    for (int64_t i = 0; i < n; i++) {
        int64_t ts = 1000 + i;
        timestamps[i] = ts;
        values[i] = ts + ni(rng);
    }
}

static void generate_float_data(std::vector<int64_t>& timestamps,
                                std::vector<float>& values, int64_t n,
                                std::mt19937_64& rng) {
    timestamps.resize(n);
    values.resize(n);
    std::uniform_real_distribution<float> nf(-5.0f, 5.0f);
    for (int64_t i = 0; i < n; i++) {
        int64_t ts = 1000 + i;
        timestamps[i] = ts;
        values[i] = static_cast<float>(ts % 10000) + nf(rng);
    }
}

static void generate_double_data(std::vector<int64_t>& timestamps,
                                 std::vector<double>& values, int64_t n,
                                 std::mt19937_64& rng) {
    timestamps.resize(n);
    values.resize(n);
    std::uniform_real_distribution<double> nd(-0.5, 0.5);
    for (int64_t i = 0; i < n; i++) {
        int64_t ts = 1000 + i;
        timestamps[i] = ts;
        values[i] = ts * 1.1 + nd(rng);
    }
}

// Bench encode using ChunkWriter with specified encoding
template <typename T>
static double bench_encode_impl(const std::vector<int64_t>& timestamps,
                                const std::vector<T>& values,
                                common::TSDataType dtype,
                                common::TSEncoding encoding, int64_t n) {
    double best_time = 1e18;
    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        storage::ChunkWriter cw;
        cw.init("bench_col", dtype, encoding, common::SNAPPY);

        auto t0 = Clock::now();
        cw.write_batch(timestamps.data(), values.data(),
                       static_cast<uint32_t>(n));
        cw.end_encode_chunk();
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();

        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
    }
    return best_time;
}

// ─── Decode benchmark using raw decoders ────────────────────────────────────
// Encode data first with the encoder, then decode from the ByteStream.

// Flatten a paged ByteStream into a contiguous buffer for wrap_from reading.
struct FlatBuf {
    char* data;
    uint32_t len;
    FlatBuf() : data(nullptr), len(0) {}
    ~FlatBuf() { if (data) free(data); }
    void flatten(common::ByteStream& paged) {
        len = static_cast<uint32_t>(paged.total_size());
        data = (char*)malloc(len);
        uint32_t read_len = 0;
        paged.read_buf((uint8_t*)data, len, read_len);
    }
    common::ByteStream* make_stream() const {
        auto* s = new common::ByteStream(common::MOD_DEFAULT);
        s->wrap_from(data, len);
        return s;
    }
private:
    FlatBuf(const FlatBuf&);
    FlatBuf& operator=(const FlatBuf&);
};

static double bench_decode_ts2diff_int32(int64_t n, std::mt19937_64& rng) {
    common::ByteStream encoded(4096, common::MOD_DEFAULT);
    storage::IntTS2DIFFEncoder encoder;
    std::uniform_int_distribution<int> ni(-100, 100);
    for (int64_t i = 0; i < n; i++) {
        int32_t val = static_cast<int32_t>((1000 + i) % 100000) + ni(rng);
        encoder.encode(val, encoded);
    }
    encoder.flush(encoded);

    FlatBuf flat;
    flat.flatten(encoded);

    double best_time = 1e18;
    const int BATCH = 1024;
    int32_t buf[BATCH];

    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        common::ByteStream* in = flat.make_stream();
        storage::TS2DIFFDecoder<int32_t> decoder;
        auto t0 = Clock::now();
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            decoder.read_batch_int32(buf, BATCH, actual, *in);
            (void)actual;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
        delete in;
    }
    return best_time;
}

static double bench_decode_ts2diff_int64(int64_t n, std::mt19937_64& rng) {
    common::ByteStream encoded(4096, common::MOD_DEFAULT);
    storage::LongTS2DIFFEncoder encoder;
    std::uniform_int_distribution<int> ni(-100, 100);
    for (int64_t i = 0; i < n; i++) {
        int64_t ts = 1000 + i;
        int64_t val = ts + ni(rng);
        encoder.encode(val, encoded);
    }
    encoder.flush(encoded);

    FlatBuf flat;
    flat.flatten(encoded);

    double best_time = 1e18;
    const int BATCH = 1024;
    int64_t buf[BATCH];

    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        common::ByteStream* in = flat.make_stream();
        storage::TS2DIFFDecoder<int64_t> decoder;
        auto t0 = Clock::now();
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            decoder.read_batch_int64(buf, BATCH, actual, *in);
            (void)actual;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
        delete in;
    }
    return best_time;
}

// ─── Per-value decode (non-batch baseline) ──────────────────────────────────

static double bench_decode_perval_int32(int64_t n, std::mt19937_64& rng) {
    common::ByteStream encoded(4096, common::MOD_DEFAULT);
    storage::IntTS2DIFFEncoder encoder;
    std::uniform_int_distribution<int> ni(-100, 100);
    for (int64_t i = 0; i < n; i++) {
        int32_t val = static_cast<int32_t>((1000 + i) % 100000) + ni(rng);
        encoder.encode(val, encoded);
    }
    encoder.flush(encoded);

    FlatBuf flat;
    flat.flatten(encoded);

    double best_time = 1e18;
    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        common::ByteStream* in = flat.make_stream();
        storage::TS2DIFFDecoder<int32_t> decoder;
        int32_t val;
        auto t0 = Clock::now();
        while (decoder.has_remaining(*in)) {
            decoder.read_int32(val, *in);
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        (void)val;
        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
        delete in;
    }
    return best_time;
}

static double bench_decode_perval_int64(int64_t n, std::mt19937_64& rng) {
    common::ByteStream encoded(4096, common::MOD_DEFAULT);
    storage::LongTS2DIFFEncoder encoder;
    std::uniform_int_distribution<int> ni(-100, 100);
    for (int64_t i = 0; i < n; i++) {
        int64_t ts = 1000 + i;
        int64_t val = ts + ni(rng);
        encoder.encode(val, encoded);
    }
    encoder.flush(encoded);

    FlatBuf flat;
    flat.flatten(encoded);

    double best_time = 1e18;
    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        common::ByteStream* in = flat.make_stream();
        storage::TS2DIFFDecoder<int64_t> decoder;
        int64_t val;
        auto t0 = Clock::now();
        while (decoder.has_remaining(*in)) {
            decoder.read_int64(val, *in);
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        (void)val;
        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
        delete in;
    }
    return best_time;
}

// FLOAT/DOUBLE: GORILLA encoding (matches W0 config, kept for reference)
static double bench_decode_gorilla_float(int64_t n, std::mt19937_64& rng) {
    common::ByteStream encoded(4096, common::MOD_DEFAULT);
    storage::FloatGorillaEncoder encoder;
    std::uniform_real_distribution<float> nf(-5.0f, 5.0f);
    for (int64_t i = 0; i < n; i++) {
        float val = static_cast<float>((1000 + i) % 10000) + nf(rng);
        encoder.encode(val, encoded);
    }
    encoder.flush(encoded);

    FlatBuf flat;
    flat.flatten(encoded);

    double best_time = 1e18;
    const int BATCH = 1024;
    float buf[BATCH];

    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        common::ByteStream* in = flat.make_stream();
        storage::FloatGorillaDecoder decoder;
        auto t0 = Clock::now();
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            decoder.read_batch_float(buf, BATCH, actual, *in);
            (void)actual;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
        delete in;
    }
    return best_time;
}

static double bench_decode_gorilla_double(int64_t n, std::mt19937_64& rng) {
    common::ByteStream encoded(4096, common::MOD_DEFAULT);
    storage::DoubleGorillaEncoder encoder;
    std::uniform_real_distribution<double> nd(-0.5, 0.5);
    for (int64_t i = 0; i < n; i++) {
        int64_t ts = 1000 + i;
        double val = ts * 1.1 + nd(rng);
        encoder.encode(val, encoded);
    }
    encoder.flush(encoded);

    FlatBuf flat;
    flat.flatten(encoded);

    double best_time = 1e18;
    const int BATCH = 1024;
    double buf[BATCH];

    for (int r = 0; r < kWarmupRounds + kBenchRounds; r++) {
        common::ByteStream* in = flat.make_stream();
        storage::DoubleGorillaDecoder decoder;
        auto t0 = Clock::now();
        while (decoder.has_remaining(*in)) {
            int actual = 0;
            decoder.read_batch_double(buf, BATCH, actual, *in);
            (void)actual;
        }
        double sec = std::chrono::duration<double>(Clock::now() - t0).count();
        if (r >= kWarmupRounds && sec < best_time) best_time = sec;
        delete in;
    }
    return best_time;
}

// ─── CSV output ─────────────────────────────────────────────────────────────

static void write_csv() {
    std::string simd = simd_label();
    std::string path = gCsvDir + "/codec_results_" + simd + ".csv";

    std::ofstream csv(path);
    csv << "dtype,operation,simd,throughput_mrows_s,time_s\n";

    for (auto& r : gRecords) {
        csv << r.dtype << "," << r.operation << "," << simd << ","
            << std::fixed << std::setprecision(2) << r.throughput_mrows << ","
            << std::setprecision(4) << r.time_s << "\n";
    }

    std::cout << "\nCSV: " << path << "\n";
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc > 1) gTotalRows = std::atoll(argv[1]);
    if (argc > 2) gCsvDir = argv[2];

    print_build_config();
    storage::libtsfile_init();

    std::mt19937_64 rng(42);

    // Encoding benchmarks (TS_2DIFF for INT32/INT64, W0 data pattern)
    std::cout << "── Encoding (TS_2DIFF) ──\n";
    {
        std::vector<int64_t> ts; std::vector<int32_t> vals;
        generate_int32_data(ts, vals, gTotalRows, rng);
        double t = bench_encode_impl(ts, vals, common::INT32,
                                     common::TS_2DIFF, gTotalRows);
        record("INT32", "encode", t, gTotalRows);
    }
    {
        std::vector<int64_t> ts; std::vector<int64_t> vals;
        generate_int64_data(ts, vals, gTotalRows, rng);
        double t = bench_encode_impl(ts, vals, common::INT64,
                                     common::TS_2DIFF, gTotalRows);
        record("INT64", "encode", t, gTotalRows);
    }

    // Per-value decoding (non-batch baseline)
    std::cout << "\n── Decoding per-value (TS_2DIFF) ──\n";
    {
        double t = bench_decode_perval_int32(gTotalRows, rng);
        record("INT32", "decode_perval", t, gTotalRows);
    }
    {
        double t = bench_decode_perval_int64(gTotalRows, rng);
        record("INT64", "decode_perval", t, gTotalRows);
    }

    // Batch decoding (TS_2DIFF for INT32/INT64)
    std::cout << "\n── Decoding batch (TS_2DIFF) ──\n";
    {
        double t = bench_decode_ts2diff_int32(gTotalRows, rng);
        record("INT32", "decode_batch", t, gTotalRows);
    }
    {
        double t = bench_decode_ts2diff_int64(gTotalRows, rng);
        record("INT64", "decode_batch", t, gTotalRows);
    }

    write_csv();
    storage::libtsfile_destroy();
    return 0;
}
