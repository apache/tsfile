/*
 * E5-4: Block-Level Time Filter Precision (Plan A vs Plan C)
 *
 * Compares two TS2DIFF block time bound estimation strategies:
 *   Plan A (Conservative): block_max = first_value + write_index * max_delta
 *   Plan C (Lookahead):    block_max = next_block.first_value (exact)
 *
 * The gap is the "phantom interval": write_index * (2^bit_width - 1) * delta_min
 *
 * Sub-experiments:
 *   E5-4a: Skip rate comparison at different bit_widths
 *   E5-4b: Query latency comparison (10% selectivity, 1000 repeats)
 *
 * Parameters:
 *   target_bw (bit_width_): 0, 4, 8, 12
 *   delta_base: 1000 ms (1 Hz sampling)
 *   n_blocks: 1000 per page
 *   block_size: 128 deltas + 1 first_value = 129 rows per block
 *
 * Build:
 *   cmake -DBUILD_TEST=OFF ..
 *   cmake --build . --target block_filter_bench
 *
 * Run:
 *   ./block_filter_bench [csv_dir]
 */

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#include "common/allocator/alloc_base.h"
#include "common/allocator/byte_stream.h"
#include "encoding/ts2diff_encoder.h"
#include "encoding/ts2diff_decoder.h"
#include "writer/tsfile_writer.h"

using Clock = std::chrono::high_resolution_clock;

// ─── Configuration ──────────────────────────────────────────────────────────

static std::string gCsvDir = ".";
static const int kBlockSize = 128;       // deltas per block (write_index_)
static const int kRowsPerBlock = 129;    // = block_size + 1 (first_value)
static const int kNumBlocks = 10000;
static const int64_t kDeltaBase = 1000;  // 1 Hz = 1000 ms between samples
static int gTargetBws[] = {0, 4, 8, 12};
static const int kNumBws = 4;
static const int kLatencyRepeats = 1000;

// ─── Data generation ────────────────────────────────────────────────────────
// Generate a TS2DIFF-encoded ByteStream with controlled bit_width.

struct GeneratedPage {
    char* flat_buf;         // contiguous encoded data
    uint32_t flat_len;
    // Per-block metadata for verification
    std::vector<int64_t> block_first_ts;   // true first timestamp per block
    std::vector<int64_t> block_last_ts;    // true last timestamp per block
    int64_t total_rows;

    GeneratedPage() : flat_buf(nullptr), flat_len(0), total_rows(0) {}

    ~GeneratedPage() {
        if (flat_buf) { free(flat_buf); flat_buf = nullptr; }
    }

    // Flatten paged ByteStream into contiguous buffer
    void flatten_from(common::ByteStream& paged) {
        flat_len = static_cast<uint32_t>(paged.total_size());
        flat_buf = (char*)malloc(flat_len);
        uint32_t read_len = 0;
        paged.read_buf((uint8_t*)flat_buf, flat_len, read_len);
    }

    // Helper: create a wrapped ByteStream for reading
    common::ByteStream* make_read_stream() const {
        auto* in = new common::ByteStream(common::MOD_DEFAULT);
        in->wrap_from(flat_buf, flat_len);
        return in;
    }

private:
    GeneratedPage(const GeneratedPage&);
    GeneratedPage& operator=(const GeneratedPage&);
};

static GeneratedPage* generate_page(int target_bw, std::mt19937_64& rng) {
    GeneratedPage* page = new GeneratedPage();
    storage::LongTS2DIFFEncoder encoder;
    common::ByteStream paged(4096, common::MOD_DEFAULT);

    int64_t timestamp = 0;

    for (int blk = 0; blk < kNumBlocks; blk++) {
        page->block_first_ts.push_back(timestamp);

        // Encode first_value + block_size deltas
        encoder.do_encode(timestamp, paged);

        for (int d = 0; d < kBlockSize; d++) {
            int64_t jitter = 0;
            if (target_bw > 0) {
                std::uniform_int_distribution<int64_t> dist(
                    0, (1LL << target_bw) - 1);
                jitter = dist(rng);
            }
            int64_t delta = kDeltaBase + jitter;
            timestamp += delta;
            encoder.do_encode(timestamp, paged);
        }

        page->block_last_ts.push_back(timestamp);
        page->total_rows += kRowsPerBlock;
    }
    // Flush any remaining
    encoder.flush(paged);

    // Flatten paged ByteStream into contiguous buffer for random access
    page->flatten_from(paged);

    return page;
}

// ─── Plan A: Conservative upper bound ───────────────────────────────────────
// For each block, compute block_max using the formula:
//   block_max = first_value + write_index * (delta_min + (2^bit_width - 1))

struct BlockInfo {
    int64_t first_value;
    int64_t block_max_planA;  // conservative
    int64_t block_max_planC;  // lookahead (exact)
    int block_count;
};

static std::vector<BlockInfo> analyze_blocks(GeneratedPage* page) {
    std::vector<BlockInfo> blocks;

    // Create a readable copy of the encoded data
    common::ByteStream* inp = page->make_read_stream();
    common::ByteStream& in = *inp;

    storage::TS2DIFFDecoder<int64_t> decoder;

    int blk_idx = 0;
    while (decoder.has_remaining(in)) {
        // Read header manually to get block parameters
        int32_t write_index, bit_width;
        int64_t delta_min, first_value;

        common::SerializationUtil::read_i32(write_index, in);
        common::SerializationUtil::read_i32(bit_width, in);
        common::SerializationUtil::read_i64(delta_min, in);
        common::SerializationUtil::read_i64(first_value, in);

        BlockInfo bi;
        bi.first_value = first_value;
        bi.block_count = write_index + 1;

        // Plan A: conservative estimate
        if (write_index == 0 || bit_width == 0) {
            bi.block_max_planA = first_value +
                                 (int64_t)write_index * delta_min;
        } else if (bit_width >= 63) {
            bi.block_max_planA = INT64_MAX;
        } else {
            int64_t max_delta = delta_min + ((1LL << bit_width) - 1);
            bi.block_max_planA = first_value +
                                 (int64_t)write_index * max_delta;
        }

        // Plan C: lookahead — peek next block's first_value
        int32_t packed_bytes = (write_index * bit_width + 7) / 8;
        if (in.remaining_size() >= (uint32_t)packed_bytes + 24) {
            // Next block header: write_index(4) + bit_width(4) + delta_min(8)
            // + first_value(8) at offset 16
            char* next_fv_ptr = in.get_wrapped_buf() + in.read_pos() +
                                packed_bytes + 16;
            bi.block_max_planC =
                (int64_t)common::SerializationUtil::read_ui64(next_fv_ptr);
        } else {
            // Last block: same as Plan A (no lookahead possible)
            bi.block_max_planC = bi.block_max_planA;
        }

        // Skip packed data
        in.wrapped_buf_advance_read_pos(packed_bytes);

        blocks.push_back(bi);
        blk_idx++;
    }

    delete inp;
    return blocks;
}

// ─── E5-4a: Skip rate comparison ────────────────────────────────────────────

struct SkipRateResult {
    int target_bw;
    std::string method;
    int blocks_total;
    int blocks_skipped;
    int phantom_blocks;
    double skip_rate_pct;
};

static std::vector<SkipRateResult> gSkipResults;

static void run_skip_rate(int target_bw, const std::vector<BlockInfo>& blocks) {
    int total = static_cast<int>(blocks.size());

    // For each block, construct a query that lands in the "phantom interval":
    //   query_start = true_block_max + delta_base/2
    // This should be OUTSIDE the true block range but INSIDE Plan A's
    // conservative range (for blocks with bit_width > 0).
    int planA_skipped = 0;
    int planC_skipped = 0;
    int phantom = 0;

    for (int i = 0; i < total; i++) {
        // Query point: just after the true end of this block
        int64_t query_point = blocks[i].block_max_planC + kDeltaBase / 2;

        // Can Plan A skip this block? (block_max < query_point means
        // the query starts AFTER the block ends)
        // satisfy_start_end_time checks if [block_min, block_max] overlaps
        // [query_start, query_end]. If block_max < query_start, skip.
        bool planA_can_skip =
            (blocks[i].block_max_planA < query_point);
        bool planC_can_skip =
            (blocks[i].block_max_planC < query_point);

        if (planA_can_skip) planA_skipped++;
        if (planC_can_skip) planC_skipped++;
        if (planC_can_skip && !planA_can_skip) phantom++;
    }

    gSkipResults.push_back({target_bw, "PlanA", total, planA_skipped, 0,
                            100.0 * planA_skipped / total});
    gSkipResults.push_back({target_bw, "PlanC", total, planC_skipped, phantom,
                            100.0 * planC_skipped / total});

    std::cout << "  bw=" << std::setw(2) << target_bw
              << "  PlanA_skip=" << std::setw(5) << planA_skipped
              << "  PlanC_skip=" << std::setw(5) << planC_skipped
              << "  phantom=" << std::setw(5) << phantom
              << "  (of " << total << " blocks)\n";
}

// ─── E5-4b: Query latency comparison ───────────────────────────────────────

struct LatencyResult {
    int target_bw;
    std::string method;
    double latency_ms_p50;
    double latency_ms_p95;
    double speedup;
};

static std::vector<LatencyResult> gLatencyResults;

// Simulate a 10% selectivity query using peek-and-skip
static double simulate_query_latency(GeneratedPage* page,
                                     bool use_lookahead,
                                     int64_t query_start, int64_t query_end) {
    common::ByteStream* inp = page->make_read_stream();
    common::ByteStream& in = *inp;

    storage::TS2DIFFDecoder<int64_t> decoder;

    const int BATCH = 256;
    int64_t buf[BATCH];
    int64_t rows_decoded = 0;
    int blocks_skipped = 0;

    auto t0 = Clock::now();

    while (decoder.has_remaining(in)) {
        int64_t block_min, block_max;
        int block_count;

        if (decoder.peek_next_block_range_int64(in, block_min, block_max,
                                                 block_count)) {
            int64_t effective_max = block_max;
            if (!use_lookahead) {
                // Recompute Plan A estimate from decoder state
                // After peek, decoder has: first_value_, write_index_,
                // bit_width_, delta_min_
                if (decoder.write_index_ == 0 || decoder.bit_width_ == 0) {
                    effective_max = decoder.first_value_ +
                        (int64_t)decoder.write_index_ * decoder.delta_min_;
                } else if (decoder.bit_width_ >= 63) {
                    effective_max = INT64_MAX;
                } else {
                    int64_t max_delta = decoder.delta_min_ +
                        ((1LL << decoder.bit_width_) - 1);
                    effective_max = decoder.first_value_ +
                        (int64_t)decoder.write_index_ * max_delta;
                }
            }

            // Check if block is entirely outside query range
            if (effective_max < query_start || block_min > query_end) {
                int skipped = 0;
                decoder.skip_peeked_block_int64(in, skipped);
                blocks_skipped++;
                continue;
            }
        }

        // Decode the block
        int actual = 0;
        decoder.read_batch_int64(buf, BATCH, actual, in);
        rows_decoded += actual;
    }

    double elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
                             Clock::now() - t0).count();
    (void)rows_decoded;
    (void)blocks_skipped;
    delete inp;
    return elapsed_us;
}

static void run_latency(int target_bw, GeneratedPage* page) {
    // 10% selectivity: query covers 10% of the total time range
    int64_t total_ts_range = page->block_last_ts.back() - page->block_first_ts[0];
    int64_t sel_range = total_ts_range / 10;

    // Place query in the middle of the time range
    int64_t mid = page->block_first_ts[0] + total_ts_range / 2;
    int64_t query_start = mid - sel_range / 2;
    int64_t query_end = mid + sel_range / 2;

    std::vector<double> latencies_planA;
    std::vector<double> latencies_planC;

    for (int r = 0; r < kLatencyRepeats; r++) {
        double la = simulate_query_latency(page, false, query_start, query_end);
        double lc = simulate_query_latency(page, true, query_start, query_end);
        latencies_planA.push_back(la);
        latencies_planC.push_back(lc);
    }

    std::sort(latencies_planA.begin(), latencies_planA.end());
    std::sort(latencies_planC.begin(), latencies_planC.end());

    int p50_idx = kLatencyRepeats / 2;
    int p95_idx = static_cast<int>(kLatencyRepeats * 0.95);

    double a_p50 = latencies_planA[p50_idx] / 1000.0;  // us -> ms
    double a_p95 = latencies_planA[p95_idx] / 1000.0;
    double c_p50 = latencies_planC[p50_idx] / 1000.0;
    double c_p95 = latencies_planC[p95_idx] / 1000.0;
    double speedup = a_p50 / std::max(c_p50, 0.001);

    gLatencyResults.push_back({target_bw, "PlanA", a_p50, a_p95, 1.0});
    gLatencyResults.push_back({target_bw, "PlanC", c_p50, c_p95, speedup});

    std::cout << "  bw=" << std::setw(2) << target_bw
              << "  PlanA_p50=" << std::fixed << std::setprecision(3)
              << a_p50 << " ms"
              << "  PlanC_p50=" << c_p50 << " ms"
              << "  speedup=" << std::setprecision(2) << speedup << "x\n";
}

// ─── CSV output ─────────────────────────────────────────────────────────────

static void write_csv() {
    // Skip rate CSV
    {
        std::string path = gCsvDir + "/skip_rate_results.csv";
        std::ofstream csv(path);
        csv << "bw,method,blocks_total,blocks_skipped,phantom_blocks,"
               "skip_rate_pct\n";
        for (auto& r : gSkipResults) {
            csv << r.target_bw << "," << r.method << ","
                << r.blocks_total << "," << r.blocks_skipped << ","
                << r.phantom_blocks << ","
                << std::fixed << std::setprecision(1) << r.skip_rate_pct
                << "\n";
        }
        std::cout << "  " << path << "\n";
    }

    // Latency CSV
    {
        std::string path = gCsvDir + "/latency_results.csv";
        std::ofstream csv(path);
        csv << "bw,method,latency_ms_p50,latency_ms_p95,speedup\n";
        for (auto& r : gLatencyResults) {
            csv << r.target_bw << "," << r.method << ","
                << std::fixed << std::setprecision(3) << r.latency_ms_p50 << ","
                << r.latency_ms_p95 << ","
                << std::setprecision(2) << r.speedup << "\n";
        }
        std::cout << "  " << path << "\n";
    }
}

// ─── Main ───────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc > 1) gCsvDir = argv[1];

    std::cout << "=== E5-4: Block-Level Time Filter Precision ===\n"
              << "  Plan A (Conservative) vs Plan C (Lookahead)\n"
              << "  blocks:     " << kNumBlocks << "\n"
              << "  block_size: " << kRowsPerBlock << " rows (1+" << kBlockSize
              << " deltas)\n"
              << "  delta_base: " << kDeltaBase << " ms\n"
              << "  bit_widths: 0, 4, 8, 12\n"
              << "  csv_dir:    " << gCsvDir << "\n\n";

    storage::libtsfile_init();
    std::mt19937_64 rng(42);

    // E5-4a: Skip rate comparison
    std::cout << "── E5-4a: Skip Rate Comparison ──\n";
    for (int b = 0; b < kNumBws; b++) {
        int bw = gTargetBws[b];
        GeneratedPage* page = generate_page(bw, rng);
        auto blocks = analyze_blocks(page);
        run_skip_rate(bw, blocks);
        delete page;
    }

    // E5-4b: Query latency comparison
    std::cout << "\n── E5-4b: Query Latency (10% selectivity, "
              << kLatencyRepeats << " repeats) ──\n";
    for (int b = 0; b < kNumBws; b++) {
        int bw = gTargetBws[b];
        GeneratedPage* page = generate_page(bw, rng);
        run_latency(bw, page);
        delete page;
    }

    std::cout << "\nCSV output:\n";
    write_csv();

    storage::libtsfile_destroy();
    return 0;
}
