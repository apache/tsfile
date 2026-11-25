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
#include "reader/arrow/query_scan_executor.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <functional>
#include <future>
#include <limits>
#include <numeric>
#include <set>

#include "common/thread_pool.h"
#include "reader/arrow/scan_page.h"
#include "reader/filter/table_field_filter.h"

namespace storage {
using namespace common;
namespace {
struct ScanColumn {
    std::string name;
    TSDataType type = INVALID_DATATYPE;
    bool projected = false;
    std::vector<ArrowPageLocation> pages;
    // End of each value chunk's page range, paired with its shared time chunk.
    std::vector<std::pair<size_t, size_t>> chunk_page_ends;
    // Non-aligned merge cursor. Only the current physical page is decoded.
    size_t next_page = 0;
    size_t row = 0;
    std::vector<int64_t> times;
    std::shared_ptr<ArrowColumn> values;
};
struct ScanDevice {
    DeviceQueryTask* task;
    std::unique_ptr<DeviceQueryTask> owned_task;
    std::vector<std::shared_ptr<PreparedSeries>> prepared;
    PageArena arena;
    std::vector<ITimeseriesIndex*> indexes;
    TimeseriesIndex* time_index = nullptr;
    std::vector<ArrowPageLocation> times;
    std::vector<int64_t> time_chunk_offsets;
    std::vector<size_t> time_page_starts;
    std::set<int64_t> excluded_chunks;
    std::vector<ScanColumn> columns;
    std::shared_ptr<ArrowInputRange> device_input;
    std::vector<int32_t> output_fields;
    // Logical time page -> physical page index for every FIELD, or missing.
    std::vector<std::vector<int32_t>> value_pages;
    bool aligned = false;
    size_t page_cursor = 0;
    explicit ScanDevice(DeviceQueryTask* t) : task(t) {
        arena.init(1024, MOD_TSFILE_READER);
    }
    ~ScanDevice() {
        if (!prepared.empty()) return;
        std::set<TimeseriesIndex*> owned;
        if (time_index) owned.insert(time_index);
        for (auto* index : indexes) {
            if (auto* a = dynamic_cast<AlignedTimeseriesIndex*>(index)) {
                owned.insert(a->time_ts_idx_);
                owned.insert(a->value_ts_idx_);
                a->~AlignedTimeseriesIndex();
            } else if (auto* t = dynamic_cast<TimeseriesIndex*>(index))
                owned.insert(t);
        }
        for (auto* index : owned)
            if (index) index->~TimeseriesIndex();
    }
};
struct SelectedPage {
    std::shared_ptr<ScanDevice> device;
    size_t time_page = 0;
    std::vector<int64_t> times;
    std::vector<uint32_t> selected;
    std::vector<std::shared_ptr<ArrowColumn>> cached;
    uint32_t consumed = 0;
    uint32_t logical_rows = 0;
    std::vector<uint32_t> cache_rows;
    std::vector<std::unique_ptr<ArrowValueCursor>> cursors;
};
struct OutputSlice {
    std::shared_ptr<SelectedPage> page;
    uint32_t begin;
    uint32_t count;
    uint32_t destination;
};
}  // namespace
struct QueryScanExecutor::Impl {
    std::unique_ptr<DeviceTaskIterator> tasks;
    TsFileIOReader* io;
    RandomAccessReadFile* file;
    std::vector<std::string> names;
    std::vector<TSDataType> types;
    uint32_t batch_size;
    std::unique_ptr<Filter> time_filter;
    std::unique_ptr<Filter> field_filter;
    AlignedFieldFilter* predicate;
    int64_t offset;
    int64_t limit;
    int error = E_OK;
    bool closed = false;
    std::vector<std::shared_ptr<ScanDevice>> ready_devices;
    size_t ready_device_index = 0;
    std::set<std::string> predicate_columns;
    std::set<std::string> projected_columns;

    ArrowInputBudget input_usage;
    std::vector<size_t> output_sources, distinct_outputs;
    size_t concurrency() const {
#ifdef ENABLE_THREADS
        if (g_config_value_.parallel_read_enabled_ && g_thread_pool_ &&
            ThreadPool::current_worker_id() == SIZE_MAX)
            return g_thread_pool_->num_threads();
#endif
        return 1;
    }
    // Submit at most the query's worker budget. Workers drain ready work; they
    // never submit child tasks or wait for other jobs on the shared pool.
    int run_tasks(size_t count, const std::function<int(size_t)>& work) {
        size_t workers = std::min(count, concurrency());
        if (workers <= 1) {
            for (size_t i = 0; i < count; ++i) {
                int ret = work(i);
                if (ret != E_OK) return ret;
            }
            return E_OK;
        }
#ifdef ENABLE_THREADS
        std::atomic<size_t> next{0};
        std::atomic<int> failure{E_OK};
        auto worker = [&]() {
            try {
                while (failure.load() == E_OK) {
                    size_t i = next.fetch_add(1);
                    if (i >= count) break;
                    int ret = work(i);
                    if (ret != E_OK) {
                        int expected = E_OK;
                        failure.compare_exchange_strong(expected, ret);
                    }
                }
            } catch (const std::bad_alloc&) {
                failure.store(E_OOM);
            } catch (...) {
                failure.store(E_FILE_READ_ERR);
            }
        };
        std::vector<std::future<void>> futures;
        futures.reserve(workers);
        try {
            for (size_t i = 0; i < workers; ++i)
                futures.push_back(g_thread_pool_->submit(worker));
        } catch (const std::bad_alloc&) {
            failure.store(E_OOM);
        } catch (...) {
            failure.store(E_FILE_READ_ERR);
        }
        // Drain only this query's tasks, including on submission/decode errors,
        // before any captured plan, output buffer, or file can be released.
        for (auto& future : futures) future.get();
        return failure.load();
#else
        return E_OK;
#endif
    }
    std::shared_ptr<ScanDevice> device;
    std::shared_ptr<SelectedPage> pending;

    Impl(std::unique_ptr<DeviceTaskIterator> t, TsFileIOReader* reader,
         std::vector<std::string> n, std::vector<TSDataType> ty, uint32_t size,
         Filter* tf, Filter* ff, int off, int lim)
        : tasks(std::move(t)),
          io(reader),
          file(reader->get_read_file()),
          names(std::move(n)),
          types(std::move(ty)),
          batch_size(size),
          time_filter(tf),
          field_filter(ff),
          predicate(dynamic_cast<AlignedFieldFilter*>(ff)),
          offset(off),
          limit(lim) {
        if (time_filter && time_filter->is_unbounded_time_filter()) {
            time_filter.reset();
        }
        if (!time_filter && !field_filter && off == 0 && lim < 0)
            input_usage = std::make_shared<std::atomic<size_t>>(0);
        names.insert(names.begin(), "time");
        types.insert(types.begin(), INT64);
        projected_columns.insert(names.begin() + 1, names.end());
        std::map<std::pair<std::string, TSDataType>, size_t> first;
        // The implicit timestamp has no corresponding projected FIELD.
        output_sources.push_back(0);
        distinct_outputs.push_back(0);
        for (size_t c = 1; c < names.size(); ++c) {
            auto found = first.emplace(std::make_pair(names[c], types[c]), c);
            output_sources.push_back(found.first->second);
            if (found.second) distinct_outputs.push_back(c);
        }
        if (predicate)
            for (const auto& name : predicate->referenced_columns())
                predicate_columns.insert(name);
    }
    int bind_device(const std::shared_ptr<ScanDevice>& d) {
        std::vector<std::string> requested;
        int ret = E_OK;
        if (d->prepared.empty()) {
            auto& fields =
                d->task->get_column_mapping()->get_measurement_columns();
            requested.assign(fields.begin(), fields.end());
            d->indexes.resize(requested.size(), nullptr);
            if (!fields.empty()) {
                ret = io->get_timeseries_indexes(d->task->get_device_id(),
                                                 fields, d->indexes, d->arena);
                if (ret != E_OK) return ret;
            }
            for (auto* index : d->indexes) {
                if (auto* aligned =
                        dynamic_cast<AlignedTimeseriesIndex*>(index)) {
                    d->time_index = aligned->time_ts_idx_;
                    break;
                }
            }
            if (d->time_index) {
                d->aligned = true;
            } else {
                ret = io->get_device_time_index(d->task->get_device_id(),
                                                d->time_index, d->arena);
                if (ret != E_OK && ret != E_NOT_SUPPORT) return ret;
                d->aligned = ret == E_OK;
            }
        } else {
            requested.assign(names.begin() + 1, names.end());
            for (auto& source : d->prepared)
                d->indexes.push_back(source->index());
            auto* aligned =
                dynamic_cast<AlignedTimeseriesIndex*>(d->indexes.front());
            d->aligned = aligned != nullptr;
            if (aligned) d->time_index = aligned->time_ts_idx_;
        }
        std::map<std::string, size_t> positions;
        for (const auto& name : requested) {
            ScanColumn column;
            column.name = name;
            column.projected = projected_columns.count(name) != 0;
            column.type =
                d->task->get_table_schema()->get_column_schema(name).data_type_;
            positions.emplace(name, d->columns.size());
            d->columns.push_back(std::move(column));
        }
        d->output_fields.assign(names.size(), -1);
        for (size_t c = 1; c < names.size(); ++c) {
            auto position = positions.find(names[c]);
            if (position != positions.end())
                d->output_fields[c] = position->second;
        }
        return E_OK;
    }
    int prefetch_dense_device(ScanDevice& d) {
        if (!input_usage || !d.aligned || d.columns.size() < 512 ||
            d.columns.size() * 2 <
                d.task->get_table_schema()->get_columns_num())
            return E_OK;
        std::vector<int64_t> offsets;
        auto collect = [&](SimpleList<ChunkMeta*>* chunks) {
            if (!chunks) return;
            for (auto it = chunks->begin(); it != chunks->end(); it++) {
                int64_t pos = it.get()->offset_of_chunk_header_;
                offsets.push_back(pos);
            }
        };
        if (d.time_index) collect(d.time_index->get_chunk_meta_list());
        for (auto* idx : d.indexes) {
            if (idx && idx->is_aligned())
                collect(idx->get_value_chunk_meta_list());
        }
        if (offsets.size() < 2) return E_OK;
        std::sort(offsets.begin(), offsets.end());
        const int64_t first = offsets.front(), last = offsets.back();
        if (first < 0 || last <= first || last > file->file_size() ||
            uint64_t(last - first) > ArrowInputRange::MAX_QUERY_BYTES)
            return E_OK;
        for (size_t i = 1; i < offsets.size(); ++i)
            if (offsets[i] - offsets[i - 1] > 1024 * 1024) return E_OK;
        // The final header has no following offset. Plan that chunk separately.
        return ArrowInputRange::read(*file, input_usage, first, last - first,
                                     d.device_input);
    }
    bool time_range_matches(const Statistic* stat) const {
        return !time_filter || !stat ||
               time_filter->satisfy_start_end_time(stat->start_time_,
                                                   stat->end_time_);
    }
    // Use the shared clock for aligned chunks: a value statistic omits NULLs.
    // Evaluate the caller's filter before starting parallel page planning.
    void select_chunks(ScanDevice& d) {
        auto select = [&](ITimeseriesIndex* index, bool clock) {
            auto* chunks = index->get_chunk_meta_list();
            if (!chunks) return;
            for (auto it = chunks->begin(); it != chunks->end(); it++) {
                auto* chunk = it.get();
                auto offset = chunk->offset_of_chunk_header_;
                if (clock) d.time_chunk_offsets.push_back(offset);
                auto* stat = chunk->statistic_;
                if (!stat && chunks->size() == 1) stat = index->get_statistic();
                if (!time_range_matches(stat)) d.excluded_chunks.insert(offset);
            }
        };
        if (d.aligned) {
            select(d.time_index, true);
        } else if (time_filter) {
            for (auto* index : d.indexes)
                if (index) select(index, false);
        }
    }
    int plan_column(ScanDevice& d, size_t c) {
        const bool clock = c == d.columns.size();
        auto* index = clock ? d.time_index : d.indexes[c];
        if (!index) return E_OK;
        auto* chunks = index->is_aligned() ? index->get_value_chunk_meta_list()
                                           : index->get_chunk_meta_list();
        if (!chunks) return E_OK;
        auto& pages = clock ? d.times : d.columns[c].pages;
        auto* single_stat =
            chunks->size() == 1 ? index->get_statistic() : nullptr;
        size_t ordinal = 0;
        for (auto it = chunks->begin(); it != chunks->end(); it++, ++ordinal) {
            auto* chunk = it.get();
            size_t group = ordinal;
            int64_t offset = chunk->offset_of_chunk_header_;
            if (clock) {
                d.time_page_starts.push_back(pages.size());
            } else if (d.aligned) {
                // Complete value lists pair by ordinal, including empty chunks.
                // Older sparse layouts omit a value chunk in some chunk groups.
                if (chunks->size() != d.time_chunk_offsets.size()) {
                    auto next =
                        std::upper_bound(d.time_chunk_offsets.begin(),
                                         d.time_chunk_offsets.end(), offset);
                    if (next == d.time_chunk_offsets.begin())
                        return E_TSFILE_CORRUPTED;
                    group = next - d.time_chunk_offsets.begin() - 1;
                }
                offset = d.time_chunk_offsets[group];
            }
            if (d.excluded_chunks.count(offset)) continue;
            int ret = plan_arrow_pages(*file, *chunk, single_stat, pages,
                                       input_usage, d.device_input);
            if (ret != E_OK) return ret;
            if (!clock && d.aligned)
                d.columns[c].chunk_page_ends.emplace_back(group, pages.size());
        }
        if (clock) d.time_page_starts.push_back(pages.size());
        return E_OK;
    }
    int align_pages(const std::shared_ptr<ScanDevice>& d) {
        if (!d->aligned) return E_OK;
        d->value_pages.resize(d->times.size(),
                              std::vector<int32_t>(d->columns.size(), -1));
        for (size_t c = 0; c < d->columns.size(); ++c) {
            size_t first = 0;
            for (const auto& chunk : d->columns[c].chunk_page_ends) {
                size_t time_page = d->time_page_starts[chunk.first];
                size_t end = d->time_page_starts[chunk.first + 1];
                for (; first < chunk.second; ++first, ++time_page) {
                    if (time_page >= end || d->value_pages[time_page][c] != -1)
                        return E_TSFILE_CORRUPTED;
                    d->value_pages[time_page][c] = first;
                }
            }
        }
        return E_OK;
    }
    int prepare_window(std::vector<std::shared_ptr<ScanDevice>>& devices) {
        int ret = run_tasks(devices.size(),
                            [&](size_t i) { return bind_device(devices[i]); });
        if (ret != E_OK) return ret;
        for (const auto& d : devices) {
            select_chunks(*d);
            ret = prefetch_dense_device(*d);
            if (ret != E_OK) return ret;
        }
        std::vector<std::pair<ScanDevice*, size_t>> columns;
        for (const auto& d : devices)
            for (size_t c = 0; c <= d->columns.size(); ++c)
                columns.emplace_back(d.get(), c);
        ret = run_tasks(columns.size(), [&](size_t i) {
            return plan_column(*columns[i].first, columns[i].second);
        });
        if (ret != E_OK) return ret;
        return run_tasks(devices.size(),
                         [&](size_t i) { return align_pages(devices[i]); });
    }
    int next_device() {
        device.reset();
        if (!tasks) return E_NO_MORE_DATA;
        if (ready_device_index == ready_devices.size()) {
            ready_devices.clear();
            ready_device_index = 0;
            size_t count = (input_usage && names.size() >= 1024)
                               ? 1
                               : std::min<size_t>(8, concurrency());
            if (limit > 0) count = std::min<size_t>(count, limit);
            while (ready_devices.size() < count) {
                bool available = false;
                int ret = tasks->has_next(available);
                if (ret != E_OK) return ret;
                if (!available) break;
                DeviceQueryTask* task = nullptr;
                ret = tasks->next(task);
                if (ret != E_OK) return ret;
                if (!task) return E_OOM;
                ready_devices.emplace_back(new ScanDevice(task));
            }
            if (ready_devices.empty()) return E_NO_MORE_DATA;
            int ret = prepare_window(ready_devices);
            if (ret != E_OK) return ret;
        }
        device = std::move(ready_devices[ready_device_index++]);
        return E_OK;
    }
    int decode_cached(SelectedPage& page, size_t c) {
        if (page.cached[c]) return E_OK;
        const auto& d = *page.device;
        page.cached[c] =
            std::make_shared<ArrowColumn>(d.columns[c].type, page.times.size());
        int32_t physical = d.value_pages[page.time_page][c];
        if (physical < 0) return E_OK;
        std::vector<uint32_t> rows(page.times.size());
        std::iota(rows.begin(), rows.end(), 0);
        int ret = decode_arrow_values(*file, d.columns[c].pages[physical], true,
                                      page.times.size(), rows, 0, rows.size(),
                                      *page.cached[c], 0);
        page.cached[c]->finish(page.times.size());
        return ret;
    }
    int filter_page(SelectedPage& page, bool all_match = false) {
        SelectionMask mask(page.times.size());
        mask.set_all();
        if (predicate && !all_match) {
            std::vector<AlignedFieldFilter::ColumnView> views;
            for (size_t c = 0; c < page.device->columns.size(); ++c) {
                AlignedFieldFilter::ColumnView view;
                view.name = page.device->columns[c].name;
                view.type = page.device->columns[c].type;
                view.arrow_column = page.cached[c].get();
                views.push_back(view);
            }
            int ret =
                predicate->evaluate_columns(views, page.times.size(), mask);
            if (ret != E_OK) return ret;
        }
        for (uint32_t r = 0; r < page.times.size(); ++r) {
            if (!mask.test(r) ||
                (time_filter && !time_filter->satisfy_start_end_time(
                                    page.times[r], page.times[r])))
                continue;
            if (field_filter && !predicate) {
                std::vector<TableFieldValue> values;
                std::vector<std::array<char, 8>> scratch(page.cached.size());
                for (size_t c = 0; c < page.cached.size(); ++c) {
                    uint32_t size;
                    const char* data = page.cached[c]->native_value(
                        r, scratch[c].data(), size);
                    values.push_back({page.device->columns[c].name,
                                      page.device->columns[c].type, data,
                                      size});
                }
                bool matched = false;
                int ret = evaluate_table_field_filter(
                    *field_filter, {page.times[r], values}, matched);
                if (ret != E_OK) return ret;
                if (!matched) continue;
            }
            page.selected.push_back(r);
        }
        page.logical_rows = page.times.size();
        page.cache_rows.resize(page.selected.size());
        std::iota(page.cache_rows.begin(), page.cache_rows.end(), 0);
        for (size_t c = 0; c < page.cached.size(); ++c) {
            if (!page.cached[c]) continue;
            if (!page.device->columns[c].projected || page.selected.empty()) {
                page.cached[c].reset();
                continue;
            }
            if (page.selected.size() != page.times.size()) {
                auto compact = std::make_shared<ArrowColumn>(
                    page.cached[c]->type(), page.selected.size());
                int ret = compact->copy_selected(*page.cached[c], page.selected,
                                                 0, page.selected.size(), 0);
                if (ret != E_OK) return ret;
                compact->finish(page.selected.size());
                page.cached[c] = std::move(compact);
            }
        }
        if (page.selected.size() != page.times.size()) {
            std::vector<int64_t> accepted;
            accepted.reserve(page.selected.size());
            for (uint32_t row : page.selected)
                accepted.push_back(page.times[row]);
            page.times = std::move(accepted);
        }
        return E_OK;
    }
    int aligned_page(std::shared_ptr<SelectedPage>& page) {
        if (device->page_cursor == device->times.size()) return E_NO_MORE_DATA;
        page = std::make_shared<SelectedPage>();
        page->device = device;
        page->time_page = device->page_cursor++;
        const auto& time = device->times[page->time_page];
        if (!time_range_matches(time.statistic.get())) return E_OK;
        bool all_match = false;
        if (predicate) {
            std::map<std::string, Statistic*> stats;
            for (size_t c = 0; c < device->columns.size(); ++c) {
                if (!predicate_columns.count(device->columns[c].name)) continue;
                int p = device->value_pages[page->time_page][c];
                stats[device->columns[c].name] =
                    p < 0 ? nullptr
                          : device->columns[c].pages[p].statistic.get();
            }
            int64_t rows = time.statistic ? time.statistic->count_ : 0;
            if (!predicate->may_match(stats, rows)) return E_OK;
            all_match = predicate->all_match(stats, rows);
        }
        int ret = decode_arrow_times(*file, time, true, page->times);
        if (ret != E_OK) return ret;
        page->cached.resize(device->columns.size());
        page->cursors.resize(device->columns.size());
        if (field_filter && !all_match) {
            ret = run_tasks(device->columns.size(), [&](size_t c) {
                if (predicate &&
                    !predicate_columns.count(device->columns[c].name))
                    return E_OK;
                return decode_cached(*page, c);
            });
            if (ret != E_OK) return ret;
        }
        return filter_page(*page, all_match);
    }

    int refill(ScanColumn& column) {
        while (column.row == column.times.size()) {
            column.times.clear();
            column.values.reset();
            column.row = 0;
            if (column.next_page == column.pages.size()) return E_NO_MORE_DATA;
            const auto& page = column.pages[column.next_page++];
            if (!time_range_matches(page.statistic.get())) continue;
            int ret = decode_arrow_times(*file, page, false, column.times);
            if (ret != E_OK) return ret;
            column.values =
                std::make_shared<ArrowColumn>(column.type, column.times.size());
            std::vector<uint32_t> rows(column.times.size());
            std::iota(rows.begin(), rows.end(), 0);
            ret = decode_arrow_values(*file, page, false, rows.size(), rows, 0,
                                      rows.size(), *column.values, 0);
            if (ret != E_OK) return ret;
            column.values->finish(rows.size());
        }
        return E_OK;
    }
    int merged_page(std::shared_ptr<SelectedPage>& page) {
        // Non-aligned series use their union of timestamps. This bounded merge
        // window is independent of the public result batch size.
        const uint32_t window = 1024;
        page = std::make_shared<SelectedPage>();
        page->device = device;
        for (const auto& column : device->columns)
            page->cached.emplace_back(new ArrowColumn(column.type, window));
        for (uint32_t row = 0; row < window; ++row) {
            int64_t time = INT64_MAX;
            bool found = false;
            for (auto& column : device->columns) {
                int ret = refill(column);
                if (ret == E_NO_MORE_DATA) continue;
                if (ret != E_OK) return ret;
                time = std::min(time, column.times[column.row]);
                found = true;
            }
            if (!found) break;
            page->times.push_back(time);
            for (size_t c = 0; c < device->columns.size(); ++c) {
                auto& column = device->columns[c];
                if (column.row < column.times.size() &&
                    column.times[column.row] == time) {
                    char scratch[8];
                    uint32_t size;
                    const char* data = column.values->native_value(
                        column.row++, scratch, size);
                    int ret = page->cached[c]->set_native(row, column.type,
                                                          data, size);
                    if (ret != E_OK) return ret;
                }
            }
        }
        if (page->times.empty()) return E_NO_MORE_DATA;
        for (auto& column : page->cached) column->finish(page->times.size());
        return filter_page(*page);
    }
    int next_page() {
        pending.reset();
        for (;;) {
            if (!device) {
                int ret = next_device();
                if (ret != E_OK) return ret;
            }
            int ret =
                device->aligned ? aligned_page(pending) : merged_page(pending);
            if (ret == E_NO_MORE_DATA) {
                device.reset();
                continue;
            }
            if (ret != E_OK) return ret;
            if (pending->selected.empty()) continue;
            if (offset > 0) {
                uint32_t skip =
                    std::min<int64_t>(offset, pending->selected.size());
                pending->consumed = skip;
                offset -= skip;
                if (pending->consumed == pending->selected.size()) continue;
            }
            return E_OK;
        }
    }
    int write_column(size_t output, const std::vector<OutputSlice>& slices,
                     ArrowColumn& column) {
        ArrowReadWindow window;
        std::vector<int64_t> read_ends(slices.size(), 0);
        const ArrowPageLocation* next_page = nullptr;
        // Coalesce only adjacent payloads needed by this output batch. Stop at
        // skipped pages, cached predicate values and device/chunk boundaries.
        for (size_t i = slices.size(); i-- > 0;) {
            auto& page = *slices[i].page;
            auto& d = *page.device;
            int32_t c = d.output_fields[output];
            const ArrowPageLocation* current = nullptr;
            if (c >= 0 && d.aligned && !page.cached[c]) {
                int physical = d.value_pages[page.time_page][c];
                if (physical >= 0) current = &d.columns[c].pages[physical];
            }
            if (current) {
                read_ends[i] = current->offset + current->compressed_size;
                if (next_page &&
                    current->chunk_offset == next_page->chunk_offset &&
                    read_ends[i] == next_page->header_offset)
                    read_ends[i] = read_ends[i + 1];
            }
            next_page = current;
        }
        for (size_t i = 0; i < slices.size(); ++i) {
            const auto& slice = slices[i];
            auto& page = *slice.page;
            auto& d = *page.device;
            auto schema = d.task->get_table_schema();
            const auto& name = names[output];
            if (output == 0 || d.task->get_column_mapping()->is_time(name)) {
                std::memcpy(column.mutable_values() +
                                uint64_t(slice.destination) * sizeof(int64_t),
                            page.times.data() + slice.begin,
                            uint64_t(slice.count) * sizeof(int64_t));
                column.set_valid_range(slice.destination, slice.count);
            } else if (d.task->get_column_mapping()->is_tag(name)) {
                int pos = schema->find_id_column_order(name) +
                          !schema->is_virtual_table();
                auto id = d.task->get_device_id();
                std::string* tag =
                    pos >= 0 && size_t(pos) < id->get_split_seg_num()
                        ? id->get_split_segname_at(pos)
                        : nullptr;
                for (uint32_t i = 0; i < slice.count; ++i) {
                    int ret = column.set_native(slice.destination + i, STRING,
                                                tag ? tag->data() : nullptr,
                                                tag ? tag->size() : 0);
                    if (ret != E_OK) return ret;
                }
            } else {
                int32_t c = d.output_fields[output];
                if (c < 0) continue;
                int ret = E_OK;
                if (page.cached[c]) {
                    ret = column.copy_selected(*page.cached[c], page.cache_rows,
                                               slice.begin, slice.count,
                                               slice.destination);
                } else {
                    int physical = d.value_pages[page.time_page][c];
                    if (physical >= 0) {
                        if (!page.cursors[c])
                            page.cursors[c].reset(new ArrowValueCursor(
                                *file, d.columns[c].pages[physical], true,
                                page.logical_rows));
                        ret = page.cursors[c]->write(
                            page.selected, slice.begin, slice.count, column,
                            slice.destination, &window, read_ends[i]);
                        // Slices retain their pages until every column finishes
                        // the output batch. Release completed payloads now so a
                        // sparse selection cannot pin all contributing pages.
                        // Keep codec state only if a later batch can resume it.
                        if (slice.begin + slice.count == page.selected.size() ||
                            limit == 0)
                            page.cursors[c].reset();
                    }
                }
                if (ret != E_OK) return ret;
            }
        }
        return E_OK;
    }
    int next(std::shared_ptr<ArrowBatch>& result) {
        result.reset();
        if (closed || limit == 0) return E_NO_MORE_DATA;
        if (error != E_OK) return error;
        std::vector<OutputSlice> slices;
        uint32_t rows = 0;
        while (rows < batch_size && limit != 0) {
            if (!pending || pending->consumed == pending->selected.size()) {
                int ret = next_page();
                if (ret == E_NO_MORE_DATA) break;
                if (ret != E_OK) return ret;
            }
            uint32_t count =
                std::min<uint32_t>(batch_size - rows, pending->selected.size() -
                                                          pending->consumed);
            if (limit >= 0) count = std::min<int64_t>(count, limit);
            slices.push_back({pending, pending->consumed, count, rows});
            pending->consumed += count;
            rows += count;
            if (limit >= 0) limit -= count;
        }
        if (rows == 0) return E_NO_MORE_DATA;
        auto batch = std::make_shared<ArrowBatch>(names, types, rows);
        batch->row_count = rows;
        // Duplicate projections share an immutable result column and are
        // decoded once. Each distinct column has exactly one writer.
        for (size_t c = 0; c < output_sources.size(); ++c)
            if (c != output_sources[c])
                batch->columns[c] = batch->columns[output_sources[c]];
        int ret = run_tasks(distinct_outputs.size(), [&](size_t i) {
            size_t c = distinct_outputs[i];
            return write_column(c, slices, *batch->columns[c]);
        });
        if (ret != E_OK) return ret;
        batch->finish();
        result = std::move(batch);
        return E_OK;
    }
};
QueryScanExecutor::QueryScanExecutor(std::unique_ptr<DeviceTaskIterator> tasks,
                                     TsFileIOReader* io,
                                     std::vector<std::string> names,
                                     std::vector<TSDataType> types,
                                     uint32_t batch_size, Filter* time_filter,
                                     Filter* field_filter, int offset,
                                     int limit)
    : impl_(new Impl(std::move(tasks), io, std::move(names), std::move(types),
                     batch_size, time_filter, field_filter, offset, limit)) {}
QueryScanExecutor::~QueryScanExecutor() = default;
int QueryScanExecutor::bind_prepared(
    const std::vector<std::shared_ptr<PreparedSeries>>& prepared) {
    if (prepared.empty() || impl_->names.size() != prepared.size() + 1)
        return E_INVALID_ARG;
    for (size_t i = 0; i < prepared.size(); ++i) {
        const auto& p = prepared[i];
        if (!p || !p->index()) return E_INVALID_ARG;
        if (p->index()->get_data_type() == INVALID_DATATYPE ||
            p->index()->get_data_type() == VECTOR)
            return E_TYPE_NOT_SUPPORTED;
        if (prepared.size() > 1) {
            const auto& a = prepared.front()->generation();
            const auto& b = p->generation();
            const auto& x = prepared.front()->locator();
            const auto& y = p->locator();
            if (!p->index()->is_aligned() || y.layout != 1 ||
                y.time_metadata_length == 0)
                return E_NOT_SUPPORT;
            if (a.mapped_index_identity != b.mapped_index_identity ||
                a.file_id != b.file_id || a.file_size != b.file_size ||
                a.file_fingerprint != b.file_fingerprint ||
                x.time_metadata_offset != y.time_metadata_offset ||
                x.time_metadata_length != y.time_metadata_length)
                return E_INVALID_ARG;
        }
    }
    std::vector<ColumnSchema> columns;
    for (size_t i = 1; i < impl_->names.size(); ++i)
        columns.emplace_back(impl_->names[i], impl_->types[i],
                             ColumnCategory::FIELD);
    auto schema = std::make_shared<TableSchema>("prepared", columns);
    auto mapping = std::make_shared<ColumnMapping>();
    for (size_t i = 1; i < impl_->names.size(); ++i)
        mapping->add(impl_->names[i], i - 1, *schema);
    std::unique_ptr<DeviceQueryTask> task(new DeviceQueryTask(
        nullptr,
        std::vector<std::string>(impl_->names.begin() + 1, impl_->names.end()),
        mapping, nullptr, schema));
    impl_->device = std::make_shared<ScanDevice>(task.get());
    impl_->device->owned_task = std::move(task);
    impl_->device->prepared = prepared;
    std::vector<std::shared_ptr<ScanDevice>> devices{impl_->device};
    return impl_->prepare_window(devices);
}
int QueryScanExecutor::next(std::shared_ptr<ArrowBatch>& batch) {
    if (impl_->error != E_OK) {
        batch.reset();
        return impl_->error;
    }
    int ret;
    try {
        ret = impl_->next(batch);
    } catch (const std::bad_alloc&) {
        ret = E_OOM;
    } catch (...) {
        ret = E_FILE_READ_ERR;
    }
    if (ret != E_OK && ret != E_NO_MORE_DATA) {
        impl_->error = ret;
        batch.reset();
    }
    return ret;
}
void QueryScanExecutor::close() {
    impl_->closed = true;
    impl_->pending.reset();
    impl_->device.reset();
    impl_->ready_devices.clear();
    impl_->tasks.reset();
}
}  // namespace storage
