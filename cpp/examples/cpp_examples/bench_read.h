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
#pragma once
#include <cstdint>

/**
 * TsFile vs Parquet+Arrow baseline read benchmark.
 * Writes bench files to cwd, then measures TAG_FILTER and TIME_FILTER.
 * row_count must be a positive multiple of 10 (default: 1,000,000).
 */
// Write TsFile (and optionally Parquet) bench files to cwd.
int bench_write(int64_t row_count = 1000000, bool run_parquet = true);

// Best-effort OS page cache drop for the bench files.
// On macOS: calls `purge` (requires sudo; harmless if it fails).
// On Linux: writes to /proc/sys/vm/drop_caches (requires root).
void bench_drop_cache();

// Run read benchmarks against already-written bench files.
// run_parquet: include Parquet+Arrow comparison (set false for TsFile-only profiling).
int bench_read(int64_t row_count = 1000000, bool run_parquet = true);
