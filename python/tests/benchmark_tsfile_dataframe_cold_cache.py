# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Cold/hot-cache, random-series TsFileDataFrame comparison benchmark.

The benchmark intentionally imports ``tsfile`` only in spawned workers.  This
lets the coordinator execute ``sync`` and Linux ``drop_caches`` before every
process-count group without importing the implementation under test first.

Each worker uses a deterministic, worker-specific random seed.  Worker 0
therefore uses the same positions in the 1, 2, and 4 process groups, while the
additional workers exercise different series.  After the cold random pass,
each worker repeats the exact same positions in the same order without
dropping caches, producing a directly comparable hot-cache pass.  The output
includes every position so two implementations can be checked for an
identical workload.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import importlib.metadata
import json
import multiprocessing as mp
import os
import platform
import random
import statistics
import sys
import time
from typing import Any, Dict


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    rank = max(1, int(len(ordered) * fraction + 0.999999999))
    return ordered[min(len(ordered), rank) - 1]


def _summary(values: list[float]) -> dict[str, float | int]:
    return {
        "count": len(values),
        "mean_ms": statistics.fmean(values),
        "p50_ms": statistics.median(values),
        "p95_ms": _percentile(values, 0.95),
        "p99_ms": _percentile(values, 0.99),
        "max_ms": max(values),
    }


def _meminfo_kib() -> Dict[str, int]:
    values: Dict[str, int] = {}
    accepted = {
        "MemTotal",
        "MemFree",
        "MemAvailable",
        "Buffers",
        "Cached",
        "SReclaimable",
        "Shmem",
        "SwapTotal",
        "SwapFree",
    }
    with open("/proc/meminfo", encoding="ascii") as stream:
        for line in stream:
            name, separator, remainder = line.partition(":")
            if not separator or name not in accepted:
                continue
            token = remainder.strip().split(maxsplit=1)[0]
            if token.isdigit():
                values[name] = int(token)
    return values


def _drop_os_caches() -> dict[str, Any]:
    before_sync = _meminfo_kib()
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w", encoding="ascii") as stream:
        stream.write("3\n")
    after_drop = _meminfo_kib()
    return {
        "method": "os.sync(); write 3 to /proc/sys/vm/drop_caches",
        "before_sync_kib": before_sync,
        "after_drop_kib": after_drop,
    }


def _process_memory_kib(pid: int) -> Dict[str, int]:
    values: Dict[str, int] = {}
    accepted = {
        "Rss",
        "Pss",
        "Pss_Dirty",
        "Pss_Anon",
        "Pss_File",
        "Pss_Shmem",
        "Shared_Clean",
        "Shared_Dirty",
        "Private_Clean",
        "Private_Dirty",
        "Anonymous",
        "Swap",
        "SwapPss",
    }
    with open(f"/proc/{pid}/smaps_rollup", encoding="ascii") as stream:
        for line in stream:
            name, separator, remainder = line.partition(":")
            if not separator or name not in accepted:
                continue
            token = remainder.strip().split(maxsplit=1)[0]
            if token.isdigit():
                values[name] = int(token)
    values["Uss"] = values.get("Private_Clean", 0) + values.get("Private_Dirty", 0)
    values["Shared"] = values.get("Shared_Clean", 0) + values.get("Shared_Dirty", 0)
    values["OpenFDs"] = len(os.listdir(f"/proc/{pid}/fd"))
    return values


def _memory_snapshot(worker_results: list[dict[str, Any]]) -> dict[str, Any]:
    by_pid = {
        str(result["pid"]): _process_memory_kib(result["pid"])
        for result in worker_results
    }
    totals = {
        key: sum(process_memory.get(key, 0) for process_memory in by_pid.values())
        for key in ("Rss", "Pss", "Uss", "Shared", "OpenFDs")
    }
    return {"by_pid": by_pid, "total": totals}


def _read_window(dataframe, position: int, query_rows: int) -> tuple[float, int]:
    series = None
    started = time.perf_counter_ns()
    try:
        series = dataframe[position]
        values = series[:query_rows]
        elapsed_ms = (time.perf_counter_ns() - started) / 1_000_000
        rows = len(values)
        del values
        return elapsed_ms, rows
    finally:
        if series is not None:
            close = getattr(series, "close", None)
            if close is not None:
                close()


def _positions_hash(positions: list[int]) -> str:
    rendered = ",".join(str(position) for position in positions).encode("ascii")
    return hashlib.sha256(rendered).hexdigest()


def _worker(
    dataset: str,
    worker_index: int,
    planned_positions: list[int] | None,
    sample_count: int,
    query_rows: int,
    seed: int,
    first_start,
    random_start,
    hot_start,
    release,
    sender,
) -> None:
    dataframe = None
    try:
        from tsfile import TsFileDataFrame

        construct_started = time.perf_counter_ns()
        dataframe = TsFileDataFrame(dataset, show_progress=False)
        construct_ms = (time.perf_counter_ns() - construct_started) / 1_000_000
        worker_seed = None if planned_positions is not None else seed + worker_index
        positions = planned_positions
        if positions is None:
            positions = random.Random(worker_seed).sample(
                range(len(dataframe)), sample_count + 1
            )
        if len(positions) != sample_count + 1:
            raise ValueError(
                f"worker {worker_index} needs {sample_count + 1} positions, "
                f"got {len(positions)}"
            )
        if min(positions) < 0 or max(positions) >= len(dataframe):
            raise ValueError(f"worker {worker_index} workload position is out of range")
        common = {
            "pid": os.getpid(),
            "worker_index": worker_index,
            "worker_seed": worker_seed,
            "construct_ms": construct_ms,
            "series_count": len(dataframe),
            "file_count": len(dataframe._paths),
            "first_position": positions[0],
            "random_positions": positions[1:],
            "positions_sha256": _positions_hash(positions),
        }
        sender.send({"phase": "constructed", **common})

        first_start.wait()
        first_ms, first_rows = _read_window(dataframe, positions[0], query_rows)
        sender.send(
            {
                "phase": "first_complete",
                **common,
                "first_query_ms": first_ms,
                "first_query_rows": first_rows,
            }
        )

        random_start.wait()
        random_started = time.perf_counter_ns()
        random_ms = []
        random_rows = []
        for position in positions[1:]:
            elapsed_ms, rows = _read_window(dataframe, position, query_rows)
            random_ms.append(elapsed_ms)
            random_rows.append(rows)
        random_wall_ms = (time.perf_counter_ns() - random_started) / 1_000_000
        gc.collect()
        sender.send(
            {
                "phase": "random_complete",
                **common,
                "first_query_ms": first_ms,
                "first_query_rows": first_rows,
                "random_query_ms": random_ms,
                "random_query_summary": _summary(random_ms),
                "random_query_rows_min": min(random_rows),
                "random_query_rows_max": max(random_rows),
                "random_query_rows_total": sum(random_rows),
                "random_wall_ms": random_wall_ms,
            }
        )

        hot_start.wait()
        hot_started = time.perf_counter_ns()
        hot_ms = []
        hot_rows = []
        for position in positions[1:]:
            elapsed_ms, rows = _read_window(dataframe, position, query_rows)
            hot_ms.append(elapsed_ms)
            hot_rows.append(rows)
        hot_wall_ms = (time.perf_counter_ns() - hot_started) / 1_000_000
        gc.collect()
        sender.send(
            {
                "phase": "hot_complete",
                **common,
                "hot_query_ms": hot_ms,
                "hot_query_summary": _summary(hot_ms),
                "hot_query_rows_min": min(hot_rows),
                "hot_query_rows_max": max(hot_rows),
                "hot_query_rows_total": sum(hot_rows),
                "hot_wall_ms": hot_wall_ms,
            }
        )
        release.wait()
    except BaseException as exc:
        try:
            sender.send(
                {
                    "phase": "error",
                    "pid": os.getpid(),
                    "worker_index": worker_index,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
        finally:
            release.wait()
        raise
    finally:
        if dataframe is not None:
            dataframe.close()


def _receive_phase(receivers, phase: str) -> list[dict[str, Any]]:
    results = [receiver.recv() for receiver in receivers]
    errors = [result for result in results if result.get("phase") == "error"]
    if errors:
        raise RuntimeError(f"workers failed before {phase}: {errors}")
    unexpected = [result for result in results if result.get("phase") != phase]
    if unexpected:
        raise RuntimeError(f"expected phase {phase}, received {unexpected}")
    return results


def _run_group(
    dataset: str,
    process_count: int,
    sample_count: int,
    query_rows: int,
    seed: int,
    drop_os_cache: bool,
    workload_plan: dict[int, list[int]] | None,
) -> dict[str, Any]:
    cache_drop = _drop_os_caches() if drop_os_cache else None
    print(
        f"starting cold-cache group: processes={process_count}",
        file=sys.stderr,
        flush=True,
    )
    context = mp.get_context("spawn")
    first_start = context.Event()
    random_start = context.Event()
    hot_start = context.Event()
    release = context.Event()
    processes = []
    receivers = []
    group_started = time.perf_counter_ns()
    for worker_index in range(process_count):
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_worker,
            args=(
                dataset,
                worker_index,
                None if workload_plan is None else workload_plan[worker_index],
                sample_count,
                query_rows,
                seed,
                first_start,
                random_start,
                hot_start,
                release,
                sender,
            ),
        )
        process.start()
        sender.close()
        processes.append(process)
        receivers.append(receiver)

    try:
        constructed = _receive_phase(receivers, "constructed")
        constructed_wall_ms = (time.perf_counter_ns() - group_started) / 1_000_000
        memory_before_query = _memory_snapshot(constructed)

        first_phase_started = time.perf_counter_ns()
        first_start.set()
        first_complete = _receive_phase(receivers, "first_complete")
        first_phase_wall_ms = (time.perf_counter_ns() - first_phase_started) / 1_000_000

        random_phase_started = time.perf_counter_ns()
        random_start.set()
        random_complete = _receive_phase(receivers, "random_complete")
        random_phase_wall_ms = (
            time.perf_counter_ns() - random_phase_started
        ) / 1_000_000
        memory_after_random_query = _memory_snapshot(random_complete)

        hot_phase_started = time.perf_counter_ns()
        hot_start.set()
        hot_complete = _receive_phase(receivers, "hot_complete")
        hot_phase_wall_ms = (
            time.perf_counter_ns() - hot_phase_started
        ) / 1_000_000
        memory_after_hot_query = _memory_snapshot(hot_complete)

        first_ms = [result["first_query_ms"] for result in first_complete]
        random_ms = [
            elapsed
            for result in random_complete
            for elapsed in result["random_query_ms"]
        ]
        hot_ms = [
            elapsed
            for result in hot_complete
            for elapsed in result["hot_query_ms"]
        ]
        query_wall_ms = max(result["random_wall_ms"] for result in random_complete)
        hot_query_wall_ms = max(result["hot_wall_ms"] for result in hot_complete)
        if any(result["first_query_rows"] != query_rows for result in first_complete):
            raise RuntimeError(f"first query did not return {query_rows} rows")
        if any(
            result["random_query_rows_min"] != query_rows
            or result["random_query_rows_max"] != query_rows
            for result in random_complete
        ):
            raise RuntimeError(f"random query did not return {query_rows} rows")
        if any(
            result["hot_query_rows_min"] != query_rows
            or result["hot_query_rows_max"] != query_rows
            for result in hot_complete
        ):
            raise RuntimeError(f"hot query did not return {query_rows} rows")
        if len({result["series_count"] for result in constructed}) != 1:
            raise RuntimeError("workers observed different series counts")

        result = {
            "processes": process_count,
            "cache_drop": cache_drop,
            "constructed_wall_ms": constructed_wall_ms,
            "construct_ms": _summary(
                [result["construct_ms"] for result in constructed]
            ),
            "series_count": constructed[0]["series_count"],
            "file_count": constructed[0]["file_count"],
            "workload": [
                {
                    "worker_index": item["worker_index"],
                    "worker_seed": item["worker_seed"],
                    "first_position": item["first_position"],
                    "random_positions": item["random_positions"],
                    "positions_sha256": item["positions_sha256"],
                }
                for item in constructed
            ],
            "first_query": {
                "phase_wall_ms": first_phase_wall_ms,
                "latency": _summary(first_ms),
                "rows_per_worker": query_rows,
                "workers": first_complete,
            },
            "random_query": {
                "cache_state": "cold first pass over random_positions",
                "query_wall_ms": query_wall_ms,
                "coordinator_phase_wall_ms_including_worker_gc": random_phase_wall_ms,
                "aggregate_qps": len(random_ms) / (query_wall_ms / 1000),
                "latency": _summary(random_ms),
                "query_count": len(random_ms),
                "rows_per_query": query_rows,
                "rows_total": len(random_ms) * query_rows,
                "workers": random_complete,
            },
            "hot_query": {
                "cache_state": (
                    "immediate second pass over the same random_positions "
                    "in the same order without dropping caches"
                ),
                "query_wall_ms": hot_query_wall_ms,
                "coordinator_phase_wall_ms_including_worker_gc": hot_phase_wall_ms,
                "aggregate_qps": len(hot_ms) / (hot_query_wall_ms / 1000),
                "latency": _summary(hot_ms),
                "query_count": len(hot_ms),
                "rows_per_query": query_rows,
                "rows_total": len(hot_ms) * query_rows,
                "workers": hot_complete,
            },
            "memory_before_query_kib": memory_before_query,
            "memory_after_random_query_kib": memory_after_random_query,
            "memory_after_hot_query_kib": memory_after_hot_query,
        }
        print(
            "completed cold-cache group: "
            f"processes={process_count}, "
            f"first_p50={result['first_query']['latency']['p50_ms']:.3f} ms, "
            f"cold_random_p50={result['random_query']['latency']['p50_ms']:.3f} ms, "
            f"hot_p50={result['hot_query']['latency']['p50_ms']:.3f} ms",
            file=sys.stderr,
            flush=True,
        )
        return result
    finally:
        release.set()
        for process in processes:
            process.join()
        bad = [process for process in processes if process.exitcode != 0]
        if bad and sys.exc_info()[0] is None:
            raise RuntimeError(
                "benchmark workers exited abnormally: "
                + ", ".join(f"{process.pid}={process.exitcode}" for process in bad)
            )


def _dataset_stats(dataset: str) -> dict[str, int]:
    paths = [
        os.path.join(root, name)
        for root, _, names in os.walk(dataset)
        for name in names
        if name.endswith(".tsfile")
    ]
    return {
        "tsfile_count": len(paths),
        "tsfile_bytes": sum(os.path.getsize(path) for path in paths),
    }


def _rejection_key(exc: Exception) -> str:
    message = str(exc).split(" for series ", maxsplit=1)[0]
    return f"{type(exc).__name__}: {message}"


def _generate_workload_plan(
    dataset: str,
    worker_count: int,
    sample_count: int,
    query_rows: int,
    seed: int,
) -> dict[str, Any]:
    from tsfile import TsFileDataFrame

    required = worker_count * (sample_count + 1)
    accepted: list[int] = []
    attempted: set[int] = set()
    rejected: dict[str, int] = {}
    dataframe = TsFileDataFrame(dataset, show_progress=False)
    try:
        random_source = random.Random(seed)
        max_attempts = min(len(dataframe), required * 100)
        while len(accepted) < required and len(attempted) < max_attempts:
            position = random_source.randrange(len(dataframe))
            if position in attempted:
                continue
            attempted.add(position)
            try:
                _, rows = _read_window(dataframe, position, query_rows)
                if rows != query_rows:
                    key = f"short result: {rows} rows"
                    rejected[key] = rejected.get(key, 0) + 1
                    continue
            except Exception as exc:
                key = _rejection_key(exc)
                rejected[key] = rejected.get(key, 0) + 1
                continue
            accepted.append(position)
            if len(accepted) % 100 == 0 or len(accepted) == required:
                print(
                    f"workload plan: accepted {len(accepted)}/{required}, "
                    f"attempted {len(attempted)}",
                    file=sys.stderr,
                    flush=True,
                )
        if len(accepted) != required:
            raise RuntimeError(
                f"found only {len(accepted)} valid positions after "
                f"{len(attempted)} attempts; need {required}"
            )
        workers = []
        width = sample_count + 1
        for worker_index in range(worker_count):
            positions = accepted[worker_index * width : (worker_index + 1) * width]
            workers.append(
                {
                    "worker_index": worker_index,
                    "positions": positions,
                    "positions_sha256": _positions_hash(positions),
                }
            )
        return {
            "dataset": dataset,
            "generated_with_package_version": importlib.metadata.version("tsfile"),
            "query_rows": query_rows,
            "sample_count_per_worker": sample_count,
            "worker_count": worker_count,
            "seed": seed,
            "series_count": len(dataframe),
            "file_count": len(dataframe._paths),
            "attempted_candidate_count": len(attempted),
            "accepted_position_count": len(accepted),
            "rejected_candidate_count": len(attempted) - len(accepted),
            "rejected_by_reason": rejected,
            "workers": workers,
        }
    finally:
        dataframe.close()


def _load_workload_plan(
    path: str, sample_count: int, query_rows: int
) -> tuple[dict[str, Any], dict[int, list[int]]]:
    with open(path, encoding="utf-8") as stream:
        raw = json.load(stream)
    if raw.get("query_rows") != query_rows:
        raise ValueError(
            f"workload plan query_rows={raw.get('query_rows')}, expected {query_rows}"
        )
    if raw.get("sample_count_per_worker") != sample_count:
        raise ValueError(
            "workload plan sample_count_per_worker="
            f"{raw.get('sample_count_per_worker')}, expected {sample_count}"
        )
    workers = {
        int(worker["worker_index"]): [int(value) for value in worker["positions"]]
        for worker in raw["workers"]
    }
    if any(len(set(positions)) != len(positions) for positions in workers.values()):
        raise ValueError("workload plan contains duplicate positions within a worker")
    flattened = [position for positions in workers.values() for position in positions]
    if len(set(flattened)) != len(flattened):
        raise ValueError("workload plan contains positions shared by workers")
    return raw, workers


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset")
    parser.add_argument("--processes", default="1,2,4")
    parser.add_argument("--sample-count", type=int, default=128)
    parser.add_argument("--query-rows", type=int, default=2880)
    parser.add_argument("--seed", type=int, default=20260814)
    parser.add_argument("--label")
    parser.add_argument("--output")
    parser.add_argument("--workload-plan")
    parser.add_argument("--generate-workload-plan")
    parser.add_argument("--plan-worker-count", type=int, default=4)
    parser.add_argument(
        "--drop-os-cache",
        action="store_true",
        help="run sync and write 3 to /proc/sys/vm/drop_caches before every group",
    )
    args = parser.parse_args()
    if args.sample_count < 1 or args.query_rows < 1:
        parser.error("sample count and query rows must be positive")
    if args.drop_os_cache and os.geteuid() != 0:
        parser.error("--drop-os-cache requires root on Linux")

    dataset = os.path.abspath(args.dataset)
    if args.generate_workload_plan:
        plan = _generate_workload_plan(
            dataset,
            args.plan_worker_count,
            args.sample_count,
            args.query_rows,
            args.seed,
        )
        rendered = json.dumps(plan, ensure_ascii=False, indent=2)
        print(rendered)
        with open(args.generate_workload_plan, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.write("\n")
        return

    raw_workload_plan = None
    workload_plan = None
    if args.workload_plan:
        raw_workload_plan, workload_plan = _load_workload_plan(
            args.workload_plan, args.sample_count, args.query_rows
        )
        requested_processes = [int(value) for value in args.processes.split(",")]
        missing_workers = [
            worker_index
            for worker_index in range(max(requested_processes))
            if worker_index not in workload_plan
        ]
        if missing_workers:
            parser.error(f"workload plan is missing workers {missing_workers}")
    else:
        requested_processes = [int(value) for value in args.processes.split(",")]
    report = {
        "label": args.label,
        "package_version": importlib.metadata.version("tsfile"),
        "python": sys.version,
        "machine": {
            "hostname": platform.node(),
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
            "meminfo_kib": _meminfo_kib(),
        },
        "dataset": dataset,
        "dataset_stats": _dataset_stats(dataset),
        "sample_count_per_worker": args.sample_count,
        "query_rows": args.query_rows,
        "seed": args.seed,
        "workload_plan_path": args.workload_plan,
        "workload_plan_metadata": (
            None
            if raw_workload_plan is None
            else {
                key: value
                for key, value in raw_workload_plan.items()
                if key != "workers"
            }
        ),
        "drop_os_cache_before_every_group": args.drop_os_cache,
        "groups": [
            _run_group(
                dataset,
                process_count,
                args.sample_count,
                args.query_rows,
                args.seed,
                args.drop_os_cache,
                workload_plan,
            )
            for process_count in requested_processes
        ],
    }
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.write("\n")


if __name__ == "__main__":
    main()
