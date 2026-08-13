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

"""Reproducible cold-build and multi-process TsFileDataFrame benchmark.

This is intentionally a standalone benchmark rather than a pytest test.  It
uses only the Python standard library for process coordination and Linux
``/proc`` accounting, so it can run in the project's wheel-building virtual
environment without installing psutil.
"""

from __future__ import annotations

import argparse
import gc
import json
import multiprocessing as mp
import os
import platform
import resource
import statistics
import sys
import time
from typing import Dict

from tsfile import TsFileDataFrame
from tsfile.dataset.index import INDEX_FILE_NAME


def _index_path(dataset: str) -> str:
    return os.path.join(os.path.abspath(dataset), INDEX_FILE_NAME)


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


def _percentile(values, fraction: float) -> float:
    ordered = sorted(values)
    position = max(0, min(len(ordered) - 1, int(len(ordered) * fraction) - 1))
    return ordered[position]


def _worker(dataset: str, query_rows: int, query_repeats: int, ready, release) -> None:
    started = time.perf_counter_ns()
    dataframe = None
    series = None
    try:
        dataframe = TsFileDataFrame(dataset, show_progress=False)
        constructed = time.perf_counter_ns()
        series = dataframe[0]
        query_started = time.perf_counter_ns()
        values = series[: min(query_rows, len(series))]
        query_finished = time.perf_counter_ns()
        repeat_query_ms = []
        for _ in range(query_repeats):
            repeat_started = time.perf_counter_ns()
            values = series[: min(query_rows, len(series))]
            repeat_query_ms.append(
                (time.perf_counter_ns() - repeat_started) / 1_000_000
            )
        gc.collect()
        ready.send(
            {
                "pid": os.getpid(),
                "construct_ms": (constructed - started) / 1_000_000,
                "first_query_ms": (query_finished - query_started) / 1_000_000,
                "repeat_query_ms_median": statistics.median(repeat_query_ms),
                "repeat_query_ms_p95": _percentile(repeat_query_ms, 0.95),
                "query_repeats": query_repeats,
                "query_rows": int(len(values)),
                "series_count": len(dataframe),
                "first_series_count": len(series),
                "model": dataframe.model,
            }
        )
        release.wait()
    except BaseException as exc:
        ready.send(
            {
                "pid": os.getpid(),
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        release.wait()
        raise
    finally:
        if series is not None:
            series.close()
        if dataframe is not None:
            dataframe.close()


def _run_group(
    dataset: str, process_count: int, query_rows: int, query_repeats: int
) -> dict:
    context = mp.get_context("spawn")
    release = context.Event()
    processes = []
    receivers = []
    group_started = time.perf_counter_ns()
    for _ in range(process_count):
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_worker,
            args=(dataset, query_rows, query_repeats, sender, release),
        )
        process.start()
        sender.close()
        processes.append(process)
        receivers.append(receiver)

    worker_results = [receiver.recv() for receiver in receivers]
    ready_ms = (time.perf_counter_ns() - group_started) / 1_000_000
    if any("error" in result for result in worker_results):
        release.set()
        for process in processes:
            process.join()
        raise RuntimeError(f"worker failed: {worker_results}")

    memory = {
        str(result["pid"]): _process_memory_kib(result["pid"])
        for result in worker_results
    }
    totals = {
        key: sum(process_memory.get(key, 0) for process_memory in memory.values())
        for key in ("Rss", "Pss", "Uss", "Shared", "OpenFDs")
    }

    release.set()
    for process in processes:
        process.join()
        if process.exitcode != 0:
            raise RuntimeError(
                f"benchmark worker {process.pid} exited with {process.exitcode}"
            )

    return {
        "processes": process_count,
        "group_ready_ms": ready_ms,
        "construct_ms": [result["construct_ms"] for result in worker_results],
        "construct_ms_median": statistics.median(
            result["construct_ms"] for result in worker_results
        ),
        "first_query_ms": [result["first_query_ms"] for result in worker_results],
        "first_query_ms_median": statistics.median(
            result["first_query_ms"] for result in worker_results
        ),
        "repeat_query_ms_median": statistics.median(
            result["repeat_query_ms_median"] for result in worker_results
        ),
        "repeat_query_ms_p95_max": max(
            result["repeat_query_ms_p95"] for result in worker_results
        ),
        "worker_results": worker_results,
        "memory_kib_by_pid": memory,
        "memory_kib_total": totals,
    }


def _build_index(dataset: str) -> dict:
    path = _index_path(dataset)
    existed = os.path.exists(path)
    started = time.perf_counter_ns()
    with TsFileDataFrame(dataset, show_progress=True) as dataframe:
        result = {
            "model": dataframe.model,
            "series_count": len(dataframe),
            "file_count": len(dataframe._paths),
        }
    finished = time.perf_counter_ns()
    result.update(
        {
            "index_path": path,
            "index_existed_before": existed,
            "index_size_bytes": os.path.getsize(path),
            "elapsed_ms": (finished - started) / 1_000_000,
            "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        }
    )
    return result


def _machine() -> dict:
    return {
        "hostname": platform.node(),
        "platform": platform.platform(),
        "python": sys.version,
        "cpu_count": os.cpu_count(),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("dataset")
    parser.add_argument(
        "--mode", choices=("build", "multiprocess", "all"), default="all"
    )
    parser.add_argument("--processes", default="1,2,4")
    parser.add_argument("--query-rows", type=int, default=256)
    parser.add_argument("--query-repeats", type=int, default=20)
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.query_rows < 0 or args.query_repeats < 1:
        parser.error("--query-rows must be non-negative and --query-repeats positive")

    dataset = os.path.abspath(args.dataset)
    report = {
        "machine": _machine(),
        "dataset": dataset,
        "dataset_bytes": sum(
            os.path.getsize(os.path.join(root, name))
            for root, _, names in os.walk(dataset)
            for name in names
            if name.endswith(".tsfile")
        ),
        "index_path": _index_path(dataset),
    }
    if args.mode in ("build", "all"):
        report["build"] = _build_index(dataset)
    if args.mode in ("multiprocess", "all"):
        if not os.path.exists(report["index_path"]):
            raise FileNotFoundError(
                f"hot-start benchmark requires {report['index_path']}"
            )
        report["groups"] = [
            _run_group(dataset, count, args.query_rows, args.query_repeats)
            for count in (int(value) for value in args.processes.split(","))
        ]

    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.write("\n")


if __name__ == "__main__":
    main()
