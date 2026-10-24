#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Validate an installed CLI with the current source checkout's Python resources."""

import argparse
import csv
import hashlib
import io
import math
from pathlib import Path
import shutil
import subprocess
import tempfile

# Stable fixtures from python/tests/resources in the source checkout.
# Expected data facts come from python/tests/test_load_tsfile_from_iotdb.py.
CASES = (
    {
        "file": "simple_tree.tsfile",
        "sha256": "bd221e61be15ac57b6197622b3a302015b2bf25a6d3cab84f31e74d919bbec26",
        "model": "tree",
        "object": "root.ln.wf01.wt01",
        "rows": 105,
        "schema": {"status": ("FIELD", "BOOLEAN"), "temperature": ("FIELD", "FLOAT")},
        "nulls": {"status": 5, "temperature": 5},
        "sums": {"time": 15960},
    },
    {
        "file": "simple_table_t1.tsfile",
        "sha256": "a7b7736dbe414d68eff90e792db93669738817a58bd3d560801a3ec5a7b3edef",
        "model": "table",
        "object": "test",
        "rows": 60,
        "schema": {
            "s0": ("TAG", "STRING"),
            "s1": ("TAG", "STRING"),
            "s2": ("FIELD", "INT64"),
            "s3": ("FIELD", "FLOAT"),
            "s4": ("FIELD", "STRING"),
            "s5": ("FIELD", "TIMESTAMP"),
            "s6": ("FIELD", "INT32"),
            "s7": ("FIELD", "DOUBLE"),
            "s8": ("FIELD", "DATE"),
            "s9": ("FIELD", "TEXT"),
        },
        "nulls": {"s2": 8, "s6": 8, "s9": 8},
        "sums": {"s5": 63270, "s6": 3342},
    },
    {
        "file": "simple_table_t2.tsfile",
        "sha256": "9bea5c683ef22ee01bf3c31c8a066bfdfe8db9cedee5a3360772547980508d3a",
        "model": "table",
        "object": "test",
        "rows": 40,
        "schema": {
            "s0": ("TAG", "STRING"),
            "s1": ("TAG", "STRING"),
            "s2": ("FIELD", "INT64"),
        },
        "nulls": {"s2": 5, "s6": 5, "s9": 5},
        "sums": {"time": 70404242080000, "s2": 36450, "s5": 41680, "s6": 1800},
    },
    {
        "file": "table_with_time_column.tsfile",
        "sha256": "22006050d75559466e653d61c4e8182f8f2531c667f2ca7e234b43648cc696fd",
        "model": "table",
        "object": "table2",
        "rows": 25,
        "schema": {
            "id": ("TIME", "TIMESTAMP"),
            "region_id": ("TAG", "STRING"),
            "temperature": ("FIELD", "FLOAT"),
            "humidity": ("FIELD", "DOUBLE"),
        },
        "nulls": {"temperature": 0, "humidity": 0},
        "sums": {},
    },
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def csv_rows(output):
    reader = csv.DictReader(io.StringIO(output))
    require(reader.fieldnames, "CLI returned CSV without a header")
    rows = list(reader)
    require(
        all(None not in row and None not in row.values() for row in rows),
        "Malformed CSV rows",
    )
    return rows


def column(rows, name):
    # Tree outputs may qualify the measurement with its device path.
    matches = [key for key in rows[0] if key == name or key.endswith("." + name)]
    require(len(matches) == 1, f"Expected exactly one column for {name}: {matches}")
    return [row[matches[0]] for row in rows]


def check_case(cli, resources, work, case, timeout):
    source = resources / case["file"]
    require(source.is_file(), f"Missing Python test resource: {source}")
    require(
        hashlib.sha256(source.read_bytes()).hexdigest() == case["sha256"],
        f"Resource differs from the expected Python test fixture: {source}",
    )
    print(f"Testing {case['file']} with {cli}", flush=True)

    def run(*arguments):
        command = [str(cli), *map(str, arguments)]
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=timeout,
            cwd=work,
        )
        require(
            result.returncode == 0,
            f"Command failed ({result.returncode}): {command}\nstdout: {result.stdout}\nstderr: {result.stderr}",
        )
        require(not result.stderr, f"Unexpected stderr from {command}: {result.stderr}")
        return result.stdout

    objects = csv_rows(run("ls", "-f", "csv", source))
    require(
        objects == [{"model": case["model"], "object": case["object"]}],
        f"Unexpected objects: {objects}",
    )
    metadata = csv_rows(run("meta", "-f", "csv", source))
    require(
        len(metadata) == 1 and metadata[0]["model"] == case["model"],
        f"Unexpected metadata: {metadata}",
    )
    require(
        int(metadata[0]["size_bytes"]) == source.stat().st_size
        and int(metadata[0]["format_version"]) == 4,
        f"Invalid file metadata: {metadata}",
    )
    schema = {
        row["column"]: (row["category"], row["data_type"])
        for row in csv_rows(run("schema", "-f", "csv", source))
    }
    for name, expected in case["schema"].items():
        require(
            schema.get(name) == expected,
            f"Unexpected schema for {name}: {schema.get(name)}",
        )

    statistics = csv_rows(run("stats", "-f", "csv", source))
    require(statistics, "CLI returned no field statistics")
    counts = csv_rows(run("count", "-f", "csv", source))
    scope = ("-d" if case["model"] == "tree" else "-t", case["object"])
    content = run("cat", *scope, "-f", "csv", source)
    rows = csv_rows(content)
    require(len(rows) == case["rows"], f"Expected {case['rows']} rows, got {len(rows)}")
    for name, expected_nulls in case["nulls"].items():
        values = column(rows, name)
        require(
            values.count("\\N") == expected_nulls, f"Unexpected null count for {name}"
        )
        matching = [row for row in counts if row["column"].split(".")[-1] == name]
        require(len(matching) == 1, f"Missing/duplicate count for {name}")
        require(
            int(matching[0]["row_count"]) == case["rows"]
            and int(matching[0]["null_count"]) == expected_nulls
            and int(matching[0]["non_null_count"]) == case["rows"] - expected_nulls,
            f"Incorrect count result for {name}: {matching}",
        )
    for name, expected_sum in case["sums"].items():
        actual_sum = sum(int(value) for value in column(rows, name) if value != "\\N")
        require(actual_sum == expected_sum, f"Unexpected sum for {name}: {actual_sum}")
    if case["model"] == "tree":
        values = column(rows, "status")
        require(
            values.count("true") == 50 and values.count("false") == 50,
            "Incorrect BOOLEAN values",
        )
    if case["object"] == "table2":
        require(
            column(rows, "region_id") == ["loc"] * 25, "Incorrect region TAG values"
        )
        for name in ("temperature", "humidity"):
            require(
                math.isclose(sum(map(float, column(rows, name))), 2.5, rel_tol=1e-6),
                f"Incorrect FLOAT/DOUBLE values for {name}",
            )

    require(
        csv_rows(run("head", *scope, "-n", "3", "-f", "csv", source)) == rows[:3],
        "head differs from first three cat rows",
    )
    require(
        csv_rows(run("cat", *scope, "--offset", "1", "-n", "2", "-f", "csv", source))
        == rows[1:3],
        "Incorrect offset/limit",
    )
    exported = work / (case["file"] + ".csv")
    require(
        run("export", *scope, "--type", "csv", "-o", exported, source) == "",
        "export unexpectedly wrote to stdout",
    )
    require(
        exported.read_text(encoding="utf-8") == content,
        "Exported CSV differs from cat output",
    )
    require("TsFile" in run("sketch", source), "CLI returned no TsFile sketch")
    print(
        f"PASS: {case['file']}; {len(rows)} rows, schema/counts/values/query/export checked",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--cli", required=True, help="Installed tsfile-cli executable or command"
    )
    parser.add_argument(
        "--resources",
        required=True,
        type=Path,
        help="Python test .tsfile resources from the source checkout",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30,
        help="Timeout in seconds per CLI invocation",
    )
    args = parser.parse_args()
    cli = shutil.which(args.cli)
    require(cli is not None, f"CLI executable not found: {args.cli}")
    require(args.timeout > 0, "Timeout must be positive")
    version = subprocess.run(
        [cli, "--version"],
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=args.timeout,
    )
    print(version.stdout.strip(), flush=True)
    with tempfile.TemporaryDirectory(prefix="tsfile-cli-install-") as temporary:
        for case in CASES:
            check_case(
                Path(cli).resolve(),
                args.resources.resolve(),
                Path(temporary),
                case,
                args.timeout,
            )
    print("PASS: installed CLI validated with all four Python test resources")


if __name__ == "__main__":
    main()
