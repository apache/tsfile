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

"""Retry Maven only when artifact downloads fail with transient transport errors."""

from collections import deque
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
TRANSIENT_DOWNLOAD = re.compile(
    r"Could not transfer artifact .*?(?:Connection reset|Connection timed out|"
    r"Read timed out|Remote host terminated the handshake|"
    r"status code: (?:408|429|500|502|503|504)(?:\D|$))",
    re.IGNORECASE,
)


def run_maven(arguments, *, root=ROOT, command_prefix=None):
    if command_prefix is None:
        command_prefix = [str(root / ("mvnw.cmd" if os.name == "nt" else "mvnw"))]
    for attempt in range(1, 4):
        # Maven caches failed resolutions. Force a new resolution on retry.
        options = ["-U"] if attempt > 1 and "-U" not in arguments else []
        command = [*command_prefix, *options, *arguments]
        print("+ " + subprocess.list2cmdline(command), flush=True)
        tail = deque(maxlen=200)
        with subprocess.Popen(
            command,
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        ) as process:
            for line in process.stdout:
                print(line, end="", flush=True)
                tail.append(line)
            returncode = process.wait()
        if returncode == 0:
            return 0
        errors = "".join(line for line in tail if "[ERROR]" in line)
        if attempt == 3 or TRANSIENT_DOWNLOAD.search(errors) is None:
            return returncode
        print(
            f"Transient Maven artifact download failure; retry {attempt}/2", flush=True
        )
        time.sleep(attempt * 2)
    raise AssertionError("Unreachable Maven retry state")


if __name__ == "__main__":
    raise SystemExit(run_maven(sys.argv[1:]))
