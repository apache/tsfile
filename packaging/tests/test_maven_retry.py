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

"""Exercise the Maven wrapper retry boundary with real subprocess failures."""

import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/run_maven_with_retry.py"
SPEC = importlib.util.spec_from_file_location("maven_retry", SCRIPT)
MAVEN = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MAVEN)


class MavenRetryTest(unittest.TestCase):
    def execute(self, message, failures):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / "maven.py"
            fake.write_text(
                "from pathlib import Path\nimport json, sys\n"
                "path = Path('calls.json')\n"
                "calls = json.loads(path.read_text()) if path.exists() else []\n"
                "calls.append(sys.argv[1:])\n"
                "path.write_text(json.dumps(calls))\n"
                f"if len(calls) <= {failures}:\n"
                f"    print({message!r}, flush=True)\n    sys.exit(1)\n"
                "print('[INFO] BUILD SUCCESS', flush=True)\n"
            )
            with patch.object(MAVEN.time, "sleep"):
                status = MAVEN.run_maven(
                    ["package"], root=root, command_prefix=[sys.executable, str(fake)]
                )
            return status, json.loads((root / "calls.json").read_text())

    def test_connection_reset_retries_and_refreshes_negative_cache(self):
        status, calls = self.execute(
            "[ERROR] Could not transfer artifact org.apache.maven:maven-model-builder:pom:3.0 "
            "from/to central (https://repo.maven.apache.org/maven2): Connection reset",
            1,
        )
        self.assertEqual(status, 0)
        self.assertEqual(calls, [["package"], ["-U", "package"]])

    def test_transient_http_failure_stops_after_three_attempts(self):
        status, calls = self.execute(
            "[ERROR] Could not transfer artifact example:pom:1 from/to central: "
            "status code: 503, reason phrase: Service Unavailable",
            5,
        )
        self.assertEqual(status, 1)
        self.assertEqual(len(calls), 3)

    def test_missing_artifact_does_not_retry(self):
        status, calls = self.execute(
            "[ERROR] Could not transfer artifact example:pom:1 from/to central: "
            "status code: 404, reason phrase: Not Found",
            5,
        )
        self.assertEqual(status, 1)
        self.assertEqual(len(calls), 1)

    def test_compiler_failure_does_not_retry(self):
        status, calls = self.execute(
            "[ERROR] Failed to execute goal exec-maven-plugin: compiler error", 5
        )
        self.assertEqual(status, 1)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
