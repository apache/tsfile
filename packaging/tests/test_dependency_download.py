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

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import subprocess
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[2]
HELPER = ROOT / "cpp/cmake/DownloadWithRetry.cmake"


class DependencyDownloadTest(unittest.TestCase):
    def download(self, responses):
        state = {"requests": 0}
        payload = b"verified upstream archive payload"

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                index = state["requests"]
                state["requests"] += 1
                status = responses[min(index, len(responses) - 1)]
                self.send_response(status)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            try:
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    destination = root / "archive.part"
                    status_file = root / "status.txt"
                    script = root / "download.cmake"
                    script.write_text(
                        "cmake_minimum_required(VERSION 3.11)\n"
                        f'include("{HELPER.as_posix()}")\n'
                        f'tsfile_download_with_retry("http://127.0.0.1:{server.server_port}/archive" '
                        f'"{destination.as_posix()}" status)\n'
                        f'file(WRITE "{status_file.as_posix()}" "${{status}}")\n'
                    )
                    subprocess.run(
                        ["cmake", "-P", str(script)],
                        check=True,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        timeout=30,
                    )
                    status = int(status_file.read_text().split(";", 1)[0])
                    contents = (
                        destination.read_bytes() if destination.exists() else None
                    )
                    return status, state["requests"], contents, payload
            finally:
                server.shutdown()
                worker.join(timeout=5)

    def test_transient_server_failures_retry_then_succeed(self):
        status, requests, contents, payload = self.download([503, 503, 200])
        self.assertEqual(status, 0)
        self.assertEqual(requests, 3)
        self.assertEqual(contents, payload)

    def test_repeated_failures_stop_after_three_attempts_and_remove_partial_file(self):
        status, requests, contents, _ = self.download([503])
        self.assertNotEqual(status, 0)
        self.assertEqual(requests, 3)
        self.assertIsNone(contents)

    def test_permanent_http_error_is_not_retried(self):
        status, requests, contents, _ = self.download([404])
        self.assertNotEqual(status, 0)
        self.assertEqual(requests, 1)
        self.assertIsNone(contents)

    def test_success_does_not_repeat_the_download(self):
        status, requests, contents, payload = self.download([200])
        self.assertEqual(status, 0)
        self.assertEqual(requests, 1)
        self.assertEqual(contents, payload)


if __name__ == "__main__":
    unittest.main()
