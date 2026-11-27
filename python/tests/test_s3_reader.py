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

"""Native s3:// reads against a local HTTP origin that speaks Range."""

import http.server
import threading
from pathlib import Path

import pytest

from tsfile import TsFileReader, get_tsfile_config, s3_available, set_tsfile_config
from tsfile.exceptions import FileOpenError, FileReadError
from tsfile.s3 import resolve_s3_options

RESOURCES = Path(__file__).parent / "resources"


class RangeHandler(http.server.BaseHTTPRequestHandler):
    data = b""
    requests = []
    fail_next_gets = 0
    etag = '"local-test-etag"'
    replace_after_head = False
    ignore_range = False

    def do_HEAD(self):
        self._serve(head=True)

    def do_GET(self):
        self._serve(head=False)

    def _serve(self, head):
        type(self).requests.append(
            (
                self.command,
                self.path,
                self.headers.get("Range"),
                self.headers.get("Authorization"),
                self.headers.get("If-Match"),
            )
        )
        if "missing" in self.path:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.command == "GET" and type(self).fail_next_gets > 0:
            type(self).fail_next_gets -= 1
            self.send_response(500)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if not head and self.headers.get("If-Match") not in (None, self.etag):
            self.send_response(412)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = type(self).data
        range_header = self.headers.get("Range")
        if not self.ignore_range and range_header and range_header.startswith("bytes="):
            start_text, _, end_text = range_header[6:].partition("-")
            start = int(start_text)
            end = int(end_text) if end_text else len(data) - 1
            end = min(end, len(data) - 1)
            body = data[start : end + 1]
            self.send_response(206)
            self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
        else:
            body = data
            self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        if self.etag is not None:
            self.send_header("ETag", self.etag)
        if head and self.replace_after_head:
            type(self).etag = '"replacement"'
        self.send_header("Accept-Ranges", "bytes")
        self.end_headers()
        if not head:
            self.wfile.write(body)

    def log_message(self, *args):  # keep pytest output clean
        pass


@pytest.fixture()
def http_origin():
    handler = type(
        "Handler",
        (RangeHandler,),
        {
            "data": (RESOURCES / "simple_table_t1.tsfile").read_bytes(),
            "requests": [],
            "fail_next_gets": 0,
        },
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_address[1], handler
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def _table_rows(reader):
    result = reader.query_table("test", ["s0", "s2"])
    try:
        rows = []
        while result.next():
            rows.append(tuple(result.get_value_by_index(i) for i in range(1, 4)))
        return rows
    finally:
        result.close()


requires_native_s3 = pytest.mark.skipif(
    not s3_available(), reason="native S3 support was not built (no libcurl)"
)


@requires_native_s3
def test_s3_reader_matches_local_read(http_origin):
    port, handler = http_origin
    expected = _table_rows(TsFileReader(str(RESOURCES / "simple_table_t1.tsfile")))
    with TsFileReader(
        "s3://bucket/simple_table_t1.tsfile",
        s3={"endpoint": f"http://127.0.0.1:{port}", "region": "us-east-1"},
    ) as reader:
        assert _table_rows(reader) == expected

    gets = [r for r in handler.requests if r[0] == "GET"]
    assert gets, "the reader must fetch ranges over HTTP"
    assert all(r[2] for r in gets), "page reads must be ranged GETs"
    assert all(r[3] is None for r in gets), "anonymous reads send no Authorization"
    assert len(gets) <= 8, f"block cache should bound requests, saw {len(gets)}"


@requires_native_s3
def test_s3_reader_signs_requests_when_credentials_are_set(http_origin):
    port, handler = http_origin
    with TsFileReader(
        "s3://bucket/simple_table_t1.tsfile",
        s3={
            "endpoint": f"http://127.0.0.1:{port}",
            "region": "us-east-1",
            "access_key_id": "AKIAIOSFODNN7EXAMPLE",
            "secret_access_key": "secret",
        },
    ) as reader:
        assert _table_rows(reader)
    authorizations = {r[3] for r in handler.requests}
    assert authorizations
    for value in authorizations:
        assert value.startswith("AWS4-HMAC-SHA256 Credential=")
        assert "SignedHeaders=host;x-amz-content-sha256;x-amz-date" in value


@requires_native_s3
def test_s3_reader_retries_transient_failures(http_origin):
    port, handler = http_origin
    handler.fail_next_gets = 2
    with TsFileReader(
        "s3://bucket/simple_table_t1.tsfile",
        s3={
            "endpoint": f"http://127.0.0.1:{port}",
            "region": "us-east-1",
            "max_retries": 3,
        },
    ) as reader:
        assert _table_rows(reader)
    error_responses = [r for r in handler.requests if r[0] == "GET"]
    assert len(error_responses) >= 1
    assert handler.fail_next_gets == 0, "both injected failures were attempted"


@requires_native_s3
def test_s3_reader_reports_missing_object(http_origin):
    port, _ = http_origin
    with pytest.raises(FileOpenError):
        TsFileReader(
            "s3://bucket/missing.tsfile",
            s3={"endpoint": f"http://127.0.0.1:{port}", "region": "us-east-1"},
        )


@requires_native_s3
@pytest.mark.parametrize("ignore_range", [True, False])
def test_reads_literal_key_with_etag(http_origin, ignore_range):
    port, handler = http_origin
    handler.ignore_range = ignore_range
    key = "dir/./sub/../simple_table_t1.tsfile"
    # Force nonzero offsets even for the small fixture, including the 200 fallback.
    previous = get_tsfile_config()["read_cache_enabled_"]
    set_tsfile_config({"read_cache_enabled_": False})
    try:
        with TsFileReader(
            f"s3://bucket/{key}", s3={"endpoint": f"http://127.0.0.1:{port}"}
        ):
            pass  # Opening validates the magic at both ends of the file.
    finally:
        set_tsfile_config({"read_cache_enabled_": previous})
    assert any(r[2] and not r[2].startswith("bytes=0-") for r in handler.requests)
    assert all(request[1] == "/bucket/" + key for request in handler.requests)
    assert all(
        request[4] == handler.etag
        for request in handler.requests
        if request[0] == "GET"
    )


@requires_native_s3
def test_changed_object_is_not_retried(http_origin):
    port, handler = http_origin
    handler.replace_after_head = True
    with pytest.raises((FileOpenError, FileReadError)):
        TsFileReader("s3://bucket/file", s3={"endpoint": f"http://127.0.0.1:{port}"})
    assert [request[0] for request in handler.requests] == ["HEAD", "GET"]


@requires_native_s3
@pytest.mark.parametrize("etag", [None, 'W/"weak"'])
def test_requires_strong_etag(http_origin, etag):
    port, handler = http_origin
    handler.etag = etag
    with pytest.raises(FileOpenError, match="strong ETag"):
        TsFileReader("s3://bucket/file", s3={"endpoint": f"http://127.0.0.1:{port}"})


def test_explicit_credentials_are_a_single_set(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "env-token")
    options = {"region": "us-east-1", "access_key_id": "", "secret_access_key": ""}
    anonymous = resolve_s3_options("s3://bucket/key", options)
    assert not any(
        anonymous[name]
        for name in ("access_key_id", "secret_access_key", "session_token")
    )
    options.update(access_key_id="explicit-id", secret_access_key="explicit-secret")
    assert resolve_s3_options("s3://bucket/key", options)["session_token"] == ""
    del options["secret_access_key"]
    with pytest.raises(ValueError, match="together"):
        resolve_s3_options("s3://bucket/key", options)


def test_resolve_s3_options_validation():
    with pytest.raises(ValueError, match="endpoint"):
        resolve_s3_options("s3://bucket/key")
    with pytest.raises(TypeError, match="unsupported"):
        resolve_s3_options(
            "s3://bucket/key",
            {"endpoint": "http://127.0.0.1:1", "bogus": 1},
        )
    with pytest.raises(ValueError, match="together"):
        resolve_s3_options(
            "s3://bucket/key",
            {"endpoint": "http://127.0.0.1:1", "access_key_id": "only-ak"},
        )
    with pytest.raises(TypeError, match="path_style"):
        resolve_s3_options(
            "s3://bucket/key",
            {"endpoint": "http://127.0.0.1:1", "path_style": "yes"},
        )


def test_resolve_s3_options_derives_endpoint_from_region():
    resolved = resolve_s3_options("oss://bucket/key", {"region": "cn-beijing"})
    assert resolved["endpoint"] == "https://oss-cn-beijing.aliyuncs.com"
    resolved = resolve_s3_options("s3://bucket/key", {"region": "eu-west-1"})
    assert resolved["endpoint"] == "https://s3.eu-west-1.amazonaws.com"


def test_resolve_s3_options_reads_environment(monkeypatch):
    monkeypatch.setenv("TSFILE_S3_ENDPOINT", "http://env-endpoint:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-ak")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-sk")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    resolved = resolve_s3_options("s3://bucket/key")
    assert resolved["endpoint"] == "http://env-endpoint:9000"
    assert resolved["access_key_id"] == "env-ak"
    assert resolved["secret_access_key"] == "env-sk"
    assert resolved["region"] == "us-east-1"
    assert resolved["prefetch_threads"] == 4
