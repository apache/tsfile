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

"""Resolution of native S3/OSS read settings.

``TsFileReader("s3://bucket/key", s3={...})`` reads the object directly over
HTTP range requests.  This module turns the user-supplied mapping plus the
environment into the plain values the native layer expects.
"""

import os

OBJECT_SCHEMES = ("s3", "oss")

_CONFIG_KEYS = {
    "endpoint",
    "region",
    "access_key_id",
    "secret_access_key",
    "session_token",
    "path_style",
    "connect_timeout_ms",
    "request_timeout_ms",
    "max_retries",
    "prefetch_threads",
}

_DEFAULT_CONNECT_TIMEOUT_MS = 5000
_DEFAULT_REQUEST_TIMEOUT_MS = 60000
_DEFAULT_MAX_RETRIES = 3
_DEFAULT_PREFETCH_THREADS = 4


def is_object_uri(source):
    """True when @p source names an s3:// or oss:// object."""
    if not isinstance(source, str):
        return False
    scheme, sep, _ = source.partition("://")
    return bool(sep) and scheme.lower() in OBJECT_SCHEMES


def _first(*values):
    for value in values:
        if value:
            return value
    return None


def _check_int(name, value, minimum):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"s3 option {name!r} must be an int, got {type(value)}")
    if value < minimum:
        raise ValueError(f"s3 option {name!r} must be >= {minimum}, got {value}")
    return value


def resolve_s3_options(uri, config=None):
    """Resolve endpoint, credentials and tuning values for @p uri.

    Explicit values win over the environment.  Supported environment
    variables: TSFILE_S3_ENDPOINT, AWS_ENDPOINT_URL_S3, AWS_ENDPOINT_URL,
    TSFILE_S3_REGION, AWS_REGION, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN.
    """
    if not is_object_uri(uri):
        raise ValueError(f"expected an s3:// or oss:// URI, got: {uri!r}")
    scheme = uri.partition("://")[0].lower()
    config = dict(config or {})
    unknown = set(config) - _CONFIG_KEYS
    if unknown:
        raise TypeError(f"unsupported s3 options: {sorted(unknown)}")

    region = _first(
        config.get("region"),
        os.environ.get("TSFILE_S3_REGION"),
        os.environ.get("AWS_REGION"),
        os.environ.get("AWS_DEFAULT_REGION"),
    )
    endpoint = _first(
        config.get("endpoint"),
        os.environ.get("TSFILE_S3_ENDPOINT"),
        os.environ.get("AWS_ENDPOINT_URL_S3"),
        os.environ.get("AWS_ENDPOINT_URL"),
    )
    if endpoint is None and region:
        if scheme == "oss":
            endpoint = f"https://oss-{region}.aliyuncs.com"
        else:
            endpoint = f"https://s3.{region}.amazonaws.com"
    if endpoint is None:
        raise ValueError(
            "an S3 endpoint is required: pass s3={'endpoint': ...} or set "
            "TSFILE_S3_ENDPOINT / AWS_ENDPOINT_URL_S3, or pass a region"
        )

    access_key_id = _first(
        config.get("access_key_id"), os.environ.get("AWS_ACCESS_KEY_ID")
    )
    secret_access_key = _first(
        config.get("secret_access_key"), os.environ.get("AWS_SECRET_ACCESS_KEY")
    )
    session_token = _first(
        config.get("session_token"), os.environ.get("AWS_SESSION_TOKEN")
    )
    if bool(access_key_id) != bool(secret_access_key):
        raise ValueError(
            "access_key_id and secret_access_key must be provided together"
        )

    path_style = config.get("path_style")
    if path_style is None:
        path_style = True
    elif not isinstance(path_style, bool):
        raise TypeError(
            f"s3 option 'path_style' must be a bool, got {type(path_style)}"
        )

    return {
        "uri": uri,
        "endpoint": endpoint,
        "region": region or "",
        "access_key_id": access_key_id or "",
        "secret_access_key": secret_access_key or "",
        "session_token": session_token or "",
        "path_style": path_style,
        "connect_timeout_ms": _check_int(
            "connect_timeout_ms",
            config.get("connect_timeout_ms", _DEFAULT_CONNECT_TIMEOUT_MS),
            1,
        ),
        "request_timeout_ms": _check_int(
            "request_timeout_ms",
            config.get("request_timeout_ms", _DEFAULT_REQUEST_TIMEOUT_MS),
            1,
        ),
        "max_retries": _check_int(
            "max_retries", config.get("max_retries", _DEFAULT_MAX_RETRIES), 0
        ),
        "prefetch_threads": _check_int(
            "prefetch_threads",
            config.get("prefetch_threads", _DEFAULT_PREFETCH_THREADS),
            0,
        ),
    }
