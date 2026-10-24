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

"""Resolve per-runtime read options before opening dataset resources."""

import operator
import os

DEFAULT_CACHE_SIZE = 4096


def resolve_read_options(
    *,
    max_prepared_series=None,
    descriptor_cache_size=None,
    max_open_files=None,
    query_workers=None,
    query_parallel_min_rows=None,
):
    """Use explicit values, then environment variables, then built-in defaults."""
    defaults = {
        "max_prepared_series": (max_prepared_series, DEFAULT_CACHE_SIZE, 0),
        "descriptor_cache_size": (descriptor_cache_size, DEFAULT_CACHE_SIZE, 0),
        "max_open_files": (max_open_files, 16, 1),
        "query_workers": (query_workers, min(4, os.cpu_count() or 1), 1),
        "query_parallel_min_rows": (query_parallel_min_rows, 8192, 1),
    }
    options = {}
    for name, (value, default, minimum) in defaults.items():
        source = name
        if value is None:
            source = "TSFILE_DATAFRAME_" + name.upper()
            raw = os.environ.get(source)
            if raw is None:
                value = default
            else:
                try:
                    value = int(raw)
                except ValueError as exc:
                    raise ValueError(f"{source} must be an integer") from exc
        else:
            if isinstance(value, bool):
                raise TypeError(f"{source} must be an integer, not bool")
            try:
                value = operator.index(value)
            except TypeError as exc:
                raise TypeError(f"{source} must be an integer") from exc
        if value < minimum:
            requirement = "non-negative" if minimum == 0 else "positive"
            raise ValueError(f"{source} must be {requirement}")
        options[name] = value
    return options
