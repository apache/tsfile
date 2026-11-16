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

import os

import pytest

s3fs = pytest.importorskip("s3fs")

from tsfile import (
    ColumnCategory,
    ColumnSchema,
    TableSchema,
    Tablet,
    TSDataType,
    TsFileReader,
    TsFileWriter,
)

ENDPOINT = os.environ.get("TSFILE_MINIO_ENDPOINT")
ACCESS_KEY = os.environ.get("TSFILE_MINIO_ACCESS_KEY")
SECRET_KEY = os.environ.get("TSFILE_MINIO_SECRET_KEY")
BUCKET = os.environ.get("TSFILE_MINIO_BUCKET", "tsfile-test")

pytestmark = pytest.mark.skipif(
    not (ENDPOINT and ACCESS_KEY and SECRET_KEY),
    reason="MinIO integration test needs TSFILE_MINIO_* env vars",
)


@pytest.fixture
def fs():
    return s3fs.S3FileSystem(
        endpoint_url=ENDPOINT,
        key=ACCESS_KEY,
        secret=SECRET_KEY,
    )


def _write_table(writer):
    column1 = ColumnSchema("item_id", TSDataType.STRING, ColumnCategory.TAG)
    column2 = ColumnSchema("value", TSDataType.DOUBLE)
    writer.register_table(TableSchema("test_table", [column1, column2]))

    tablet = Tablet(["item_id", "value"], [TSDataType.STRING, TSDataType.DOUBLE], 10)
    tablet.set_table_name("test_table")
    for i in range(10):
        tablet.add_timestamp(i, i)
        tablet.add_value_by_name("item_id", i, f"id{i}")
        tablet.add_value_by_name("value", i, i * 10.0)
    writer.write_table(tablet)


def test_write_to_object_storage_and_read_back(fs):
    fs.mkdir(BUCKET, exist_ok=True)
    key = f"{BUCKET}/roundtrip.tsfile"

    # The writer fills the s3fs write buffer; closing `sink` (the outer `with`)
    # triggers the multipart upload.
    with fs.open(key, "wb") as sink:
        with TsFileWriter(sink) as writer:
            _write_table(writer)

    with fs.open(key, "rb") as source:
        with TsFileReader(source) as reader:
            result = reader.query_table("test_table", ["item_id", "value"])
            try:
                seen = []
                while result.next():
                    seen.append(result.get_value_by_name("item_id"))
                assert seen == [f"id{i}" for i in range(10)]
            finally:
                result.close()
