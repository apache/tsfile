<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# MinIO integration test

This directory provisions a MinIO test cluster and documents how to run the
object-storage write/read integration test.

## Why MinIO

The C++ core has no HTTP client, so TsFile writes to object storage go through
the Python binding: `TsFileWriter(target)` accepts a writable binary file
object, and fsspec/s3fs supplies one that uploads to S3-compatible storage on
close. MinIO provides that S3-compatible endpoint locally, without a cloud
account.

## Bring up the cluster

```bash
docker compose up -d
```

This starts a single MinIO node with four drives (erasure coding enabled), the
lightweight equivalent of a distributed cluster for test purposes. The S3
endpoint is `http://127.0.0.1:9000` and the console is
`http://127.0.0.1:9001`. Credentials are the MinIO defaults (`minioadmin` /
`minioadmin`).

## Install the S3 client

The test needs `s3fs` (and its `fsspec` dependency), which is not part of the
core Python dependencies:

```bash
pip install s3fs
```

## Run the integration test

```bash
TSFILE_MINIO_ENDPOINT=http://127.0.0.1:9000 \
TSFILE_MINIO_ACCESS_KEY=minioadmin \
TSFILE_MINIO_SECRET_KEY=minioadmin \
TSFILE_MINIO_BUCKET=tsfile-test \
pytest tests/test_minio_integration.py -v
```

Without `s3fs` installed or the `TSFILE_MINIO_*` variables set, the test is
skipped rather than failed.

## What the test covers

The test writes a TsFile into the object store through `TsFileWriter` (the new
write-sink bridge), lets s3fs upload it on close, then reads it back through
`TsFileReader` and verifies the rows. This exercises the same C++ write/read
paths a non-local filesystem would.
