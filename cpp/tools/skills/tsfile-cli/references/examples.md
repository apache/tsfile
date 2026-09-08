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

# tsfile-cli Examples

Inspect before reading rows:

```sh
tsfile-cli ls -f ndjson data.tsfile
tsfile-cli meta -f ndjson data.tsfile
tsfile-cli schema -t sensors -f csv data.tsfile
tsfile-cli count -t sensors -f csv data.tsfile
```

Read bounded data:

```sh
tsfile-cli head -t sensors -m temperature -n 20 -f csv data.tsfile
tsfile-cli cat -t sensors --tag-filter site eq north -m temperature -f ndjson data.tsfile
```

Export one object atomically:

```sh
tsfile-cli export -t sensors --type csv -o sensors.csv data.tsfile
```

Create a new table-model TsFile and verify it through a read command:

```sh
printf 'time,site,temperature\n0,north,21.5\n1,north,21.7\n' \
  | tsfile-cli write --table sensors --tag site STRING \
      --field temperature DOUBLE --stdin -o sensors.tsfile
tsfile-cli count -t sensors -f csv sensors.tsfile
```
