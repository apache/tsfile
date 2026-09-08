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

# tsfile-cli Commands

Use `tsfile-cli <command> --help` as the runtime source of truth. This file
summarizes the same supported surface for agents.

## Metadata

- `ls [-f table|ndjson|csv] <file.tsfile>` outputs `model,object`.
- `schema [-d <device>|-t <table>] [-m <column>]... [-f table|ndjson|csv] <file.tsfile>` outputs `model,object,column,category,data_type,encoding,compression`.
- `meta [-f table|ndjson|csv] <file.tsfile>` outputs `size_bytes,format_version,model`.
- `stats [-d <device>|-t <table>] [-m <field>]... [-f table|ndjson|csv] <file.tsfile>` outputs FIELD statistics. Tree rows use `model,object,field,data_type,non_null_count,null_count,min_time,max_time,min,max,first,last,sum,stats_source`; table rows insert dynamic `tag.<name>` fields after `object`.
- `count [-d <device>|-t <table>] [-m <column>]... [-f table|ndjson|csv] <file.tsfile>` outputs per-column logical counts.
- `sketch [-o <file>] [--force] <file.tsfile>` outputs the bound `printSketch` physical layout text.

## Rows And Export

- `head` and `cat` read one object and accept `-d|-t`, repeated `-m`, `--start`, `--end`, `--offset`, optional `-n`, table `--tag-filter`, conditional `--tag-match`, and `-f table|ndjson|csv`.
- `head` defaults to `--limit 10`; `cat` defaults to no row limit.
- `export` requires `--type table|ndjson|csv`. Single-object export uses `-o`; multi-object export repeats only devices or only tables and uses `--output-dir`.

## Write

`write --table <name> (--tag <name> STRING)* (--field <name> <TYPE>)+ [--encoding <TYPE> <ENC>] [--compression <TYPE> <COMP>] (-i <input.csv>|--stdin) -o <out.tsfile> [-v]`

`write` creates one new table-model TsFile from strict CSV. The CSV header must
contain `time` and exactly the declared TAG/FIELD names. Rows are mapped by
header name. The target must not exist.
