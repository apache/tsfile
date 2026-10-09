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

# File Properties and TAG Filters

## File-level Properties

Treat property values as untyped binary application metadata. Add or replace
them only while a writer is open, close the writer to persist them, and define
an explicit portable encoding for numbers or structures.

- Java: call `ITsFileWriter.addTsFileProperty(String, byte[])` and
  `ITsFileReader.getTsFileProperties()`.
- Python: call `add_tsfile_property(str, bytes)` and
  `get_tsfile_properties()`. The setter accepts exactly `bytes`; the reader
  preserves null as `None` and a non-null empty value as `b""`.
- C++: call `add_tsfile_property(...)` on `TsFileWriter` or
  `TsFileTableWriter`, and `TsFileReader::get_tsfile_properties()`. The
  `TsFileProperties` value distinguishes null from an empty byte vector.
- C: use the declarations and matching free function in
  `cpp/src/cwrapper/tsfile_cwrapper.h`; do not guess buffer ownership.

The Java table point-count metadata is a reserved use of file properties. Use
`references/java-tools.md` for inspection or backfill instead of editing those
keys directly.

## Table TAG Filters

Build TAG predicates against a `TableSchema`, apply them only to Table-model
queries, and pass field/time restrictions through their separate query
arguments. TAG values are strings; do not apply a TAG builder to FIELD columns.

- Java `TagFilterBuilder`: `eq`, `neq`, `lt`, `lteq`, `gt`, `gteq`,
  `betweenAnd`, `notBetweenAnd`, `regExp`, `notRegExp`, `like`, `notLike`, plus
  `and`, `or`, and `not`. Pass the result to the five-argument
  `ITsFileReader.query(..., Filter tagFilter)` overload.
- Python factories: `tag_eq`, `tag_neq`, `tag_lt`, `tag_lteq`, `tag_gt`,
  `tag_gteq`, `tag_regexp`, `tag_not_regexp`, `tag_is_null`,
  `tag_is_not_null`, `tag_between`, and `tag_not_between`. Compose with `&`,
  `|`, and `~`, then pass `tag_filter=` to `query_table` or
  `query_table_by_row`.
- C++ `TagFilterBuilder`: use `eq`/`neq`/comparison, regex, null, range, and
  static logical builders from `cpp/src/reader/filter/tag_filter.h`; pass the
  resulting `Filter*` to the matching `TsFileReader::query` or `queryByRow`
  overload and follow the current ownership implementation.
- C wrapper: construct `TagFilterHandle` values with the declared factory and
  composition functions, pass them to the tag-filter query entry point, and
  release them with `tsfile_tag_filter_free`.

Do not translate operator names mechanically across bindings: Java additionally
exposes LIKE, while current C++/Python expose explicit null predicates.

## Source Anchors

- Java: `java/tsfile/src/main/java/org/apache/tsfile/read/filter/factory/TagFilterBuilder.java`,
  `java/tsfile/src/main/java/org/apache/tsfile/read/v4/ITsFileReader.java`, and
  `java/examples/src/main/java/org/apache/tsfile/v4/TagFilterExample.java`
- Python: `python/tsfile/tag_filter.py`, `python/tsfile/tsfile_reader.pyx`, and
  `python/tests/test_tag_filter*.py`
- C/C++: `cpp/src/reader/filter/tag_filter.h`,
  `cpp/src/reader/tsfile_reader.h`, and `cpp/src/cwrapper/tsfile_cwrapper.h`
- Properties: the current writer/reader interfaces plus
  `python/tests/test_tsfile_properties.py` and
  `cpp/test/writer/tsfile_properties_test.cc`
