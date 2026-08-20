#[[
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
]]

# Public entry points are intentionally listed separately from the transitive
# header closure. The current public headers include implementation-level
# declarations, so the closure is installed until those dependencies can be
# hidden behind a stable facade. Adding a new public entry point requires an
# explicit review of this list.
set(TSFILE_PUBLIC_ENTRYPOINT_HEADERS
        cwrapper/tsfile_cwrapper.h
        cwrapper/tsfile_cwrapper_expression.h
        reader/tsfile_reader.h
        writer/tsfile_writer.h
        writer/tsfile_table_writer.h
        writer/tsfile_tree_writer.h)

# These directories form the reviewed transitive closure of the entry points
# above. This is an installation compatibility closure, not a promise that
# every header is a stable public API. Compression, encoding, parser, and
# utility headers are implementation dependencies today, but are required for
# consumers to parse the installed public declarations.
set(TSFILE_PUBLIC_HEADER_CLOSURE
        common
        compress
        cwrapper
        encoding
        file
        parser
        reader
        utils
        writer)
