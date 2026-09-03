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

# tsfile-cli Errors

Exit status is the only success signal.

- `0`: complete success.
- `1`: usage or parameter error, including unknown commands, invalid options, missing scope, invalid projection, offset errors, or mismatched model/scope.
- `2`: input problem, including missing or unreadable TsFile input, damaged TsFile input, invalid CSV syntax or data during `write`, or unsupported source file type for `write -i`.
- `3`: runtime or target problem, including query/write failures, output target conflicts, output creation failure, or atomic commit failure.

Rules for callers:

- Ignore stdout and output files unless the exit status is `0`.
- `cat > file` is shell-managed and not atomic; use `export` for atomic files.
- `export`, `sketch -o`, and `write` write final targets only after successful command-level validation and commit.
- `write -v` emits a summary only after the target TsFile is committed.
