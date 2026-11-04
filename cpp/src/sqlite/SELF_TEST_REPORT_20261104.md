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

# SQLite + TsFile 自测报告：非通过项修复

日期：2026-11-04  
分支：`experiment-sqlite`  
范围：CMP-05、CMP-02、`.tmp` 清理  
构建：macOS + Homebrew SQLite 3.53.3

## 1. 本次修改

| 项 | 处理 |
| --- | --- |
| CMP-05 | 不增加内容变更识别；补全冷文件读取失败的错误信息，并附带 TsFile 原始错误码 |
| CMP-02 | tree model 文件明确报 `tree model is not supported` |
| `.tmp` | 只在 `tsfile_compact`、`tsfile_export` 成功后清理；普通连接和 `tsfile_seal` 不做清理 |
| QR-09 | 按约定忽略，本次不处理 |

## 2. 测试环境与构建

- 机器：Apple M5 Pro，macOS 26.6.2。
- SQLite：Homebrew SQLite 3.53.3，支持 `load_extension`。
- 编译器：Apple Clang。
- 构建目录：`cpp/build/sqlite-homebrew`。

```bash
cmake -S cpp -B cpp/build/sqlite-homebrew \
  -DBUILD_SQLITE_EXTENSION=ON \
  -DTSFILE_BUILD_SHARED=ON \
  -DBUILD_TEST=ON \
  -DBUILD_TOOLS=OFF \
  -DENABLE_ANTLR4=OFF \
  -DENABLE_SIMD=OFF \
  -DSQLite3_INCLUDE_DIR=/opt/homebrew/opt/sqlite/include \
  -DSQLite3_LIBRARY=/opt/homebrew/opt/sqlite/lib/libsqlite3.dylib

cmake --build cpp/build/sqlite-homebrew \
  --target tsfile_sqlite TsFile_Sqlite_Test -j 8
```

运行测试：

```bash
DYLD_LIBRARY_PATH=cpp/build/sqlite-homebrew/lib \
  cpp/build/sqlite-homebrew/test/lib/TsFile_Sqlite_Test
```

## 3. 测试方法：为什么需要在数据库外操作

这次三个改动中有两类状态无法只靠 SQL 构造：

1. **源文件或段文件损坏/缺失**：需要先 seal 生成真实的 `.tsfile`，再在文件系统里删除、覆盖或写入损坏内容。
2. **残留 `.tmp`**：需要直接创建一个符合 `<stem>-<pid>-<counter>.tmp` 命名的普通文件；文件名里的 PID 用来模拟“已经退出的进程”。仅执行 SQL 不会产生这种孤儿文件。

因此测试用例里除了 SQL，还会使用 `std::ofstream`、`unlink`、`access`、`DETACH`/`ATTACH` 等操作。每个用例下面都标明“数据库外操作”。

`.tmp` 清理逻辑的判定条件是：

```text
文件名前缀 = file_stem(table_name) + "-"
文件名后缀 = ".tmp"
只处理普通文件（lstat + S_ISREG），跳过 symlink
跳过 table->pending 里的当前发布文件
解析文件名中的 PID；PID 仍存活（kill(pid, 0) == 0）则跳过
```

清理触发点只有两个：

- `tsfile_compact` 成功结束后；
- `tsfile_export` 发布成功、父目录 fsync 成功后。

普通连接（`xConnect`）和 `tsfile_seal` 不调用清理。

## 4. 自动化用例

### 4.1 `TreeModelSourceIsRejectedClearly`

```text
输入 SQL:
CREATE VIRTUAL TABLE tree USING tsfile_hybrid(
  file='<CPP>/test/tools/fixtures/complex_tree.tsfile',
  source_table='root',
  directory='<TMP>-tree',
  timestamp_precision='ms');

数据库外操作:
测试夹具 complex_tree.tsfile 是预先存在的 tree model 文件。

实际执行:
[ RUN      ] TsFileSqliteTest.TreeModelSourceIsRejectedClearly
[       OK ] TsFileSqliteTest.TreeModelSourceIsRejectedClearly (8 ms)

实际结果:
tree model is not supported; only table model files can be read:
<CPP>/test/tools/fixtures/complex_tree.tsfile
```

- **输入是什么**：用仓库里的 tree model 夹具创建只读虚拟表，`source_table='root'`。
- **测试的是什么**：不应再落进 `source table not found`；应明确说明 tree model 不支持。
- **结果是什么**：建表失败，错误信息包含 `tree model` 和 `not supported`。
- **是否符合预期**：是。

### 4.2 `CorruptSourceFileReportsTsfileErrorCode`

```text
输入 SQL:
CREATE VIRTUAL TABLE corrupt USING tsfile_hybrid(
  file='<TMP>-corrupt.tsfile',
  source_table='sensor',
  directory='<TMP>-corrupt',
  timestamp_precision='ms');

数据库外操作:
测试先用 std::ofstream 把 "not a tsfile" 写入 <TMP>-corrupt.tsfile。

实际执行:
[ RUN      ] TsFileSqliteTest.CorruptSourceFileReportsTsfileErrorCode
[       OK ] TsFileSqliteTest.CorruptSourceFileReportsTsfileErrorCode (1 ms)

实际结果:
cannot read source file: <TMP>-corrupt.tsfile
(tsfile E_TSFILE_CORRUPTED=35)
```

- **输入是什么**：一个内容不是合法 TsFile 的普通文件，通过 `file=` 建表。
- **测试的是什么**：建表阶段的读取失败应带出 TsFile reader 的原始错误码，而不是只有一句通用的 `cannot read source file`。
- **结果是什么**：错误信息包含 `E_TSFILE_CORRUPTED=35`。
- **是否符合预期**：是。

### 4.3 `QueryReportsDeletedSealedFile`

```text
输入 SQL:
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
-- 数据库外删除 seal 生成的 .tsfile
SELECT count(*) FROM sensor;

数据库外操作:
seal 后用 unlink() 删除 <TMP>/tsfilesensor-<pid>-0.tsfile。

实际执行:
[ RUN      ] TsFileSqliteTest.QueryReportsDeletedSealedFile
[       OK ] TsFileSqliteTest.QueryReportsDeletedSealedFile (2 ms)

实际结果:
SQLite error: missing or changed file for sensor:
<TMP>/tsfilesensor-<pid>-0.tsfile
```

- **输入是什么**：seal 一行数据，然后从文件系统删除对应段文件，再查询。
- **测试的是什么**：查询冷段时不能静默返回旧值；文件缺失应返回错误。
- **结果是什么**：查询返回 SQLite 错误，错误信息包含 `missing or changed file`。
- **是否符合预期**：是。

### 4.4 `OrphanTempFileIsKeptAfterSeal`

```text
输入 SQL:
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
-- 数据库外创建 <TMP>/tsfilesensor-99999999-0.tmp
INSERT INTO sensor VALUES(2,'d0',2.5);
SELECT tsfile_seal('main.sensor',2);

数据库外操作:
用一个不存在的 PID 99999999 创建 stale 普通文件，随后再执行一次 seal。

实际执行:
[ RUN      ] TsFileSqliteTest.OrphanTempFileIsKeptAfterSeal
[       OK ] TsFileSqliteTest.OrphanTempFileIsKeptAfterSeal (1 ms)

实际结果:
seal 结束后 access(stale, F_OK) == 0，文件仍然存在。
```

- **输入是什么**：seal 后手工放一个 dead-PID 的 `.tmp`，再 seal 一次。
- **测试的是什么**：`tsfile_seal` 不应清理 `.tmp`。
- **结果是什么**：文件保留。
- **是否符合预期**：是（按新的清理范围）。

### 4.5 `OrphanTempFileIsKeptOnReconnect`

```text
输入 SQL:
ATTACH '<TMP>-temp-clean.db' AS persisted;
CREATE VIRTUAL TABLE persisted.clean USING tsfile_hybrid(...);
INSERT INTO persisted.clean VALUES(1,'d0',1.5);
SELECT tsfile_seal('persisted.clean',2);
DETACH persisted;

数据库外操作:
在 `persisted.clean` 的表目录创建
`tsfileclean-99999999-0.tmp`。

再执行:
ATTACH '<TMP>-temp-clean.db' AS persisted;
SELECT count(*) FROM persisted.clean;

实际执行:
[ RUN      ] TsFileSqliteTest.OrphanTempFileIsKeptOnReconnect
[       OK ] TsFileSqliteTest.OrphanTempFileIsKeptOnReconnect (4 ms)

实际结果:
重新连接后 access(stale, F_OK) == 0，文件仍然存在。
```

- **输入是什么**：file-backed DB 中 seal，DETACH，创建 stale `.tmp`，再 ATTACH 并查询。
- **测试的是什么**：正常连接不应做 `.tmp` 清理。
- **结果是什么**：文件保留。
- **是否符合预期**：是。

### 4.6 `OrphanTempFileIsCleanedAfterCompact`

```text
输入 SQL:
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
INSERT INTO sensor VALUES(2,'d0',2.5);
SELECT tsfile_seal('main.sensor',2);
-- 数据库外创建 <TMP>/tsfilesensor-99999999-0.tmp
SELECT tsfile_compact('main.sensor',10,20);

数据库外操作:
在 compact 前创建 dead-PID 的 stale 普通文件。

实际执行:
[ RUN      ] TsFileSqliteTest.OrphanTempFileIsCleanedAfterCompact
[       OK ] TsFileSqliteTest.OrphanTempFileIsCleanedAfterCompact (2 ms)

实际结果:
compact 后 access(stale, F_OK) != 0，文件已被删除。
```

- **输入是什么**：两个小段文件、一个 stale `.tmp`，执行 compact。
- **测试的是什么**：compact 成功后应清理 dead-PID 的 `.tmp`。
- **结果是什么**：文件删除，compact 返回 1 个输出文件。
- **是否符合预期**：是。

### 4.7 `OrphanTempFileIsCleanedAfterExport`

```text
输入 SQL:
INSERT INTO sensor VALUES(1,'d0',1.5);
SELECT tsfile_seal('main.sensor',2);
-- 数据库外创建 <TMP>/tsfilesensor-99999999-0.tmp
SELECT tsfile_export('main.sensor','<TMP>-export-clean');

数据库外操作:
在 export 前创建 dead-PID 的 stale 普通文件。

实际执行:
[ RUN      ] TsFileSqliteTest.OrphanTempFileIsCleanedAfterExport
[       OK ] TsFileSqliteTest.OrphanTempFileIsCleanedAfterExport (1 ms)

实际结果:
export 后 access(stale, F_OK) != 0，文件已被删除。
```

- **输入是什么**：seal 后创建一个 stale `.tmp`，执行 export。
- **测试的是什么**：export 发布成功后应清理 dead-PID 的 `.tmp`。
- **结果是什么**：文件删除，export 成功。
- **是否符合预期**：是。

## 5. 全量回归

执行完整 SQLite 测试：

```bash
DYLD_LIBRARY_PATH=cpp/build/sqlite-homebrew/lib \
  cpp/build/sqlite-homebrew/test/lib/TsFile_Sqlite_Test
```

结果：

```text
[==========] 54 tests from 1 test suite ran. (575 ms total)
[  PASSED  ] 54 tests.
```

## 6. 边界与未覆盖项

- **CMP-05 不做内容变更识别**：同 size、同 mtime 的原地改写仍可能绕过 stamp 校验；如果损坏页让 reader 卡住，本次修改不会打断。当前只保证“TsFile reader 返回错误时，错误信息可读且带错误码”。
- **QR-09 未处理**：按约定忽略。
- **`.tmp` 清理范围**：只在 compact/export 成功后执行；普通连接和 seal 不清理。symlink、活跃 PID、当前 `pending` 文件不会被删除。
- **平台**：本次在 macOS 上完成构建和测试；未在树莓派/x86 上重跑。代码使用 POSIX 文件接口，Linux 路径与 macOS 逻辑一致，但仍建议在目标平台补一次回归。

## 7. 结论

- CMP-02、`.tmp` 清理已按约定完成。
- CMP-05 的错误报告已补全，并附带 TsFile 错误码；内容变更识别留待后续单独设计。
- 新增 7 个针对性用例，完整 SQLite 套件 54/54 通过。
