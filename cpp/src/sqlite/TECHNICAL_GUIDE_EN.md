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

# SQLite + TsFile technical report

This report describes the current experimental implementation. The [user
manual](USER_GUIDE_EN.md) covers daily operations and troubleshooting. There is one primary virtual
table module, `tsfile_hybrid`, with per-table writable or read-only mode.

## Components and state

| File | Responsibility |
| --- | --- |
| `tsfile_sqlite.cc` | Arguments, schema inference, hot/cold query adapter, writes, seal/compact, filesystem ownership and virtual-table transaction callbacks |
| `tsfile_sqlite_management.inc` | Seal/export/compact SQL functions and table/device diagnostic virtual tables, included in the adapter's anonymous namespace |
| `../../test/sqlite/tsfile_sqlite_test.cc` | Integration tests using real SQLite connections and TsFile readers/writers |
| `CMakeLists.txt` | Loadable module, compatible SQLite headers, runtime library paths |

A connection owns a registry of connected `HybridTable` objects keyed by SQLite
schema and logical table name. Each table stores its normalized columns, mode,
source file or directory, device column, pending files, pending deletions and
savepoint marks. `HybridCursor` materializes rows for SQLite's xNext/xColumn
callbacks. Management functions resolve their schema-qualified arguments through
the same connection and enlist the same table transaction callbacks.

Three creation modes converge on a common column vector and
`sqlite3_declare_vtab`: explicit columns with a writable directory, inferred
columns with a writable directory, and inferred columns without a directory.
Source inference accepts one TsFile or a directory; a directory scans its
first-level regular `.tsfile` files and requires a consistent target-table schema.
An ordinary source has an implicit `time` TIME column. Extension-produced files
preserve a custom TIME name in `tsfile_sqlite.time_column`. Inference records the
maximum time for each complete TAG combination. Empty table schemas are obtained
through the reader's complete schema metadata even when no data index exists.

Persistent inferred tables reconnect from stored schema rather than reinferring
from a potentially changed source. This keeps PRAGMA column metadata and
file diagnostics available when a source disappears. Actual data access checks the
registered file identity and fails on missing or changed files.

## SQLite storage

All identifiers are quoted. Internal objects live in the same SQLite schema as
the logical table, including attached and temporary databases.

| Object | Content |
| --- | --- |
| `<table>_tsfile$hot` | Writable tables only: private row ID, business columns and an internal device-key column |
| `<table>_tsfile$segments` | Path, row count, file-internal table name, file identity and size/mtime/inode metadata |
| `<table>_tsfile$devices` | Encoded complete TAG key, watermark and maximum time |
| `<table>_tsfile$config` | Precision, directory, serialized schema, mode, source path/table/max time |
| `<table>_tsfile$key` | Unique index over normalized TAG values and TIME |

Schema serialization uses length-prefixed names plus original TsFile type and
category codes. SQLite's mapped INTEGER/REAL/TEXT/BLOB declarations therefore do
not erase TsFile semantics. Object collisions fail creation; the extension does
not adopt existing user tables. xShadowName identifies the shadow table suffixes;
direct application writes to shadow state are unsupported.

Device keys encode each TAG in order as `N;` for NULL or `S<length>:<bytes>;`,
which distinguishes NULL, empty strings and separators. `tsfile_device_info`
returns the decoded, human-readable value.

The private hot row-ID column chooses an unused name starting at `tsfile$rowid`.
Business columns named `rowid`, `oid` or `_rowid_` do not control internal row
identity. Cold rows receive negative synthetic IDs; these identify immutable
rows during an operation and are not stable business keys.

For each TAG the unique index contains `(tag IS NULL)` and `coalesce(tag,'')`,
followed by TIME after all TAG components. This makes two NULL key components
conflict while distinguishing NULL from empty string. Zero-TAG tables use TIME
alone. Source duplicates are retained; new rows must be no earlier than that
device's watermark, so a device's historical gaps cannot be backfilled while a
new device is not constrained by other devices' boundaries.

## Query and write paths

xBestIndex describes integer time ranges, BINARY TAG equalities, BINARY FIELD
comparisons (`=`, `>`, `>=`, `<`, `<=`) and projections. xFilter reads matching
hot rows and registered cold segments; it applies file mapping when the source
table differs from the logical table. It uses the pending temporary path for
segments sealed inside an uncommitted transaction. Cold files are read through
`TableQueryExecutor`, pushing time, TAG and field comparisons into the TsFile
reader. Field predicates use `AlignedFieldFilter` and can participate in page
pruning. SQLite retains residual predicate evaluation and owns joins,
aggregates, ORDER BY and LIMIT semantics. To avoid differing from SQLite
comparison semantics, FLOAT columns, integers outside the exact double range and
BOOLEAN non-equality comparisons are not pushed; SQLite keeps the residual check.

xUpdate validates types, TIME non-nullness, uniqueness and the device watermark
before writing the hot shadow table. Actual cold UPDATE/DELETE and actual
read-only writes return `SQLITE_READONLY`, a non-constraint error that aborts the
whole statement even with IGNORE/FAIL. SQLite may optimize zero-row DML away
without calling xUpdate, so read-only no-ops can succeed.

Each device has an inclusive watermark lower bound. A device without source data
starts at INT64_MIN; a source-backed device starts at its source maximum + 1; a
new device has no row and is not constrained by other devices. A NULL watermark
represents exhausted time space for that device; read-only mode is distinguished
separately. xBegin reloads persisted boundaries to avoid a stale cached watermark
after another connection seals.

## Filesystem ownership

A writable directory must be absent or empty at creation and cannot overlap
another owned directory or contain the external source. Existing ancestors are
canonicalized. A `.tsfile-owner` marker is created exclusively and synced. It
contains the canonical SQLite database path and table name; in-memory databases
use a connection-local identity. Reconnect and write transactions validate the
marker. An ATTACH alias can change, but a copied/moved database cannot silently
share the original writable directory.

The table tracks newly created directories so a failed or rolled-back creation
can remove its own marker and empty directories. DROP preserves committed files
and the marker; removing a marker during transactional DROP would break a later
rollback. The caller is responsible for eventual archival or deletion.

File identity is size plus a full-byte FNV-1a fingerprint, computed once at
registration. `tsfile_verify` and export use it for content checks; ordinary
queries compare only size, mtime and inode. It is not a cryptographic guarantee.
External files remain caller-owned and must be immutable. The extension never
scans an external parent for ownership, and never deletes unknown files merely
because of `.tmp` or `.tsfile` suffixes.

## Seal, compact and transaction callbacks

`tsfile_seal` and `tsfile_compact` require a standalone top-level SELECT. They
create an internal savepoint and perform a zero-row UPDATE of the virtual table to
enlist SQLite's virtual-table write callbacks. The operation then shares SQLite's
transaction and savepoint lifetime. A reentrancy guard prevents nested management
operations. Management functions and diagnostic tables are direct-only interfaces.

`tsfile_seal` takes a maximum output file size in MB and no time boundary:

1. Collect all hot rows, sort by nullable TAG values and time, and compute each
   device's maximum time.
2. Write one or more temporary TsFiles within the size limit, splitting chunks
   recursively; a single row larger than the limit fails while hot rows remain.
   Finish each footer, fsync, and store schema/precision.
3. Register intended final paths, fingerprints and lightweight metadata, then
   delete all hot rows inside the SQLite transaction.
4. Advance each device with hot rows to its maximum time + 1, writing NULL for
   INT64_MAX. Devices without hot rows are unchanged.
5. In xSync, reopen temporary files to check metadata, publish them with atomic
   no-replace renames and fsync the owned directory.
6. SQLite commits its own state. xCommit forgets pending ownership records.

An empty seal returns 0, creates no file and changes no device boundary.

`tsfile_compact` selects only committed registered files in the owned directory
that are smaller than the minimum size; files pending in the current transaction
are skipped. It validates lightweight metadata, then fingerprints and registered
row counts before reading all rows through `TableQueryExecutor` and rewriting them
near the target size. New files are registered first and selected old registrations
are deleted in the transaction. Old paths are only unlinked after commit; failure
or rollback retains the originals and publishes no partial result. Hot rows and
device watermarks are unchanged.

xRollback removes only files owned by the pending operation and drops pending
deletions. xSavepoint records both pending-file and pending-deletion counts keyed
by SQLite savepoint ID; xRollbackTo removes subsequent unpublished files and
truncates the deletion list. Rolling back past a newly created virtual table
tolerates its already-removed config and releases the new directory resources.
Rollback paths do not execute reads while SQLite is rolling back, avoiding a
lingering read transaction that would block DETACH. xRelease removes released
savepoint marks.

File publication uses `renamex_np(..., RENAME_EXCL)` on macOS and Linux
`renameat2(..., RENAME_NOREPLACE)`. An existing directory entry, including a
dangling symlink, is never overwritten. A filesystem/kernel without the required
operation causes an I/O error rather than a replacement fallback.

Files become durable before SQLite can commit a reference to them. A process
interruption before SQLite commit can leave an unregistered file while SQLite
retains the old hot rows; the file is ignored and can be reported by verify.
The extension deliberately does not guess ownership and delete such files on
reconnect. These guarantees depend on the SQLite journal/synchronous configuration
and the filesystem honoring fsync; no cross-filesystem transaction is introduced.

## Export publication

Export rejects explicit user transactions and non-standalone calls before side
effects. It does not seal; it copies the table's own registered sealed files.
A read-only table copies its registered source files. External source files of a
writable table are outside its owned directory and are not exported. The output
must be a new absolute directory with an existing parent and must not overlap the
owned directory or a source file. Existing paths are never overwritten.

After path validation it creates a staging directory whose name contains
`.tsfile-export-`. Each file is copied while its content fingerprint is computed;
a mismatch with the registration fails immediately. Original file names are
preserved; empty or duplicate names fail. After copying and syncing the staging
directory, the output is published with an atomic no-replace rename and the parent
is synced. Failures clean known staging files; a crash may leave an isolated
staging directory, never a partial published output. A nonempty table returns the
number of copied files; an empty table creates an empty directory and returns 0.
The output retains the sealed file's internal table name and time precision, and
does not include other tables from a source file.

## Diagnostics

`tsfile_table_info` reads mode, precision, device count, the minimum device
watermark, source maximum time, hot rows, file count and path information in the
caller's transaction view. An empty table reports INT64_MIN for watermark; a
read-only table reports NULL. `tsfile_device_info` returns each complete TAG
combination's decoded key, watermark, current maximum time (including hot rows)
and hot row count.

`tsfile_verify` enumerates registered files and checks Reader metadata, selected
schema existence and content fingerprint. It reads each file in full to compute
the fingerprint but does not decode every business data page. Its statuses are
OK, MISSING, CORRUPT and MISMATCH. It also reports unregistered regular `.tsfile`
files in the owned directory as UNREGISTERED, without following scan symlinks or
recursing. Verification does not decode pages, repair files or change registration.

Both interfaces are eponymous-only virtual tables with fixed visible columns and
a hidden schema-qualified table argument. Their diagnostic row IDs are ephemeral.

## Validation and remaining limits

The SQLite integration suite covers the three creation modes, directory sources,
nullable logical keys, no-TAG tables, source/target mapping, empty and
multi-table sources, precision, watermark exhaustion, rowid-named columns, hot
CRUD, immutable cold rows, device boundaries, transactions/savepoints, persistent
reconnect, concurrent connection boundaries, field/time/TAG combinations, size
splitting, compact and its rollback, read-only export, source loss/change,
directory ownership and round-trip reading. Regression cases also exercise export
failure, rollback across CREATE, copied-database directory reuse, and existing
entries during publication.

Additional subprocess checks reopen databases after abrupt exit before or after
seal commit in both DELETE and WAL journal modes. These checks cover those
boundaries, not every power-loss or filesystem fault. Build and test commands are provided in the [user guide](USER_GUIDE_EN.md).

Queries, seal and compact currently materialize rows; export copies files and its
cost scales with file size; whole-file fingerprint checks add I/O. Field pushdown
covers BINARY integer, string and exactly representable double comparisons;
FLOAT and BOOLEAN non-equality comparisons remain SQLite residual predicates.
This is a functional implementation, not a bounded-memory streaming or
performance-tuned engine. There is no schema migration for the prototype shadow
layout, background seal, retention, historical correction, multi-file
registration, or full database backup/restore. Linux-specific publication code
needs Linux CI; local validation was on macOS with Homebrew SQLite.
