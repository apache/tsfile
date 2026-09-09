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
| `tsfile_sqlite.cc` | Arguments, schema inference, hot/cold query adapter, writes, seal, filesystem ownership and virtual-table transaction callbacks |
| `tsfile_sqlite_management.inc` | Seal/export SQL functions and diagnostic virtual tables, included in the adapter's anonymous namespace |
| `../../test/sqlite/tsfile_sqlite_test.cc` | Integration tests using real SQLite connections and TsFile readers/writers |
| `CMakeLists.txt` | Loadable module, compatible SQLite headers, runtime library paths |

A connection owns a registry of connected `HybridTable` objects keyed by SQLite
schema and logical table name. Each table stores its normalized columns, mode,
source mapping, precision, watermark, pending files and savepoint marks.
`HybridCursor` materializes rows for SQLite's xNext/xColumn callbacks. Management
functions resolve their schema-qualified arguments through the same connection
and enlist the same table transaction callbacks.

Three creation modes converge on a common column vector and
`sqlite3_declare_vtab`: explicit columns with a writable directory, inferred
columns with a writable directory, and inferred columns without a directory.
An ordinary source has an implicit `time` TIME column. Extension-produced files
preserve a custom TIME name in `tsfile_sqlite.time_column`. Source inference selects
one file table, validates its schema, scans that table for maximum time and records
file identity before/after the scan. Empty table schemas are obtained through the
reader's complete schema metadata even when no data index exists.

Persistent inferred tables reconnect from stored schema rather than reinferring
from a potentially changed source. This keeps PRAGMA column metadata and
file diagnostics available when a source disappears. Actual data access checks the
registered file identity and fails on missing or changed files.

## SQLite storage

All identifiers are quoted. Internal objects live in the same SQLite schema as
the logical table, including attached and temporary databases.

| Object | Content |
| --- | --- |
| `<table>_tsfile$hot` | Writable tables only: private row ID and business columns |
| `<table>_tsfile$segments` | Path, cutoff, row count, file-internal table name, file identity |
| `<table>_tsfile$config` | Watermark, precision, directory, serialized schema, mode, source path/table/max time |
| `<table>_tsfile$key` | Unique index over normalized TAG values and TIME |

Schema serialization uses length-prefixed names plus original TsFile type and
category codes. SQLite's mapped INTEGER/REAL/TEXT/BLOB declarations therefore do
not erase TsFile semantics. Object collisions fail creation; the extension does
not adopt existing user tables. xShadowName identifies the three shadow table
suffixes; direct application writes to shadow state are unsupported.

The private hot row-ID column chooses an unused name starting at `tsfile$rowid`.
Business columns named `rowid`, `oid` or `_rowid_` do not control internal row
identity. Cold rows receive negative synthetic IDs; these identify immutable
rows during an operation and are not stable business keys.

For each TAG the unique index contains `(tag IS NULL)` and `coalesce(tag,'')`,
followed by TIME after all TAG components. This makes two NULL key components
conflict while distinguishing NULL from empty string. Zero-TAG tables use TIME
alone. Source duplicates are retained because new rows must be later than the
selected source maximum, rather than being merged into historical keys.

## Query and write paths

xBestIndex describes integer time ranges, BINARY TAG equalities and projections.
xFilter reads matching hot rows and registered cold segments; it applies file
mapping when the source table differs from the logical table. It uses the pending
temporary path for segments sealed inside an uncommitted transaction. TsFile
readers receive supported time and TAG filters; SQLite retains residual predicate
evaluation and owns joins, aggregates, ORDER BY and LIMIT semantics.

xUpdate validates types, TIME non-nullness, uniqueness and watermark before
writing the hot shadow table. Actual cold UPDATE/DELETE and actual read-only
writes return `SQLITE_READONLY`, a non-constraint error that aborts the whole
statement even with IGNORE/FAIL. SQLite may optimize zero-row DML away without
calling xUpdate, so read-only no-ops can succeed.

The watermark is an inclusive lower bound. Explicit empty tables start at
INT64_MIN; writable source-backed tables start at source_max+1. A NULL watermark
represents exhausted time space for writable tables, or absence of a writable
boundary for read-only tables. Mode disambiguates these states. xBegin reloads
persisted config to avoid a stale cached watermark after another connection seals.

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

File identity is size plus a full-byte FNV-1a fingerprint. It detects ordinary
replacement/change and is not a cryptographic guarantee. External files remain
caller-owned and must be immutable. The extension never scans an external parent
for ownership, and never deletes unknown files merely because of `.tmp` or
`.tsfile` suffixes.

## Seal and transaction callbacks

`tsfile_seal` requires a standalone top-level SELECT. It creates an internal
savepoint and performs a zero-row UPDATE of the virtual table to enlist SQLite's
virtual-table write callbacks. The hot-to-cold transition then shares SQLite's
transaction and savepoint lifetime. A reentrancy guard prevents nested management
operations. Management functions and diagnostic tables are direct-only interfaces.

For a nonempty seal:

1. Collect eligible hot rows and sort by nullable TAG values, then time.
2. Write a new temporary TsFile with schema/precision, finish its footer and fsync.
3. Register the intended final path and identity, delete eligible hot rows and
   update watermark inside the SQLite transaction.
4. In xSync, reopen the temporary file to check metadata, publish it with an
   atomic no-replace rename and fsync the owned directory.
5. SQLite commits its own state. xCommit forgets the pending ownership records.

Empty seals update only the watermark. Explicit cutoff is half-open; automatic
export can use an inclusive final bound for INT64_MAX and mark exhaustion.

xRollback removes only files owned by the pending operation and restores cached
configuration from SQLite. xSavepoint records pending-file counts keyed by SQLite
savepoint ID; xRollbackTo removes subsequent files and reloads config. Rolling
back past a newly created virtual table tolerates its already-removed config and
releases the new directory resources. xRelease removes released savepoint marks.

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

## Export snapshot and publication

Export rejects explicit user transactions and non-standalone calls before side
effects. It validates output paths, opens an internal transaction, validates and
captures cold rows, and enlists writable state before collecting hot rows. SQLite
transaction locking prevents a concurrent writer from being silently included
between the snapshot and boundary update; an incompatible snapshot upgrade fails.

All captured hot rows are sealed, and the internal savepoint is released to
commit that transition. The captured logical rows are then sorted and rewritten
to one standard TsFile in a unique sibling staging directory. A nonempty output
contains `part-000001.tsfile`; an empty output is an empty directory. The source
file's unrelated tables are never copied into the output.

The staging directory is synced, published through atomic no-replace rename, and
its parent is synced. Generation/publication failures clean the operation's known
staging file where possible and report that sealing already committed. A crash
may leave an isolated staging directory. If the final parent fsync fails, the
error explicitly reports that complete output has already been published.
Automatic seal and output publication are two separate committed stages; an
output failure cannot restore mutability of already sealed rows. Later writes
are outside the captured output and remain in the hot area.

## Diagnostics

`tsfile_table_info` reads config and row/file counts in the caller's transaction
view. `tsfile_verify` enumerates registered files and checks Reader metadata,
selected schema existence and content fingerprint. Its statuses are OK, MISSING,
CORRUPT and MISMATCH. It also reports unregistered regular `.tsfile` files in the
owned directory as UNREGISTERED, without following scan symlinks or recursing.
Verification does not decode every page, repair files or change registration.

Both interfaces are eponymous-only virtual tables with fixed visible columns and
a hidden schema-qualified table argument. Their diagnostic row IDs are ephemeral.

## Validation and remaining limits

The SQLite integration suite covers the three creation modes, nullable logical
keys, no-TAG tables, source/target mapping, empty and multi-table sources, precision,
watermark exhaustion, rowid-named columns, hot CRUD, immutable cold rows,
transactions/savepoints, persistent reconnect, concurrent connection boundaries,
source loss/change, directory ownership, automatic export and round-trip reading.
Regression cases also exercise output failure after seal commit, rollback across
CREATE, copied-database directory reuse, and existing entries during publication.

Additional subprocess checks reopen databases after abrupt exit before or after
seal commit in both DELETE and WAL journal modes. These checks cover those
boundaries, not every power-loss or filesystem fault. Build and test commands are provided in the [user guide](USER_GUIDE_EN.md).

Queries, seal and export currently materialize rows, and export rewrites the full
snapshot. Whole-file fingerprint checks also add I/O. This is a functional
implementation, not a bounded-memory streaming or performance-tuned engine.
There is no schema migration for the prototype shadow layout, background seal,
compaction, automatic retention, historical correction, multi-file registration,
or full database backup/restore. Linux-specific publication code needs Linux CI;
local validation was on macOS with Homebrew SQLite.
