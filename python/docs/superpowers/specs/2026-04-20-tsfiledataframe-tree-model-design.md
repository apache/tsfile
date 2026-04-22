# TsFileDataFrame Tree Model Support — Design Spec

## Overview

Extend `TsFileDataFrame` to support tree-model TsFiles alongside existing table-model TsFiles. A directory may contain a mix of both model types. A single TsFile containing both models is not supported and will raise an error.

## Design Decisions

| Decision | Choice |
|----------|--------|
| Series path format | Native tree path: `root.d1.s1` (no escaping) |
| Data types indexed | Numeric only (BOOLEAN, INT32, INT64, FLOAT, DOUBLE, TIMESTAMP) |
| Implementation approach | Extend existing `TsFileSeriesReader` with tree-model branch |
| Field columns across devices | Union merge; missing measurements produce count=0 series |

## Core Mapping: Tree Model → MetadataCatalog

The existing `MetadataCatalog` uses `TableEntry` (table_name, tag_columns, tag_types, field_columns) and `DeviceEntry` (table_id, tag_values, min_time, max_time). Tree-model devices map to this abstraction by splitting the device path into segments:

```
Device path: root.sg1.d1
Measurements: s1 (INT64), s2 (DOUBLE), s3 (TEXT)

Split: ["root", "sg1", "d1"]
  → table_name = "root"            (first segment)
  → tag_values = ("sg1", "d1")     (remaining segments)
  → field_columns = ("s1", "s2")   (numeric only; s3 TEXT filtered out)

Series paths: root.sg1.d1.s1, root.sg1.d1.s2
```

### Virtual Tag Columns

Tree-model devices do not have named tag columns. Virtual column names `__seg_0`, `__seg_1`, ... are generated based on segment position. All `tag_types` are `TSDataType.STRING`.

### Field Column Union

Devices under the same `table_name` may have different measurements. The `field_columns` for a virtual table is the ordered union of all numeric measurements across all devices in that table. Devices missing a measurement produce a series with `count=0`.

Example:
```
root.d1: s1, s2       root.d2: s2, s3
→ table "root" field_columns = ("s1", "s2", "s3")

root.d1.s1 count=10   root.d2.s1 count=0
root.d1.s2 count=10   root.d2.s2 count=10
root.d1.s3 count=0    root.d2.s3 count=10
```

### Depth Variance

Devices under the same table_name may have different path depths (e.g., `root.d1` vs `root.sg1.d2`). The `tag_columns` length equals the maximum depth minus one. Shorter devices have trailing `None` tag values, handled by the existing sparse-tag mechanism in `MetadataCatalog`.

## File Changes

| File | Change |
|------|--------|
| `dataset/reader.py` | Main changes: model detection, tree metadata caching, tree read paths |
| `dataset/dataframe.py` | **No changes** |
| `dataset/metadata.py` | **No changes** |
| `dataset/timeseries.py` | **No changes** |
| `tests/` | New test file for tree-model TsFileDataFrame tests |

## reader.py Detailed Design

### Model Detection: `_cache_metadata()`

```python
def _cache_metadata(self):
    table_schemas = self._reader.get_all_table_schemas()
    device_schemas = self._reader.get_all_timeseries_schemas()

    has_table = bool(table_schemas)
    has_tree = bool(device_schemas)

    if has_table and has_tree:
        raise ValueError(
            f"TsFile '{self.file_path}' contains both table-model and "
            f"tree-model data, which is not supported."
        )

    if has_table:
        self._is_tree_model = False
        self._cache_metadata_table_model()
    elif has_tree:
        self._is_tree_model = True
        self._cache_metadata_tree_model(device_schemas)
    else:
        raise ValueError(f"No tables or devices found in TsFile: {self.file_path}")
```

### Tree Metadata Caching: `_cache_metadata_tree_model()`

Two-pass algorithm:

**Pass 1 — Collect unified table metadata:**
- Iterate all devices from `device_schemas`
- For each device, split path into segments; first segment = `table_name`
- Track per-table: max tag depth, ordered union of numeric measurement names

**Pass 2 — Register tables, devices, and series stats:**
- For each table: register `TableEntry` with virtual tag_columns and unified field_columns
- For each device: register `DeviceEntry` with tag_values from path segments
- Use `get_timeseries_metadata(None)` for per-field statistics
- For measurements a device doesn't have: write zero-count stats

### Device Path Reconstruction

```python
def _reconstruct_device_path(self, device_id: int) -> str:
    device_entry = self._catalog.device_entries[device_id]
    table_entry = self._catalog.table_entries[device_entry.table_id]
    segments = [table_entry.table_name]
    segments.extend(str(v) for v in device_entry.tag_values if v is not None)
    return ".".join(segments)
```

### Read Path: `read_device_fields_by_time_range()`

Dispatches based on `self._is_tree_model`:

- **Table model (existing):** `_read_arrow()` → `query_table()` with tag filter + Arrow batch
- **Tree model (new):** `_read_arrow_tree()` → reconstruct device path → `query_timeseries(device_path, measurement_names, start_time, end_time)` → Arrow batch

### Read Path: `read_series_by_row()`

Dispatches based on `self._is_tree_model`:

- **Table model (existing):** `query_table_by_row()` with tag filter
- **Tree model (new):** reconstruct device path → `query_tree_by_row([device_path], [measurement_name], offset, limit)`

### Case Sensitivity

Tree-model measurement names are **case-sensitive** (unlike table model which lowercases everything). The `_cache_metadata_tree_model()` preserves original casing from `get_all_timeseries_schemas()`.

## Mixed-Directory Handling

`TsFileDataFrame._load_metadata()` opens each file independently via `TsFileSeriesReader`. Each reader detects its own model type and populates `MetadataCatalog` accordingly. The dataframe's `_register_reader()` merges all catalogs into a unified `_LogicalIndex`.

Cross-file table schema validation (`_validate_table_schema()`) ensures consistency. A tree-model virtual table named `"root"` and a real table-model table named `"root"` would fail validation — this is correct behavior since they represent incompatible schemas.

## Test Plan

1. **Single-file tree model:** Write tree TsFile → `TsFileDataFrame` → verify `list_timeseries()`, `len()`, `__getitem__` by name and index, `loc[]` time-range queries
2. **Multiple devices, different depths:** `root.d1` and `root.sg1.d2` coexist → verify sparse tag handling and correct path resolution
3. **Different measurement sets:** Devices with different measurements → verify field union and zero-count series
4. **Mixed-model directory:** Table-model and tree-model TsFiles in same directory → verify both model types accessible
5. **Mixed-model single file error:** Verify `ValueError` is raised
6. **Multi-shard merge:** Same tree device data split across files → verify timestamp merge and duplicate detection
