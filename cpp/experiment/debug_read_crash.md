# Read Benchmark Crash Debug Log

目标：让 `experiment/read_perf/read_benchmark` 能正确写入并读取 TsFile。

**状态：全部修复完成，benchmark 正常运行。**

---

## Bug 1：double-free crash（exit 134 / SIGABRT）

### 现象
运行 `read_benchmark` 时 ASAN 报 double-free，backtrace 指向 `StringArrayDeviceID::~StringArrayDeviceID`。

### 根因
`StringArrayDeviceID::deserialize`（`device_id.cc:147`）中：
```cpp
for (uint32_t i = 0; i < num_segments; ++i) {
    std::string* segment;   // ← 未初始化！
    if (RET_FAIL(read_var_char_ptr(segment, read_stream))) {
        delete segment;     // ← 若失败，segment 持有上一次迭代残留的栈值
        return ret;
    }
    segments_.push_back(segment);
}
```
循环第二次迭代时，若 `read_var_char_ptr` 失败，`segment` 因栈帧复用持有上一次成功分配的
指针（已进入 `segments_`），`delete segment` 提前释放，析构函数再释放一次，触发 double-free。

### 修复
- **`device_id.cc:147`**：`std::string* segment = nullptr;`
- **`byte_stream.h`** `read_var_char_ptr` 入口：`str = nullptr;`（防御性初始化）

---

## Bug 2：`unordered_map::at: key not found`（元数据反序列化失败）

### 现象
double-free 修复后，`TsFileMeta::deserialize_from` 读到 `table_schemas_size=0`，
后续 `table_schemas_.at("bench_table")` 抛异常。

### 根因
`wrap_from(tsfile_meta_buf, 630)` 设置：
```
page_size_ = 630
page_mask_ = 629   // 0b1001110101，非 2 的幂
```

`check_space()` 用 `(read_pos_ & page_mask_) == 0` 判断是否需要跨页：
- 读取 1 字节（map_size）后 `read_pos_=1`
- 读取长度前缀 `0x16` 后 `read_pos_=2`
- `2 & 629 = 0`（629 的 bit-1 未置位）→ **误触发跨页**，`read_page_ = next_ = nullptr`
- 返回 `E_OUT_OF_RANGE`，`read_var_str` 中止，`key=""` 只消耗了 1 字节

### 修复
**`byte_stream.h`** `wrap_from` 中将 `page_size_` 向上取整到最近的 2 的幂：
```cpp
uint32_t ps = 1;
while (ps < (uint32_t)buf_len) ps <<= 1;
page_size_ = ps;
page_mask_ = ps - 1;
total_size_.store(buf_len);  // 实际数据量不变
```

---

## Bug 3：`ASSERT(false)` in `decode_tv_buf_into_tsblock_by_datatype`

### 现象
```
[DEBUG] unexpected data_type_=255
Assertion failed: ((false)), function decode_tv_buf_into_tsblock_by_datatype,
    file aligned_chunk_reader.cc, line 1233.
```

### 根因
`AlignedChunkReader::get_next_page` 有两个重载：
1. `get_next_page(TsBlock*, Filter*, PageArena&)` — 正确检查 `multi_value_mode_`，路由到 `get_next_page_multi`
2. `get_next_page(TsBlock*, Filter*, PageArena&, int64_t, int&, int&)` — **缺少** `multi_value_mode_` 检查

`TsFileSeriesScanIterator::get_next` 调用第 2 个重载（带 `min_time_hint`/`row_offset`/`row_limit`
参数）。即使 `multi_value_mode_=true`，也会进入单值解码路径，此时 `value_chunk_header_`
从未被 `load_by_aligned_meta` 初始化，`data_type_=255=INVALID_DATATYPE`，触发 ASSERT。

### 修复
在第 2 个重载顶部添加 `multi_value_mode_` 检查：

**`src/reader/aligned_chunk_reader.cc`**（第 1320 行）：
```cpp
int AlignedChunkReader::get_next_page(..., int64_t min_time_hint, int& row_offset, int& row_limit) {
    if (multi_value_mode_) {
        return get_next_page_multi(ret_tsblock, oneshoot_filter, pa);
    }
    ...
```

---

## 已修改文件（最终）

| 文件 | 改动 |
|------|------|
| `src/common/device_id.cc:147` | `segment` 初始化为 `nullptr` |
| `src/common/allocator/byte_stream.h` | `wrap_from` 取 2 的幂；`read_var_char_ptr` 入口 `str=nullptr` |
| `src/reader/aligned_chunk_reader.cc` | 第 2 个 `get_next_page` 重载添加 `multi_value_mode_` 检查 |

所有临时 `[DEBUG]` 调试输出已清理。
