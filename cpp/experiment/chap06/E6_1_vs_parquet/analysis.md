# E6-1 实验结果分析：TsFile vs Parquet 端到端读取

## 实验环境

| 项目 | TsFile | Parquet |
|------|--------|---------|
| 平台 | Apple Silicon (ARM64) | 同左 |
| 编译配置 | C4: SIMD ON + 多线程 ON, Release (-O2) | 同左 |
| 压缩 | SNAPPY | SNAPPY |
| DOUBLE 编码 | GORILLA | Auto (BYTE_STREAM_SPLIT / PLAIN) |
| INT 编码 | TS_2DIFF | Auto (DELTA_BINARY_PACKED / PLAIN) |
| 时间戳编码 | TS_2DIFF (内置) | Auto |
| 库版本 | libtsfile (本项目) | Apache Arrow 22.0.0 |

> 注：两种格式的编码体系不兼容，无法使用完全一致的编码。压缩统一为 SNAPPY，编码各自使用最优方案。DOUBLE 列的空间代价不可直接对比（GORILLA vs BYTE_STREAM_SPLIT 压缩率不同）。

## 数据集

| Dataset | 来源 | 数据点 | 设备数 | 点/设备 | Tags | Fields |
|---------|------|--------|--------|---------|------|--------|
| TSBS | 生成 (Python) | 5.0M | 100 | 50,000 | name, fleet, driver | lat, lon, ele, vel |
| GeoLife | 公开 (Microsoft) | 24.2M | 182 | ~133,000 | user_id | lat, lon, alt |
| TDrive | 公开 (Microsoft) | 16.3M | 10,295 | ~1,580 | taxi_id | lon, lat |

三个数据集覆盖了不同的设备密度特征：TSBS 中等设备数 + 高密度，GeoLife 少设备 + 高密度，TDrive 大量设备 + 极低密度。

---

## T6-1a: 空间代价 (Space Cost)

| Dataset | TsFile | Parquet | TsFile/Parquet |
|---------|--------|---------|----------------|
| TSBS | 111 MB | 110 MB | 1.01x |
| GeoLife | 294 MB | 416 MB | **0.71x** |
| TDrive | 171 MB | 147 MB | 1.16x |

- GeoLife: TsFile 比 Parquet **小 29%**。TsFile 按设备组织 chunk group，182 个设备的 ID 仅存储在 chunk group header 和索引中；Parquet 每行都重复存储 `user_id` 字符串列（虽然有字典编码，但字典+索引开销仍大于 TsFile 的结构化方式）。
- TDrive: TsFile 比 Parquet **大 16%**。10,295 个设备导致大量 chunk group header 和 B-tree 索引节点开销。设备数量极多且每设备数据量很小（~1,580 行），索引元数据占比增大。
- TSBS: 两者接近，100 个设备是一个平衡点。

**结论**：TsFile 的空间效率与设备数 × 每设备数据量的比例相关。设备数适中时 TsFile 更紧凑；设备极多且每设备数据极少时，索引元数据占比增大。

---

## T6-1b: 设备定位查询 (Tag Filter)

| Dataset | 设备数 | TsFile 延迟 | Parquet 延迟 | TsFile 加速比 |
|---------|--------|------------|-------------|-------------|
| TSBS | 100 | 4.1 ms | 64.1 ms | **15.7x** |
| GeoLife | 182 | 10.0 ms | 41.5 ms | **4.2x** |
| TDrive | 10,295 | 4.2 ms | 30.5 ms | **7.2x** |

- TsFile 的 **B-tree 索引**使设备定位延迟与设备总数基本无关（始终 4-10 ms）。B-tree 的查找复杂度为 O(log₂₅₆ N)，N=10,295 时仅需 2 层查找。
- Parquet 依赖 **row group 统计信息裁剪**：逐个检查 row group 的 min/max，且裁剪后仍需在 batch 内逐行过滤 `id == target`。
- TSBS 加速最大（15.7x）：100 个设备占 Parquet 的多个 row group，裁剪效果差，绝大多数 row group 都包含目标设备数据。
- **所有数据集上 TsFile 均显著优于 Parquet**，验证了 B-tree 设备索引在 IoT 场景下的核心价值。

---

## T6-1c: 时间过滤查询 (Time Filter)

### TSBS (100 设备, 均匀时间分布)

| 选择率 | TsFile (M rows/s) | Parquet (M rows/s) | TsFile 加速比 |
|--------|---|---|---|
| 10% | 7.8 | 1.9 | **4.2x** |
| 50% | 13.7 | 9.3 | **1.5x** |
| 100% | 15.8 | 18.4 | 0.86x |

TsFile 在低选择率下优势显著：chunk index 的时间范围元数据可以直接跳过不相关的 chunk，而 Parquet 的 row group 粒度较粗。100% 选择率时 Parquet 略优，因为全量扫描不需要索引。

### GeoLife / TDrive

GeoLife 和 TDrive 的时间过滤结果受数据分布影响较大：不同设备的时间跨度各不相同（GPS 轨迹记录的时间段不重叠），导致全局时间窗口的"选择率"与实际命中行数不成正比。GeoLife 10%/50% 仅命中 3 行，TDrive 的时间过滤也无法有效利用 chunk 级裁剪（每设备一个 chunk，时间窗口总是命中部分设备的全部数据）。

---

## T6-1d: 全量扫描 (Full Scan)

| Dataset | 设备数 | TsFile (M rows/s) | Parquet (M rows/s) | Parquet 优势 |
|---------|--------|---|---|---|
| TSBS | 100 | 15.7 | 17.5 | 1.1x |
| GeoLife | 182 | 15.4 | 25.9 | **1.7x** |
| TDrive | 10,295 | 4.2 | 34.7 | **8.2x** |

全量扫描是 Parquet 的优势场景。差距与设备数量强相关：

1. **Parquet 的 row group 结构适合顺序扫描**：少量大 row group，Arrow 的 `RecordBatchReader` 高效流式读取，零拷贝列式内存映射。
2. **TsFile 的 chunk group 开销与设备数成正比**：全扫需遍历每个设备的 chunk group → 打开 chunk header → 解析 page → 解码。TDrive 有 10,295 个设备（每设备仅 ~1,580 行），元数据遍历成为主要瓶颈。
3. **TsFile 的设计目标不是全量扫描**：按设备组织 + B-tree 索引是为设备定位和时间范围查询优化的，代价是全量扫描时多了设备维度的遍历。

---

## 综合结论

### TsFile 的优势场景

| 查询模式 | 说明 | 加速比 |
|---------|------|-------|
| **设备定位** | 选取特定设备的全部数据 | **4-16x** |
| **低选择率时间过滤** | 小时间窗口查询 | **2-4x** |

### Parquet 的优势场景

| 查询模式 | 说明 | 加速比 |
|---------|------|-------|
| **全量扫描** | 分析型全表扫描 | **1.1-8.2x** |
| **高选择率时间过滤** | 接近全量的查询 | ~1.2x |

### 设计取舍

TsFile 和 Parquet 的性能差异源于根本的数据组织策略：

- **TsFile**：按设备组织（chunk group per device），B-tree 索引定位设备，chunk index 定位时间范围。优化目标是 IoT 的典型查询：**选定设备 + 时间窗口**。
- **Parquet**：按行分组（row group），列式存储，min/max 统计裁剪。优化目标是分析型工作负载：**全表或大范围扫描**。

TDrive 数据集（10,295 设备 × ~1,580 行/设备）极端地放大了两者的设计差异：TsFile 的元数据开销在设备数极多时成为瓶颈，但设备定位查询仍保持 O(log N) 的低延迟。

---

## 写入补充（基于 `py_all_results.csv`）

### T6-1e: 写入吞吐 (Write Throughput)

| Dataset | TsFile (M rows/s) | Parquet (M rows/s) | Parquet 加速比 |
|---------|---|---|---|
| TSBS | 1.15 | 2.54 | **2.21x** |
| GeoLife | 1.43 | 3.53 | **2.47x** |
| TDrive | 2.08 | 5.07 | **2.43x** |

- **Parquet 在三个数据集上都稳定更快**，优势大约在 **2.2-2.5x**。
- 主要原因是 **Parquet 的写路径更接近顺序列式刷盘**：按固定 row group 追加批次即可，Arrow/Parquet 写端实现也更成熟。
- **TsFile 写入需要维持按设备组织的 chunk group 结构**，同时生成更复杂的索引与元数据。这部分开销不会在写阶段被隐藏掉，最终体现为更低的写吞吐。
- TDrive 上 TsFile 写入吞吐有所提升（2.08 M rows/s，高于 TSBS/GeoLife），推测与每设备记录数较小、单设备 chunk 较短有关；但 Parquet 仍保持明显优势。

**写入结论**：如果工作负载以导入/落盘吞吐为核心目标，Parquet 更占优；TsFile 的代价主要换来了后续设备定位与低选择率时间过滤上的查询优势。

---

## 待补充

1. **REDD 数据集**：官网（redd.csail.mit.edu）502 不可用，待恢复后补充
2. **TSBS 大规模**：当前 5M 点（100 设备），论文配置为 496M 点（4000 设备）
3. **写入阶段拆分**：当前只做端到端写入吞吐，未拆分数据页编码、索引构建、flush/close 的单独耗时
4. **GeoLife/TDrive 时间过滤修正**：真实数据集时间分布不均匀，需按 per-device 时间范围计算有效选择率

---

*实验日期: 2026-04-04*
*补充写入分析数据: `py_all_results.csv`*
