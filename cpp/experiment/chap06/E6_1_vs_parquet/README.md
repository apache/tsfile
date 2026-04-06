# E6-1：TsFile vs Parquet 读写端到端对比

## 状态：IN PROGRESS

**产出**：T6-1（数据集 × 操作类型 × 格式 × 性能矩阵）

## 数据集

来自 TsFile VLDB paper (Table 1) 的 4 个数据集：

| Dataset | Points | Devices | Tags | Fields (DOUBLE) |
|---------|--------|---------|------|-----------------|
| REDD    | 56M    | 115     | building, meter | power |
| GeoLife | 72M    | 181     | user_id | lat, lon, alt |
| TDrive  | 18M    | 8889    | taxi_id | lon, lat |
| TSBS    | 496M   | 4000    | name, fleet, driver | lat, lon, ele, vel |

## 数据准备

```bash
cd ../datasets
bash prepare_all.sh
```

详见 `../datasets/README.md`。

## 程序：`dataset_bench`

### 编译

```bash
cmake -S ../.. -B ../../cmake-build-release \
    -DCMAKE_BUILD_TYPE=Release \
    -DENABLE_SIMD=ON -DENABLE_THREADS=ON
cmake --build ../../cmake-build-release --target dataset_bench
```

### 运行

```bash
# 单个数据集
./dataset_bench --dataset redd --data-dir ../datasets/prepared/redd

# 全部数据集
./dataset_bench --all --data-root ../datasets/prepared --csv-out vs_parquet_results.csv
```

## 实验内容

| 操作类型 | 说明 |
|---------|------|
| write | 写入 TsFile / Parquet，记录耗时和吞吐 |
| space_bytes | 输出文件大小（字节） |
| full_scan | 全量扫描，读取所有数据点 |
| tag_filter | 设备定位，选取单个设备（B-tree 索引 vs row group 统计裁剪）|
| time_filter | 时间过滤，选择率 10% / 50% / 100% |
| tag_time_filter | 设备 + 时间联合过滤 |

## 输出

- `vs_parquet_results.csv`：`dataset, experiment, engine, params, seconds, result_rows, rows_per_sec`

其中：

- `write` 的 `result_rows` 为写入行数
- `space_bytes` 的 `result_rows` 为文件大小字节数

## Plot 脚本与生成物

`plot_e6_1.py` 只从结果 CSV 取数，不再手工写死空间数据。

```bash
# 直接传结果文件
python3 plot_e6_1.py vs_parquet_results.csv

# 或传目录，脚本会自动选择信息最完整的 CSV
python3 plot_e6_1.py .
```

生成：

- `F6_1a_space_cost.pdf`
- `F6_1b_tag_filter.pdf`
- `F6_1c_full_scan.pdf`
- `F6_1d_time_filter.pdf`
- `F6_1e_write_throughput.pdf`
- `F6_1_summary.pdf`

## 预期结论

- **Write**：Parquet 通常更快，顺序 row group 写路径更直接
- **Space cost**：与设备数和每设备数据量的比例强相关
- **Tag filter**：TsFile 优势显著（B-tree 索引 O(log n) vs Parquet 线性扫描 row group 统计）
- **Time filter**：TsFile 利用时间戳单调递增特性，chunk index 直接跳过不相关区间
- **Full scan**：Parquet 可能略优（Arrow 批处理成熟度高）
- 数据集特征影响结论：TDrive（8889 设备）tag filter 差距最大，REDD（115 设备）差距较小
