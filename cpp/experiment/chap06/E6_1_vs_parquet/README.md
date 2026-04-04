# E6-1：TsFile vs Parquet 端到端读取

## 状态：IN PROGRESS

**产出**：T6-1（数据集 × 查询类型 × 格式 × 吞吐矩阵）

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

| 查询类型 | 说明 |
|---------|------|
| full_scan | 全量扫描，读取所有数据点 |
| tag_filter | 设备定位，选取单个设备（B-tree 索引 vs row group 统计裁剪）|
| time_filter | 时间过滤，选择率 10% / 50% / 100% |

## 输出

- `vs_parquet_results.csv`：`dataset, experiment, engine, params, seconds, result_rows, rows_per_sec`

## 预期结论

- **Tag filter**：TsFile 优势显著（B-tree 索引 O(log n) vs Parquet 线性扫描 row group 统计）
- **Time filter**：TsFile 利用时间戳单调递增特性，chunk index 直接跳过不相关区间
- **Full scan**：Parquet 可能略优（Arrow 批处理成熟度高）
- 数据集特征影响结论：TDrive（8889 设备）tag filter 差距最大，REDD（115 设备）差距较小
