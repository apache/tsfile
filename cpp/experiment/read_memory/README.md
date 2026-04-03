# Read Memory Model Experiment

验证读取侧内存公式：

$$M_{read} \approx M_{fixed} + batch\_size \times s_{row} + N_{cols} \times C_{page}$$

## 程序：`read_mem_model`

### 编译配置

**需要 ENABLE_MEM_STAT=ON** 来追踪各模块峰值内存。

```bash
cmake -DENABLE_MEM_STAT=ON -DBUILD_TEST=OFF ..
cmake --build . --target read_mem_model
```

### 实验参数矩阵

| 变量 | 取值 |
|------|------|
| N_cols（查询列数） | 1, 4, 8, 16 |
| batch_size | 1024, 4096, 16384, 65536 |

**固定参数**：
- 基线数据：W0 (10 设备，200M 行，SNAPPY 压缩)
- 如果基线文件不存在，程序会自动创建

### 关键 AllocModID

读取侧内存主要分布在：
- `MOD_TSBLOCK`：TsBlock 缓冲，随 batch_size 线性增长
- `MOD_DECODER_OBJ`：解压缓冲，随 N_cols 线性增长
- `MOD_DEFAULT`：固定开销 M_fixed
- 其他模块：辅助开销

### 执行流程

1. **数据准备**：若基线文件不存在则生成（10 设备，200M 行）
2. **参数遍历**：对每组 (N_cols, batch_size) 执行：
   - 重置 ModStat
   - 全量扫描基线文件
   - 记录峰值内存
3. **输出结果**：CSV 文件

### 运行

```bash
# 默认配置
./read_mem_model

# 指定基线文件和输出路径
./read_mem_model <baseline_path> <csv_path>

# 示例
./read_mem_model read_mem_baseline.tsfile results.csv
```

### 输出格式

`read_memory_results.csv`：
```
n_cols,batch_size,peak_total_kb,DEFAULT_kb,TVLIST_DATA_kb,TSBLOCK_kb,...
1,1024,...
1,4096,...
...
16,65536,...
```

### 预期现象

- **TSBLOCK 内存**随 batch_size 线性增长
- **DECODER_OBJ 内存**随 N_cols 线性增长
- **DEFAULT 内存**保持相对稳定（M_fixed）
- 组合后应符合公式预测

### 后续处理

使用 Python 脚本（如 `plot_read_memory.py`）绘制：
- X 轴：batch_size
- Y 轴：峰值内存（KB）
- 多条曲线：不同 N_cols
- 虚线：公式预测值 vs 实测值
