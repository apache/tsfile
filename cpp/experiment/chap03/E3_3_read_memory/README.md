# E3-3：读取内存模型验证

## 状态：TODO

**产出**：F3-3（折线图）、T3-6（精度矩阵）

## 目的

验证读取内存公式：

```
M_read ≈ M_fixed + batch_size × s_row + N_cols × C_page
```

## 程序：`read_mem_model`

### 编译配置

**C5**（ENABLE_MEM_STAT=ON，需要 ModStat 按 AllocModID 追踪峰值内存）

### 实验参数矩阵

| 变量 | 取值 |
|------|------|
| N_cols（查询列数） | 1 / 4 / 8 / 16 |
| batch_size | 1,024 / 4,096 / 16,384 / 65,536 |

固定：W0 基线数据文件（10 设备，200M 行，SNAPPY）。读取时按列数选择对应的子集列。

### 关键 AllocModID

读取侧内存主要分布在：
- `MOD_TSBLOCK`：TsBlock 缓冲，随 batch_size 线性增长
- `MOD_DECODER_OBJ`：解压缓冲，随 N_cols 线性增长
- `MOD_DEFAULT` 等：固定开销 M_fixed

### 执行流程

1. 写入阶段（一次性）：若 W0 基线文件不存在则生成
2. 读取阶段：对每组 (N_cols, batch_size) 做全量扫描，ModStat 记录峰值
3. 计算公式估算值，与实测对比

### 输出

- `read_memory_results.csv`：`n_cols, batch_size, measured_kb, formula_kb, error_pct` + 各 AllocModID 明细
- `plot_read_memory.py`：折线图，x = batch_size，y = 峰值内存，不同 N_cols 为不同曲线，虚线为公式预测

### 预期结论

- TsBlock 缓冲随 batch_size 线性增长
- 解压缓冲随 N_cols 线性增长
- M_fixed 与两者均无关
