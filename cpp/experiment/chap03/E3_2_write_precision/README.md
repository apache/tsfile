# E3-2：写入内存模型精度验证

## 状态：✅ 已完成

数据来源：`../../write_memory/`（已有程序 `write_memory.cpp` 跑出）

**产出**：T3-5（batch_size 5K/6K/8K/10K × 实测内存 / 公式估算 / 误差）

## 配置

- **编译**：C5
- **负载**：50 设备 × 50 FIELD（INT32）× 100K 行
- **变量**：batch_size = 5,000 / 6,000 / 8,000 / 10,000

## 如需复现

```bash
cd ../../write_memory
# 用 write_memory 程序，传入不同 batch_size 参数
./write_memory 5000000 5000 ...    # 50设备×100K行，batch=5K
./write_memory 5000000 6000 ...    # batch=6K
./write_memory 5000000 8000 ...    # batch=8K
./write_memory 5000000 10000 ...   # batch=10K
```

（具体参数见 `../../write_memory/run.sh`）
