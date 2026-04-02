# common — 跨实验共用工具

本目录提供所有实验程序共享的数据生成逻辑，避免每个程序各自重复实现。

## 待实现文件

### `data_gen.h`

提供统一的数据生成接口：

```cpp
// 构造标准 TableSchema（基线：2 TAG + N FIELD，涵盖 INT32/INT64/FLOAT/DOUBLE）
std::shared_ptr<storage::TableSchema> make_bench_schema(int n_field_cols,
                                                         CompressionType comp = SNAPPY);

// 向 TsFileTableWriter 写入 total_rows 行（batch_size 行一批）
// devices 个设备，每设备数据顺序写入
int write_bench_data(storage::TsFileTableWriter* writer,
                     int n_devices, int64_t total_rows, uint32_t batch_size);
```

### `bench_utils.h`

```cpp
// 高精度计时器包装
struct Timer { void start(); double elapsed_sec(); };

// CSV 输出工具
struct CsvWriter {
    void write_header(const std::vector<std::string>& cols);
    void write_row(const std::vector<std::string>& vals);
};

// 打印构建配置（SIMD/THREADS 开关）
void print_build_config();
```

## 数据集基线参数

详见 `../plan.md` 第 4 节。各实验在此基础上只修改自己关注的变量。
