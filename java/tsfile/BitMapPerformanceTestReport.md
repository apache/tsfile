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

# BitMap 长度 64 双实现测试报告

## 测试结论

测试通过。`BitMapLongImpl` 在长度为 64 时显著降低内存占用，并在单点读写、全量状态判断、`markAll/reset`、`clone`、`merge` 和 `append` 等主要位操作上优于 `BitMapArrayImpl`。

需要关注的代价是：`BitMapLongImpl` 调用字节数组序列化接口时需要临时生成 `byte[]`，因此 `getByteArray`、`getTruncatedByteArray` 和从字节数组构造的性能较慢。优化后的 `equals/equalsInRange` 不再生成临时数组，Long 实现比 Array 实现快约 43.6%；`hashCode` 同样已消除临时数组，但逐字节计算仍比 JVM 优化后的 `Arrays.hashCode` 慢约 34.5%。

## 测试信息

| 项目 | 内容 |
| --- | --- |
| 测试时间 | 2026-07-20 19:23–19:28（Asia/Hong_Kong） |
| Git 分支 | `develop` |
| Git 基线 | `b8825b18`，包含工作区内本次 BitMap 修改 |
| 操作系统 | Windows 11 amd64，10.0.26200.0 |
| 处理器 | Intel64 Family 6 Model 151，24 个逻辑处理器 |
| JDK | Oracle Java 21.0.8，64-bit HotSpot VM |
| Maven | Apache Maven 3.9.11 |
| BitMap 长度 | 64 |

执行命令：

```powershell
mvn.cmd -P with-java -pl java/tsfile -am test `
  "-Dtest=BitMapTest,TabletTest,FloatDecoderTest,BitMapPerformanceTest" `
  "-Dtsfile.runPerformanceTests=true" `
  "-Dsurefire.failIfNoSpecifiedTests=false"
```

## 正确性与回归测试

| 测试类 | 用例数 | Failure | Error | Skipped | 耗时 |
| --- | ---: | ---: | ---: | ---: | ---: |
| `BitMapTest` | 15 | 0 | 0 | 0 | 68.998 s |
| `TabletTest` | 15 | 0 | 0 | 0 | 0.268 s |
| `FloatDecoderTest` | 7 | 0 | 0 | 0 | 0.251 s |
| `BitMapPerformanceTest` | 1 | 0 | 0 | 0 | 1.592 s |
| **合计** | **38** | **0** | **0** | **0** | **71.109 s** |

Long 专用 `hashCode` 路径完成后，又定向执行了跨实现 equals/hash、动态工厂选择和性能测试，共 3 个用例，结果同样为 0 Failure、0 Error、0 Skipped。

验证范围包括：

- public 构造函数固定使用 `BitMapArrayImpl`，`createDynamicBitMap` 在长度不大于 64 时选择 `BitMapLongImpl`，以及 `64 -> 65` 扩容迁移；
- 单点和范围标记、重置、全量状态判断；
- 长度 64 的完整范围操作、字节数组转换、克隆，以及 Array–Long 双向 equals、范围比较和哈希一致性；
- `merge` 的长度、源偏移和目标偏移穷举组合；
- Tablet 序列化、反序列化、追加和空值位图处理；
- Float 编解码中 BitMap 的序列化与反序列化路径。

## 性能测试方法

- 两种实现均通过相同的 `BitMap` 公共接口执行；
- 每项测试预热 3 轮、测量 7 轮，交替改变两种实现的执行顺序；
- 报告值为 7 轮测量的中位数，单位为 ns/op；
- 写操作使用变化中的位置和状态，序列化/复制结果写入逃逸容器，降低 JIT 消除实际操作的可能；
- `Long/Array < 1` 表示 `BitMapLongImpl` 更快，`Long/Array > 1` 表示 `BitMapArrayImpl` 更快。

## 性能结果

| 操作 | ArrayImpl ns/op | LongImpl ns/op | Long/Array | 结果 |
| --- | ---: | ---: | ---: | --- |
| `isMarked` | 0.542 | 0.358 | 0.660x | Long 快约 34.0% |
| `isAllMarked/isAllUnmarked` | 5.173 | 1.715 | 0.331x | Long 快约 66.9% |
| `mark/unmark`（单次写） | 1.300 | 1.107 | 0.852x | Long 快约 14.8% |
| `markRange/unmarkRange`（单次写） | 3.048 | 2.301 | 0.755x | Long 快约 24.5% |
| `markAll/reset`（单次写） | 3.066 | 0.541 | 0.176x | Long 快约 82.4% |
| `getByteArray` | 1.497 | 4.858 | 3.246x | Long 慢约 3.25 倍 |
| `getTruncatedByteArray` | 8.987 | 14.068 | 1.565x | Long 慢约 56.5% |
| 从 `byte[]` 构造 | 8.027 | 10.218 | 1.273x | Long 慢约 27.3% |
| `clone` | 10.432 | 4.014 | 0.385x | Long 快约 61.5% |
| `merge` | 9.943 | 2.748 | 0.276x | Long 快约 72.4% |
| `append` | 85.240 | 61.024 | 0.716x | Long 快约 28.4% |
| `getRegion` | 114.356 | 88.900 | 0.777x | Long 快约 22.3% |
| `equals/equalsInRange` | 5.402 | 3.045 | 0.564x | Long 快约 43.6% |
| `hashCode` | 4.412 | 5.933 | 1.345x | Long 慢约 34.5% |
| `toString` | 151.272 | 125.010 | 0.826x | Long 快约 17.4% |

## 内存占用

内存数据由 `RamUsageEstimator` 按当前 JVM 的对象头、引用宽度和 8 字节对象对齐规则估算，包含 `BitMap` 外层对象、具体实现对象以及数组实现的 `byte[]`。

| 场景 | BitMapArrayImpl | BitMapLongImpl | 节省 |
| --- | ---: | ---: | ---: |
| 单个长度 64 的 BitMap | 72 bytes | 40 bytes | 32 bytes，44.44% |
| 1,000,000 个 BitMap（含引用数组） | 76,000,016 bytes | 44,000,016 bytes | 32,000,000 bytes，约 30.52 MiB |

## 分析与建议

1. 对以标记、查询、合并和克隆为主的长度不大于 64 的 BitMap，使用 `BitMapLongImpl` 可以同时获得更低内存占用和更好的性能。
2. `getByteArray` 是 Long 实现最明显的性能退化点，因为每次调用都需要将 `long` 展开为新的字节数组。应避免在高频路径中重复调用，可在调用方一次生成后复用结果。
3. `equals`、`equalsInRange` 和 `hashCode` 已改为实现层直接计算，不再间接生成字节数组。Long 的 equals 组合已优于 Array；Long 的 hashCode 虽已显著降低成本，但仍可考虑针对固定 9 字节序列进一步展开计算。
4. public 构造函数保留数组实现及其可变 `byte[]` 兼容语义；需要按长度节省内存时，应显式使用 `createDynamicBitMap`。

## 限制

本测试是项目内 JUnit 微基准，不是 JMH 基准。预热、中位数统计、交替执行和逃逸容器可以降低常见测量误差，但绝对 ns/op 仍会受到 CPU 频率、JIT 编译、GC 和后台负载影响。跨机器或跨 JVM 比较时应在相同环境中重新运行，重点观察相对倍率而非绝对耗时。
