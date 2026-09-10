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

# LabVIEW 单线程异步关闭设计规格

## 概述

在现有 LabVIEW C ABI 中增加可选的两阶段 Writer 关闭方式。调用方可以继续
同步关闭，也可以把 TsFile `close` 交给进程内唯一的后台关闭线程。异步模式
立即使 Writer Handle 失效并返回一个 Close Task Handle；调用方稍后通过
等待接口取得真正的关闭结果。

后台关闭槽位全进程唯一。如果提交新的异步关闭时上一次关闭仍未结束，提交
线程会一直等待上一次关闭完成，然后才启动本次关闭。因此连续文件轮转不会
积累后台线程，也不会让多个 TsFile Writer 同时执行 close。

本规格扩展
`2026-09-10-labview-bulk-io-design.zh.md`。其中“异步关闭不在首版范围”的
限制仅适用于此前的批量 I/O 工作，本次变更明确解除该限制。实现继续位于
`feat/labview-bulk-io`，不移动或改写 `ly/labview` 原始分支。

## 目标

- 用一个可选开关控制同步或异步关闭。
- 整个进程最多运行一个额外的 close 线程。
- 上一次后台 close 尚未结束时，下一次关闭提交同步等待，不创建第二个线程。
- 让文件轮转后的新 Writer 可以在旧文件后台收尾期间开始写入。
- 通过两阶段接口可靠返回底层 close 的最终状态。
- 保留现有同步关闭 API 和 ABI。
- 在动态库退出时等待尚未结束的 close，避免后台线程访问已卸载代码。

## 非目标

- 不把普通 block write 或显式 flush 放入后台线程。
- 不实现通用写入队列、队列容量、丢弃策略或背压。
- 不允许同一个 Writer Handle 上并发 write、flush 和 close。
- 不支持取消已经开始的 close。
- 不创建每个 Writer 独占的后台线程，也不允许多个 close 并行。
- 不提供超时、轮询或“等待指定毫秒”语义；等待接口始终等到任务完成。

## 公共 API

新增两个导出函数：

```c
LV_API LV_Status lv_tsfile_writer_close_ex(LV_Handle writer,
                                            int32_t async_close,
                                            LV_Handle* out_close_task);

LV_API LV_Status lv_tsfile_close_task_wait(LV_Handle close_task);
```

`lv_tsfile_writer_close_ex` 的约定：

- 入口先将 `*out_close_task` 置零。
- `async_close` 只接受 `0` 或 `1`。
- `async_close == 0` 时，在当前线程完成 close 和资源释放，返回最终 close
  状态，`out_close_task` 保持为零。
- `async_close == 1` 时，Writer Handle 立即从 Registry 注销。函数等待并
  join 此前的后台 close（若有），再启动本次后台 close。成功提交返回
  `E_OK` 和非零 Close Task Handle。
- 异步调用返回后，原 Writer Handle 永久失效；任何继续写入或 flush 都
  返回无效参数错误。
- 如果后台线程创建失败，函数在当前线程同步关闭当前 Writer，释放全部资源，
  保持 `out_close_task == 0`，并返回该同步关闭的最终状态。安全释放优先于
  异步性能。

`lv_tsfile_close_task_wait` 不接受超时参数。它一直等待指定任务结束，返回
底层 TsFile close 的最终状态，并消费 Close Task Handle。任务已经结束时
立即返回；无效 Handle 或重复等待返回 `E_INVALID_ARG`。

现有接口保持原签名：

```c
LV_API LV_Status lv_tsfile_writer_close(LV_Handle writer);
```

它内部执行同步关闭，行为等价于
`lv_tsfile_writer_close_ex(writer, 0, &unused_task)`。现有 LabVIEW VI、C 程序
和动态链接调用无需修改。

## 内部结构

增加 `kCloseTask` Handle 类型和 `CloseTaskCtx`。任务状态包含：

- 完成标记；
- 最终 `LV_Status`；
- 用于等待和通知的 mutex 与 condition variable。

Writer 的实际销毁集中到一个不抛异常的内部函数。它按固定顺序执行：

1. 调用底层 `tsfile_writer_close`；
2. 释放 `WriteFile`；
3. 释放缓存的 block Tablet；
4. 删除 `WriterCtx`；
5. 捕获所有 C++ 异常并转换为现有 `LV_Status`。

进程级 Close Coordinator 持有一个 `std::thread`。提交操作由独立 mutex
串行化，并在持有提交所有权期间 join 上一条线程，再创建下一条线程。后台
线程只拥有已经从 Handle Registry 注销的 `WriterCtx`，不读取 LabVIEW
传入的数组，也不再访问 Schema Builder 或 Tablet Handle。

后台线程结束时把最终状态写入 `CloseTaskCtx`，设置完成标记并通知等待者。
Close Task Handle 持有任务状态直到 `lv_tsfile_close_task_wait` 消费它；
即使调用方延迟等待，Writer 和文件资源也会在后台线程中及时释放。

Coordinator 析构时 join 尚可 join 的线程。正常调用仍要求宿主在卸载动态库
前等待最后一个 Close Task；析构 join 是最后的安全网，不替代显式等待和
错误检查。

## 并发与生命周期

同一 Writer 的调用顺序仍由调用方保证。`close_ex` 一旦取得 Writer Context，
该 Handle 即失效；它不与正在执行的 write 或 flush 协调。LabVIEW 必须在
写入循环内串行调用 block write、可选 flush 和 close_ex。

不同 Writer 可以存在于同一进程，但所有异步 close 共用一个后台槽位。
提交 mutex 覆盖“等待上一条线程并创建下一条线程”的完整过程，避免两个
调用方同时观察到空闲槽位后各自创建线程。

Close Task 只用于等待和读取最终状态，不能恢复 Writer。等待成功后 Handle
立即失效。调用方必须保存每次异步关闭返回的任务，并且至少等待一次；否则
会丢失关闭错误并在 Registry 中保留一个小型任务对象。

## 推荐的 LabVIEW 文件轮转流程

LabVIEW 写入循环在 shift register 中保存 `Writer Handle` 和前一个
`Close Task Handle`：

```text
持续写入当前文件
        |
达到轮转条件
        |
close_ex(current_writer, async_close=1) --> current_close_task
        |
立即 open 新文件并继续写
        |
下一次轮转或程序停止前：wait(current_close_task)
```

推荐在提交下一次 close 之前显式等待并检查前一个 Close Task。即使调用方
遗漏这一步，Close Coordinator 仍会在提交下一次异步关闭时等待上一个后台
线程，保证线程数量上限。程序最终停止时必须等待最后一个任务。

显式 flush 仍是同步操作。调用方可以在合适的非实时位置提前 flush，以减少
后台 close 的剩余工作，但 DAQ 定时循环不应直接执行 flush 或同步 close。

## 错误处理

- 所有异常在 C ABI 边界内转换为现有状态码。
- 无效 Writer、无效 Close Task、空输出指针或非法开关值返回
  `E_INVALID_ARG`。
- 异步提交成功只表示后台任务已启动，不代表文件已经关闭成功。
- 真正的文件关闭错误由 `lv_tsfile_close_task_wait` 返回。
- 后台线程创建失败时退化为同步关闭，确保 Writer Context 和文件句柄不泄漏。
- 一个任务无论成功或失败都只能等待并消费一次。

## 测试策略

实现遵循测试驱动开发，先添加失败测试，再实现生产代码。

- 验证现有 `lv_tsfile_writer_close` 仍同步关闭并生成可读文件。
- 验证 `close_ex(..., 0, ...)` 返回最终状态且不创建任务。
- 验证异步提交后 Writer Handle 立即失效。
- 验证 `close_task_wait` 一直等待、返回最终状态并消费 Handle。
- 验证同一任务重复等待和各种无效参数。
- 用可控的内部关闭动作验证同时运行的 close 最大值恒为 1。
- 验证第二次异步提交会等待第一条关闭线程完成，随后才启动新线程。
- 验证第一条任务的结果在被 Coordinator join 后仍可通过其 Task Handle 获取。
- 验证线程创建失败的同步降级路径不会泄漏 Writer。
- 验证 Coordinator 析构会等待最后一条线程。
- 执行现有 LabVIEW smoke、block、config 和完整 C++ 测试套件，确保 ABI 与
  文件兼容性没有回归。

## 文档与基准

更新 LabVIEW README 和中文操作手册，给出同步/异步 CLFN 参数映射、文件
轮转流程、最后任务等待要求和错误传播方式。

写入 benchmark 增加异步 close 阶段，分别报告：提交耗时、后台 close 完成
耗时，以及下一次关闭因上一任务未完成而产生的等待。性能测试只用于观察，
正确性不依赖固定时间阈值。
