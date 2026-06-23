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
# tsfile-viewer

[Apache TsFile Viewer](https://github.com/apache/tsfile-viewer) 是一个基于 Web 的应用，
用于在浏览器中浏览与分析 TsFile 数据。它由一个 Spring Boot 后端（通过 Apache TsFile 库读取
`.tsfile` 文件）与一个 Vue 3 前端（渲染元数据、分页表格与交互式图表）组成。

- **仓库**：<https://github.com/apache/tsfile-viewer>
- **许可证**：Apache-2.0

## 功能

- **文件浏览与上传**——在界面中打开 `.tsfile` 文件。
- **元数据展示**——schema、设备与测点。
- **分页数据表格**，支持按时间范围、设备、测点、数值范围过滤。
- **交互式图表**（ECharts），支持多序列叠加与聚合。
- **支持两种数据模型**——树模型与表模型 TsFile。
- **导出**——数据导出为 CSV 或 JSON；图表导出为 PNG 或 SVG。
- **性能**——chunk 级读取与元数据缓存。

## 环境要求

| 组件 | 版本 |
|---|---|
| JDK | 17 或 21（LTS） |
| Maven | 3.9+ |
| Node.js | `^20.19.0 \|\| >=22.12.0` |
| pnpm | 最新版 |
| Apache TsFile | 2.3.0（捆绑依赖） |

## 获取源码

克隆仓库，然后按下文构建并运行：

```bash
git clone https://github.com/apache/tsfile-viewer.git
cd tsfile-viewer
```

## 从源码运行（开发模式）

在两个终端分别运行后端与前端。

**后端**（Spring Boot）：

```bash
cd backend
mvn spring-boot:run
```

**前端**（Vue + Vite 开发服务器）：

```bash
cd frontend
pnpm install
pnpm dev
```

然后打开开发界面 <http://localhost:5173/view/>。

## 构建并运行生产包

构建一个自包含的发行包，再启动打好的 jar（前端由后端提供服务）：

```bash
./build-dist.sh
java -jar backend/target/tsfile-viewer-*.jar
```

打开应用 <http://localhost:8080/view/>。

## 使用

打开界面后，典型流程如下：

1. **选择文件**：在左侧文件树浏览服务器白名单目录并点击 `.tsfile`，或在右侧"上传 TsFile"卡片拖入/选择本地文件上传；最近打开的文件可在"最近访问"中快速重开。
2. **查看元数据**：进入元数据页查看版本、时间范围、设备/测点数等概要；用"视图模式"在树模型（测点、设备、Chunk）与表模型（表的 TAG/FIELD 列定义）间切换。
3. **预览数据**：点击"数据预览"，在左侧筛选面板按时间范围、设备、测点、数值范围过滤，并设置分页（每页条数/偏移量），以分页表格查看数据。
4. **可视化**：将所选序列绘制为交互式图表（ECharts），支持多序列叠加、聚合与下钻；大数据集会自动降采样。
5. **导出**：将筛选后的数据导出为 CSV / JSON，或将图表导出为 PNG / SVG。

更完整的操作说明见仓库内的[用户指南](https://github.com/apache/tsfile-viewer/blob/main/docs/USER_GUIDE.zh-CN.md)。
