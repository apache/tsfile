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
# 走进时间序列数据

## 什么是时间序列数据？

在当今的物联网时代，物联网、工业等各种场景正在经历数字化转型。人们通过在设备上安装传感器来采集其各种状态数据。例如，电机采集电压和电流；风扇采集叶片转速、角速度和发电量；车辆采集经纬度、速度和油耗；桥梁采集振动频率、挠度、位移等。传感器的数据采集已经深入到各个行业。

![](/img/20240505154735.png)

通常，我们将每一个采集点称为测量点（也称为物理量、时间序列、时间线、信号量、指标、测量值等）。每个测量点会随着时间持续采集新的数据信息，形成一条时间序列。从表格的角度来看，每个时间序列是一张由“时间”和“数值”两列组成的表；从图形的角度来看，每条时间序列是一张随时间变化的趋势图，也可以形象地称作设备的“心电图”。

![](/img/20240505154843.png)

传感器产生的海量时间序列数据，是各行业数字化转型的基础，因此我们对时间序列数据的建模主要聚焦于设备和传感器。

## 时间序列数据的关键概念

时间序列数据中涉及的主要概念可以自底向上分为：数据点、测量点和设备。

![](/img/20240505154513.png)

### 数据点

- 定义：由一个时间戳和一个数值组成，其中时间戳为 long 类型，值可以是 BOOLEAN、FLOAT、INT32 等多种类型。
- 示例：在上图中，表格形式中的一行时间序列，或图形形式中的一个时间序列上的点，都是一个数据点。

![](/img/20240505154432.png)

### 测量点

- 定义：由多个按照时间戳递增排列的数据点构成的时间序列。通常一个测量点表示一个采集点，并能定期采集其所处环境中的物理量。
- 也称为：物理量、时间序列、时间线、信号量、指标、测量值等。
- 示例：
  - 电力场景：电流、电压
  - 能源场景：风速、转速
  - 车联网场景：油耗、车速、经度、纬度
  - 工厂场景：温度、湿度

### 设备

- 定义：对应实际场景中的一个物理设备，通常是多个测量点的集合，并通过一个或多个标签进行标识
- 示例：
  - 车联网场景：通过车辆识别码（VIN）标识的车辆
  - 工厂场景：机械臂，通过物联网平台生成的唯一 ID 进行标识
  - 能源场景：风电机组，通过区域、站点、线路、型号、实例等维度进行标识
itoring scenario: CPU, identified by machine room, rack, Hostname, device type, etc