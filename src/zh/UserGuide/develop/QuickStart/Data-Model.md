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
# 数据模型

## 基础概念

为管理工业物联网时序数据，TsFile 的测点数据模型包含如下信息

| **概念**                  | **定义**                                                     |
| --------------------------- | ------------------------------------------------------------ |
| **表**          | 具有相同模式的设备集合。<br> 建模时定义的存储表由时间列、标签列和测点列三部分组成。|
| **时间列（TIME）**          | 每个时序表必须有一个时间列，数据类型为 TIMESTAMP，名称可以自定义<br>时间列的值不能为空，必须顺序的。 |
| **标签列（TAG）**           | 设备的唯一标识（联合主键），可以为 0 至多个<br>标识列的数据类型目前仅支持String，不指定时默认为String<br>标签信息不可修改和删除，但允许增加<br>推荐按粒度由大到小进行排列<br>写入时必须指定所有标识列（未指定的标识列默认使用 null 填充）|
| **测点列（FIELD）** | 一个设备采集的测点可以有1个至多个，值随时间变化<br>表的测点列没有数量限制，可以达到数十万以上<br>字段支持多种数据类型（与标签列固定为STRING类型不同）。 |

## 示例

表描述的是具有相同标签的设备的集合。如下图所示，它模拟了工厂设备的管理，每个设备的物理量采集都具备一定共性（如都采集温度和湿度物理量、同一设备的物理量同频采集等），因此可以逐个设备进行管理。

此时，物理设备可以通过3个标签【地区】-【工厂】-【设备】（下图橙色列，又称设备标签）进行唯一标识。设备最终采集的指标为【温度】、【湿度】、【状态】、【到达时间】（下图中的蓝色列）。


![](/img/Data-model01.png)

