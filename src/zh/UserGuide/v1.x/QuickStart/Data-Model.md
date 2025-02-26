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
# TsFile 数据模型

## 基本概念

为管理工业物联网时序数据，TsFile 的测点数据模型包含如下信息

- DeviceId（String）：设备名
- MeasurementSchema：测点
  - measurementId（String）：测点名
  - tsDataType（TSDataType）：数据类型

有关上述详细介绍，参见：[走进时序数据](https://tsfile.apache.org/zh/UserGuide/latest/QuickStart/Navigating_Time_Series_Data.html)

## 示例

![](/img/tsfile%E6%95%B0%E6%8D%AE%E6%A8%A1%E5%9E%8B.png)

在上述示例中，TsFile 的元数据（Schema）共包含 2 个设备，5条时间序列，建立为表结构如下图：

<table>
	<thead>
		<tr>
			<th rowspan="1">设备ID</th>
			<th rowspan="1">测点</th>
		</tr>
	</thead>
	<tbody>
		<tr>
			<td rowspan="2">太阳能板1</td>
			<td>电压（FLOAT）</td>
		</tr>
		<tr>
			<td>电流（FLOAT）</td>
		</tr>
		<tr>
			<td rowspan="4">风机1</td>
		</tr>
		<tr>
			<td>电压（FLOAT）</td>
		</tr>
		<tr>
			<td>电流（FLOAT）</td>
		</tr>
		<tr>
			<td>风速（FLOAT）</td>
		</tr>
	</tbody>
</table>