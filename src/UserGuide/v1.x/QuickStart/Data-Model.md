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

# Data Model

## Basic Concepts

To manage industrial IoT timing data, the measurement point data model of TsFile includes the following information

- DeviceId（String）：Device Name
- MeasurementSchema：Measurement points
  - measurementId（String）：Measurement Point Name
  - tsDataType（TSDataType）：Data Type

For the above detailed introduction, please refer to：[Entering Time Series Data](https://tsfile.apache.org/UserGuide/latest/QuickStart/Navigating_Time_Series_Data.html)

## Example

![](/img/20240502164237-dkcm.png)

In the above example, the metadata (Scheme) of TsFile contains 2 devices and 5 time series, and is established as a table structure as shown in the following figure:


<table>
  <thead>
    <tr>
      <th>Device ID</th>
      <th>Measurement points</th>
    </tr>
  </thead>
	<tbody>
		<tr>
			<td rowspan="2">Solar panel 1</td>
			<td>Voltage（FLOAT）</td>
		</tr>
		<tr>
			<td>Current（FLOAT）</td>
		</tr>
		<tr>
			<td rowspan="4">Fan1</td>
		</tr>
		<tr>
			<td>Voltage（FLOAT）</td>
		</tr>
		<tr>
			<td>Current（FLOAT）</td>
		</tr>
		<tr>
			<td>Wind Speed（FLOAT）</td>
		</tr>
	</tbody>
</table>