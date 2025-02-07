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

- 标签列：以字符串键值对形式对一维物联网设备的描述。例如，“category=XT451”表示风力涡轮机的类别为“XT451”，“year=2021”表示其建造于2021年。
- 测点列：设备正在测量的变量。与风扇速度、电压、温度、风速等类似。与目前固定为STRING数据类型的标记不同，字段受各种数据类型的支持。
- 时间戳和时间序列：设备测量的FIELD的每个值都与一个唯一的时间戳相关联。FIELD的时间戳和值的序列是一个时间序列。
- 元数据：一组可以唯一标识一种设备的标签，以及由这些设备测量的字段集。例如，风力涡轮机可以通过“省”、“市”、“风场”和“序列号”进行唯一定位，因此方案“wind_turbine”的TAG集可以是“省”，“市”，“风场“和“序列编号”。而其FIELD集包含“风扇速度”、“电压”、“温度”、“风速”等。
- 设备号：物联网设备的唯一标识符，由模式名称和所有标签值组成。例如，对于上述模式“wind_turbine”，DeviceId可以是（“wind_urbine”、“Beijing”、“Beijing_1135”、“T1523678”）。

<table>       
  <tr>             
    <th rowspan="1">概念</th>             
    <th rowspan="1">定义</th>                          
  </tr>       
  <tr>             
    <th rowspan="1">表</th>
      <th>一类具有相同模式的设备的集合。建模时定义的存储表由标识列、时间列和物理量列三部分组成。</th>    
  </tr>  
  <tr>
    <th rowspan="1">标识列</th>
  	<th>设备唯一标识，一个表内可包含0至多个标识列，标识列的值按建表时的列顺序组合形成的复合值称为标识，复合值相同的标识为同一标识。标识列的数据类型目前只能为String，可以不指定，默认为String标识列的值可以全为空写入时必须指定所有标识列（未指定的标识列默认使用 null 填充）</th>
  </tr>
  <tr>
    <th rowspan="1">时间戳</th>  
    <th>一个表必须有一列时间列，相同标识取值的数据默认按时间排序。时间列的值不能为空，必须顺序的。</th>
  </tr> 
  <tr>             
    <th rowspan="1">测点列</th>  
    <th>测点列定义了时序数据的测点名称、数据类型。</th>
  </tr> 
  <tr> 
    <th rowspan="1">行</th>  
    <th>表中的一行数据</th>
  </tr> 
</table>


## 示例

元数据描述的是具有相同模式的设备的集合。如下图所示，它模拟了工厂设备的管理，每个设备的物理量采集都具备一定共性（如都采集温度和湿度物理量、同一设备的物理量同频采集等），因此可以逐个设备进行管理。

此时，物理设备可以通过3个标签[区域]-[工厂]-[设备]（下图中的橙色列，也称为设备标识信息）进行唯一标识。设备收集的字段为[温度]、[湿度]、[状态]和[到达时间]（下图中的蓝色列）。

![](https://alioss.timecho.com/docs/img/data_model_example_image-zh.png)