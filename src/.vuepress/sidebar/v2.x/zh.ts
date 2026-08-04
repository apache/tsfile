/*
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
 */

export const zhSidebar = {
  '/zh/UserGuide/latest/': [
    {
      text: 'TsFile 用户手册 (V2.x)',
      children: [],
    },
    {
      text: '走进时序数据',
      collapsible: true,
      link: 'QuickStart/Navigating_Time_Series_Data',
    },
    {
      text: '数据模型',
      collapsible: true,
      link: 'QuickStart/Data-Model',
    },
    {
      text: '文件格式',
      collapsible: true,
      link: '/zh/UserGuide/latest/FileFormat/',
      children: [
        { text: '二进制表示', link: '/zh/UserGuide/latest/FileFormat/Binary-Representation' },
        { text: '配置', link: '/zh/UserGuide/latest/FileFormat/Configurations' },
        { text: '扩展机制', link: '/zh/UserGuide/latest/FileFormat/Extensibility' },
        { text: '元数据', link: '/zh/UserGuide/latest/FileFormat/Metadata' },
        { text: '类型', link: '/zh/UserGuide/latest/FileFormat/Types' },
        { text: '布隆过滤器', link: '/zh/UserGuide/latest/FileFormat/Bloom-Filter' },
        {
          text: '数据页',
          collapsible: true,
          link: '/zh/UserGuide/latest/FileFormat/DataPages/',
          children: [
            { text: '数据块', link: '/zh/UserGuide/latest/FileFormat/DataPages/Chunks' },
            { text: '压缩', link: '/zh/UserGuide/latest/FileFormat/DataPages/Compression' },
            { text: '编码', link: '/zh/UserGuide/latest/FileFormat/DataPages/Encodings' },
            { text: '加密', link: '/zh/UserGuide/latest/FileFormat/DataPages/Encryption' },
            { text: '校验和', link: '/zh/UserGuide/latest/FileFormat/DataPages/Checksumming' },
            { text: '错误恢复', link: '/zh/UserGuide/latest/FileFormat/DataPages/Error-Recovery' },
          ],
        },
        { text: '空值', link: '/zh/UserGuide/latest/FileFormat/Nulls' },
        { text: '格式版本', link: '/zh/UserGuide/latest/FileFormat/Format-Versions' },
      ],
    },
    {
      text: '快速上手',
      collapsible: true,
      prefix: 'QuickStart/',
      children: [
        { text: '快速上手-C', link: 'QuickStart-C' },
        { text: '快速上手-C++', link: 'QuickStart-CPP' },
        { text: '快速上手-Java', link: 'QuickStart' },
        { text: '快速上手-Python', link: 'QuickStart-PYTHON' },
      ],
      // prefix: 'QuickStart/',
      // children: 'structure',
      // children: [
      //   { text: '快速上手', link: 'QuickStart' },
      // ],
    },
    {
      text: '接口定义',
      collapsible: true,
      prefix: 'QuickStart/InterfaceDefinition',
      children: [
        { text: '接口定义-C', link: 'InterfaceDefinition-C' },
        { text: '接口定义-C++', link: 'InterfaceDefinition-CPP' },
        { text: '接口定义-Java', link: 'InterfaceDefinition-Java' },
        { text: '接口定义-Python', link: 'InterfaceDefinition-Python' },
      ],
    },
    {
      text: 'TsFileDataFrame',
      collapsible: true,
      link: 'DataFrame/TsFileDataFrame',
    },
    {
      text: '工具',
      collapsible: true,
      prefix: 'Tools/',
      children: [
        { text: 'tsfile-cli', link: 'Tsfile-CLI' },
        { text: 'tsfile-viewer', link: 'Tsfile-Viewer' },
      ],
    },
    /* {
      text: '生态集成',
      collapsible: true,
      prefix: 'Ecosystem-Integration/',
      children: [
        { text: 'Apache Flink', link: 'Flink-TsFile' },
        { text: 'Apache Spark', link: 'Spark-TsFile' },           
        { text: 'Apache Hive', link: 'Hive-TsFile' },
      ],
    }, */
  ]
};
