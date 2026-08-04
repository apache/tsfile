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

export const enSidebar = {
  '/UserGuide/latest/': [
    {
      text: 'TsFile User Guide (V2.x)',
      children: [],
    },
    {
      text: 'Navigating Time Series Data',
      collapsible: true,
      link: 'QuickStart/Navigating_Time_Series_Data',
    },
    {
      text: 'Data Model',
      collapsible: true,
      link: 'QuickStart/Data-Model',
    },
    {
      text: 'File Format',
      collapsible: true,
      link: '/UserGuide/latest/FileFormat/',
      children: [
        { text: 'Binary Representation', link: '/UserGuide/latest/FileFormat/Binary-Representation' },
        { text: 'Configurations', link: '/UserGuide/latest/FileFormat/Configurations' },
        { text: 'Extensibility', link: '/UserGuide/latest/FileFormat/Extensibility' },
        { text: 'Metadata', link: '/UserGuide/latest/FileFormat/Metadata' },
        { text: 'Types', link: '/UserGuide/latest/FileFormat/Types' },
        { text: 'Bloom Filter', link: '/UserGuide/latest/FileFormat/Bloom-Filter' },
        {
          text: 'Data Pages',
          collapsible: true,
          link: '/UserGuide/latest/FileFormat/DataPages/',
          children: [
            { text: 'Chunks', link: '/UserGuide/latest/FileFormat/DataPages/Chunks' },
            { text: 'Compression', link: '/UserGuide/latest/FileFormat/DataPages/Compression' },
            { text: 'Encodings', link: '/UserGuide/latest/FileFormat/DataPages/Encodings' },
            { text: 'Encryption', link: '/UserGuide/latest/FileFormat/DataPages/Encryption' },
            { text: 'Checksumming', link: '/UserGuide/latest/FileFormat/DataPages/Checksumming' },
            { text: 'Error Recovery', link: '/UserGuide/latest/FileFormat/DataPages/Error-Recovery' },
          ],
        },
        { text: 'Nulls', link: '/UserGuide/latest/FileFormat/Nulls' },
        { text: 'Format Versions', link: '/UserGuide/latest/FileFormat/Format-Versions' },
      ],
    },
    {
      text: 'Quick Start',
      collapsible: true,
      prefix: 'QuickStart/',
      children: [
        { text: 'QuickStart-C', link: 'QuickStart-C' },
        { text: 'QuickStart-C++', link: 'QuickStart-CPP' },
        { text: 'QuickStart-Java', link: 'QuickStart' },
        { text: 'QuickStart-Python', link: 'QuickStart-PYTHON' },
      ],
      // prefix: 'QuickStart/',
      // // children: 'structure',
      // children: [
      //   { text: 'Quick Start', link: 'QuickStart' },
      // ],
    },
    {
      text: 'Interface Definitions',
      collapsible: true,
      prefix: 'QuickStart/InterfaceDefinition',
      children: [
        { text: 'InterfaceDefinition-C', link: 'InterfaceDefinition-C' },
        { text: 'InterfaceDefinition-C++', link: 'InterfaceDefinition-CPP' },
        { text: 'InterfaceDefinition-Java', link: 'InterfaceDefinition-Java' },
        { text: 'InterfaceDefinition-Python', link: 'InterfaceDefinition-Python' },
      ],
    },
    {
      text: 'TsFileDataFrame',
      collapsible: true,
      link: 'DataFrame/TsFileDataFrame',
    },
    {
      text: 'Tools',
      collapsible: true,
      prefix: 'Tools/',
      children: [
        { text: 'tsfile-cli', link: 'Tsfile-CLI' },
        { text: 'tsfile-viewer', link: 'Tsfile-Viewer' },
      ],
    },
    /* {
      text: 'Ecosystem Integration',
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
