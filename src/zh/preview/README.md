---
title: 在线预览
sidebar: false
---

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

打开一个 `.tsfile` 文件即可查看其中的序列，无需安装任何东西——文件由编译成
WebAssembly 的 TsFile 读取器在浏览器内解码，不会上传到任何服务器。

<iframe
  class="tsfile-preview-frame"
  src="/preview-app/index.html"
  title="TsFile 在线预览"
></iframe>
