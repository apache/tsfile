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

import { navbar } from "vuepress-theme-hope";

export const zhNavbar = navbar([
  {
    text: '文档',
    link: '/zh/UserGuide/latest/QuickStart/QuickStart',
    // children: [
    //   // { text: 'develop', link: '/zh/UserGuide/develop/QuickStart/QuickStart' },
    //   { text: 'v1.0.x', link: '/zh/UserGuide/latest/QuickStart/QuickStart' },
    // ],
  },
  // {
  //   text: '发布版本',
  //   link: '/zh/Download/',
  // },
  {
    text: '社区',
    children: [
      { text: '关于社区', link: '/zh/Community/About' },
      { text: '交流与反馈', link: '/zh/Community/Feedback' },
    ],
  },
  {
    text: '开发',
    children: [
      { text: '成为开发者', link: '/zh/Development/Community-Project-Committers' },
      { text: 'Power by', link: '/zh/Development/Powered-By' },
    ],
  },
  {
    text: 'ASF',
    children: [
      { text: '基金会', link: 'https://www.apache.org/' },
      { text: '许可证', link: 'https://www.apache.org/licenses/' },
      { text: '安全', link: 'https://www.apache.org/security/' },
      { text: '赞助', link: 'https://www.apache.org/foundation/sponsorship.html' },
      { text: '致谢', link: 'https://www.apache.org/foundation/thanks.html' },
      { text: '活动', link: 'https://www.apache.org/events/current-event' },
    ],
  },
]);
