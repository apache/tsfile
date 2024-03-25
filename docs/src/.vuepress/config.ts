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

import { getDirname, path } from '@vuepress/utils';
import { defineUserConfig } from "vuepress";
import { docsearchPlugin } from './components/docsearch/node/index.js';
import theme from "./theme.js";

const dirname = getDirname(import.meta.url);

export default defineUserConfig({
  base: "/",

  locales: {
    "/": {
      lang: "en-US",
      title: "Apache TsFile",
      description: "File Format for Internet of Things",
    },
    "/zh/": {
      lang: "zh-CN",
      title: "Apache TsFile",
      description: "物联网时序数据文件格式",
    },
  },

  theme,
  head: [
    ['link', { rel: 'icon', href: '/favicon.ico' }],
    ['script', { type: 'text/javascript' }, `
 var _paq = window._paq = window._paq || [];
 /* tracker methods like "setCustomDimension" should be called before "trackPageView" */
 _paq.push(["setDoNotTrack", true]);
 _paq.push(["disableCookies"]);
 _paq.push(['trackPageView']);
 _paq.push(['enableLinkTracking']);
 (function() {
   var u="https://analytics.apache.org/";
   _paq.push(['setTrackerUrl', u+'matomo.php']);
   _paq.push(['setSiteId', '53']);
   var d=document, g=d.createElement('script'), s=d.getElementsByTagName('script')[0];
   g.async=true; g.src=u+'matomo.js'; s.parentNode.insertBefore(g,s);
 })();
    `],
  ],
  alias: {
    '@theme-hope/components/PageFooter': path.resolve(
      dirname,
      './components/PageFooter.vue',
    ),
    // '@theme-hope/modules/info/utils/index': path.resolve(
    //   dirname,
    //   './utils/index',
    // ),
  },
  plugins: [
    docsearchPlugin({
      appId: 'JLT9R2YGAE',
      apiKey: '5d062598828a610e4f9e6d9d3389ae45',
      indexName: 'iotdb-apache_tsfile',
      // disableUserPersonalization: true,
      locales: {
        '/zh/': {
          placeholder: '搜索文档',
          translations: {
            button: {
              buttonText: '搜索文档',
              buttonAriaLabel: '搜索文档',
            },
            modal: {
              searchBox: {
                resetButtonTitle: '清除查询条件',
                resetButtonAriaLabel: '清除查询条件',
                cancelButtonText: '取消',
                cancelButtonAriaLabel: '取消',
              },
              startScreen: {
                recentSearchesTitle: '搜索历史',
                noRecentSearchesText: '没有搜索历史',
                saveRecentSearchButtonTitle: '保存至搜索历史',
                removeRecentSearchButtonTitle: '从搜索历史中移除',
                favoriteSearchesTitle: '收藏',
                removeFavoriteSearchButtonTitle: '从收藏中移除',
              },
              errorScreen: {
                titleText: '无法获取结果',
                helpText: '你可能需要检查你的网络连接',
              },
              footer: {
                selectText: '选择',
                navigateText: '切换',
                closeText: '关闭',
                searchByText: '搜索提供者',
              },
              noResultsScreen: {
                noResultsText: '无法找到相关结果',
                suggestedQueryText: '你可以尝试查询',
                reportMissingResultsText: '你认为该查询应该有结果？',
                reportMissingResultsLinkText: '点击反馈',
              },
            },
          },
        },
      },
    }),
  ],
  // Enable it with pwa
  // shouldPrefetch: false,
});
