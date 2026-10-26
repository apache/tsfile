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

import { getDirname, path } from 'vuepress/utils';
import { viteBundler } from '@vuepress/bundler-vite';
import { defineUserConfig } from "vuepress";
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

  extendsMarkdown: (md) => {
    md.options.breaks = false;
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
  bundler: viteBundler({
    vuePluginOptions: {
      template: {
        compilerOptions: {
          isCustomElement: (tag) => tag === 'center',
        },
      },
    },
  }),
  // Enable it with pwa
  // shouldPrefetch: false,
});
