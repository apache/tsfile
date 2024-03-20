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

export const enNavbar = navbar([
  {
    text: 'Documentation',
    link: '/UserGuide/latest/QuickStart/QuickStart',
    // children: [
    //   // { text: 'develop', link: '/UserGuide/develop/QuickStart/QuickStart' },
    //   { text: 'v1.0.x', link: '/UserGuide/latest/QuickStart/QuickStart' },
    // ],
  },
  {
    text: 'Download',
    link: '/Download/',
  },
  // {
  //   text: 'Community',
  //   children: [
  //     { text: 'About', link: '/Community/About' },
  //     { text: 'Feedback', link: '/Community/Feedback' },
  //   ],
  // },
  // {
  //   text: 'Development',
  //   children: [
  //     { text: 'Become  a  Committer', link: '/Development/Community-Project-Committers' },
  //     { text: 'Power by', link: '/Development/Powered-By' },
  //   ],
  // },
  {
    text: 'ASF',
    children: [
      { text: 'Foundation', target:'_self', link: 'https://www.apache.org/' },
      { text: 'License', target:'_self', link: 'https://www.apache.org/licenses/' },
      { text: 'Security', link: 'https://www.apache.org/security/' },
      { text: 'Sponsorship', link: 'https://www.apache.org/foundation/sponsorship.html' },
      { text: 'Thanks', target:'_self', link: 'https://www.apache.org/foundation/thanks.html' },
      { text: 'Current  Events', link: 'https://www.apache.org/events/current-event' },
      { text: 'Privacy', link: 'https://privacy.apache.org/policies/privacy-policy-public.html' },
    ],
  },
]);
