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

const ghpages = require('gh-pages');

ghpages.publish('src/.vuepress/dist', {
  branch: 'asf-staging',
  repo: 'https://github.com/apache/tsfile-website.git',
  message: 'Site checkin for project tsfile-website',
  dotfiles: true,
}, (err) => {
  if (err instanceof Error) {
    console.error(err);
    process.exit(1);
  }
});
