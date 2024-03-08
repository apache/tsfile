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
# 开发指南
## 开发约定
### 代码格式化 (应该没有变化，昊男确认)
我们使用 [Spotless plugin](https://github.com/diffplug/spotless/tree/main/plugin-maven) 和 [google-java-format](https://github.com/google/google-java-format) 格式化 Java 代码。你可以通过以下步骤将 IDE 配置为在保存时自动应用格式以 IDEA 为例）：
1.下载 [google-java-format-plugin v1.7.0.5](https://plugins.jetbrains.com/plugin/8527-google-java-format/versions/stable/83169), 安装到 IDEA(Preferences -> plugins -> search google-java-format),更详细的内容请查看[操作手册](https://github.com/google/google-java-format#intellij-android-studio-and-other-jetbrains-ides)
2.从磁盘安装 (Plugins -> little gear icon -> "Install plugin from disk" -> Navigate to downloaded zip file)
3.开启插件，并保持默认的 GOOGLE 格式 (2-space indents)
4.在 Spotless 没有升级到 18+之前，不要升级 google-java-format 插件
5.安装 [Save Actions](https://plugins.jetbrains.com/plugin/7642-save-actions) 插件 , 并开启插件，打开 "Optimize imports" and "Reformat file" 选项。
6.在“Save Actions”设置页面中，将 "File Path Inclusion" 设置为 .*.java”, 避免在编辑的其他文件时候发生意外的重新格式化

### 编码风格 (应该没有变化，昊男确认)
我们使用 [maven-checkstyle-plugin](https://checkstyle.sourceforge.io/config_filefilters.html)来保证所有的 Java 代码风格都遵循在项目根目录下的 [checkstyle.xml](https://github.com/apache/iotdb/blob/master/checkstyle.xml) 文件中定义的规则集.

您可以从该文件中查阅到所有的代码风格要求。当开发完成后，您可以使用 `mvn validate` 命令来检查您的代码是否符合代码风格的要求。

另外, 当您在集成开发环境开发时，可能会因为环境的默认代码风格配置导致和本项目的风格规则冲突。

在 IDEA 中，您可以通过如下步骤解决风格规则不一致的问题。
- 禁用通配符引用
    - 跳转至 Java 代码风格配置页面 (Preferences... -> 编辑器 -> 代码风格 -> Java)。
    - 切换到“导入”标签。
    - 在“常规”部分，启用“使用单个类导入”选项。
    - 将“将 import 与‘*’搭配使用的类计数”改成 999 或者一个比较大的值。
    - 将“将静态 import 与‘*’搭配使用的名称计数”改成 999 或者一个比较大的值。

## 贡献方式
### 参与投票 （问昊男投票方式）
- 1.给发布版本投票
  * 非中文用户，请阅读 https://cwiki.apache.org/confluence/display/IOTDB/Validating+a+staged+Release
- 2.下载投票的版本/rc下所有内容
   * https://dist.apache.org/repos/dist/dev/iotdb/

- 3.导入发布经理的公钥

    * https://dist.apache.org/repos/dist/dev/iotdb/KEYS

    * 最下边有 Release Manager (RM) 的公钥

    * 安装 gpg2

  - 第一种方法
  
    * 公钥的开头如下
        ```
        pub   rsa4096 2019-10-15 [SC]
            10F3B3F8A1201B79AA43F2E00FC7F131CAA00430
        ```   

        或是
        
        ```
        pub   rsa4096/28662AC6 2019-12-23 [SC]
        ```

    * 下载公钥

        ```
        gpg2 --receive-keys 10F3B3F8A1201B79AA43F2E00FC7F131CAA00430 （或 28662AC6)

        或 （指定 keyserver) 
        gpg2 --keyserver p80.pool.sks-keyservers.net --recv-keys 10F3B3F8A1201B79AA43F2E00FC7F131CAA00430 （或 28662AC6)
        ```

  - 第二种方法
    * 把下方内容复制到一个文本文件中，文本名为 ```key.asc```

        * 内容如下：
            ```
            -----BEGIN PGP PUBLIC KEY BLOCK-----
            Version: GnuPG v2
            ...
            -----END PGP PUBLIC KEY BLOCK-----
            ```

            导入 RM 的公钥到自己电脑

            ```
            gpg2 --import key.asc
  
            ```

- 4.验证源码发布版

  * 验证是否有 NOTICE、LICENSE，以及内容是否正确。

  * 验证 README、RELEASE_NOTES

  * 验证 header

    ```
    mvn -B apache-rat:check
    ```

  * 验证签名和哈希值

    ```
    gpg2 --verify apache-iotdb-0.12.0-source-release.zip.asc apache-iotdb-0.12.0-source-release.zip

    出现 Good Singnature 

    shasum -a512 apache-iotdb-0.12.0-source-release.zip

    和对应的 .sha512 对比，一样就可以。
    ```

  * 验证编译

    ```
    mvnw install

    应该最后全 SUCCESS
    ```
- 5.验证二进制发布版

    * 验证是否有 NOTICE、LICENSE，以及内容是否正确。

    * 验证 README、RELEASE_NOTES

    * 验证签名和哈希值

        ```
        gpg2 --verify apache-iotdb-0.12.0-bin.zip.asc apache-iotdb-0.12.0-bin.zip

        出现 Good Singnature 

        shasum -a512 apache-iotdb-0.12.0-bin.zip

        和对应的 .sha512 对比，一样就可以。
        ```

    * 验证是否能启动以及示例语句是否正确执行

        ```
        nohup ./sbin/start-server.sh >/dev/null 2>&1 &

        ./sbin/start-cli.sh

        CREATE DATABASE root.turbine;
        CREATE TIMESERIES root.turbine.d1.s0 WITH DATATYPE=DOUBLE, ENCODING=GORILLA;
        insert into root.turbine.d1(timestamp,s0) values(1,1);
        insert into root.turbine.d1(timestamp,s0) values(2,2);
        insert into root.turbine.d1(timestamp,s0) values(3,3);
        select * from root.**;

        打印如下内容：
        +-----------------------------------+------------------+
        |                               Time|root.turbine.d1.s0|
        +-----------------------------------+------------------+
        |      1970-01-01T08:00:00.001+08:00|               1.0|
        |      1970-01-01T08:00:00.002+08:00|               2.0|
        |      1970-01-01T08:00:00.003+08:00|               3.0|
        +-----------------------------------+------------------+

        ```
- 6.回复邮件

    验证通过之后可以发邮件了

    ```
    Hi,

    +1 (PMC could binding)

    The source release:
    LICENSE and NOTICE [ok]
    signatures and hashes [ok]
    All files have ASF header [ok]
    could compile from source: ./mvnw clean install [ok]

    The binary distribution:
    LICENSE and NOTICE [ok]
    signatures and hashes [ok]
    Could run with the following statements [ok]

    CREATE DATABASE root.turbine;
    CREATE TIMESERIES root.turbine.d1.s0 WITH DATATYPE=DOUBLE, ENCODING=GORILLA;
    insert into root.turbine.d1(timestamp,s0) values(1,1);
    insert into root.turbine.d1(timestamp,s0) values(2,2);
    insert into root.turbine.d1(timestamp,s0) values(3,3);
    select * from root.**;

    Thanks,
    xxx
    ```
### 贡献代码 
#### 贡献流程：
  - Apache IoTDB 社区通过 JIRA 上的 issue 进行任务管理。
Issue 的完整生命周期：创建 issue -> 认领 issue -> 提交 pr -> 审阅 pr -> 合并 pr -> 关闭 issue。

#### 创建 issue ：（xy确认方式）
  - 在 JIRA 上创建 issue 需要注意几个事项:
      * 1.命名：争取采用清晰易懂的名字，如支持一种新的聚合查询功能（avg）、优化原始数据查询性能等。Issue 的名字之后会作为发版的 release note。
      * 2.描述：新功能、优化需要描述具体希望做什么。 Bug 反馈需要描述环境、负载、现象描述（异常日志）、影响版本等，最好有复现方法。
#### 认领 issue ：（xy确认方式）
  - 在 JIRA 上认领 issue：分配给自己。建议添加一句评论：I'm doing this。避免与其他贡献者重复开发。
 <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/upload/issue.png">
    注：如果发现自己无法认领issue，则是因为自己的账户权限不够。遇到此情况，请向 dev@iotdb.apache.org 邮件列表发送邮件，标题为： [application] apply for permission to assign issues to XXX， 其中XXX是自己的JIRA用户名。
#### 提交PR (昊男确认流程)
  - 1.如何提交代码
    - 贡献途径：
      - IoTDB 诚邀广大开发者参与开源项目构建，您可以查看 [issues](https://issues.apache.org/jira/projects/IOTDB/issues) 并参与解决，或者做其他改善。
      - 提交 pr，通过 Travis-CI 测试和 Sonar 代码质量检测后，至少有一位以上 Committer 同意且代码无冲突，就可以合并了
    - PR指南：
      - 在 Github 上面可以很方便地提交 [Pull Request (PR)](https://help.github.com/articles/about-pull-requests/)，下面将以本网站项目 [apache/iotdb](https://github.com/apache/iotdb) 为例（如果是其他项目，请替换项目名 iotdb）
        * 1.Fork仓库：
          - 进入 apache/iotdb 的 [github 页面](https://github.com/apache/iotdb) ，点击右上角按钮 `Fork` 进行 Fork
        * 2.配置 git 和提交修改

          - 第一步：将代码克隆到本地：

          ```
          git clone https://github.com/<your_github_name>/iotdb.git
          ```

             **注意:请将 <your_github_name> 替换为您的 github 名字**

          clone 完成后，origin 会默认指向 github 上的远程 fork 地址。

          - 第二步：将 apache/iotdb 添加为本地仓库的远程分支 upstream：

          ```
          cd  iotdb
          git remote add upstream https://github.com/apache/iotdb.git
          ```

          - 第三步：检查远程仓库设置：

          ```
          git remote -v
          origin https://github.com/<your_github_name>/iotdb.git (fetch)
          origin    https://github.com/<your_github_name>/iotdb.git(push)
          upstream  https://github.com/apache/iotdb.git (fetch)
          upstream  https://github.com/apache/iotdb.git (push)
          ```

          - 第四步：新建分支以便在分支上做修改：（假设新建的分支名为 fix）

          ```
          git checkout -b fix
          ```

          创建完成后可进行代码更改。

          - 第五提交代码到远程分支：（此处以 fix 分支为例）

          ```
          git commit -a -m "<you_commit_message>"
          git push origin fix
          ```

          更多 git 使用方法请访问：[git 使用](https://www.atlassian.com/git/tutorials/setting-up-a-repository)
        * 3.Git 提交注意事项
            - 在 Git 上提交代码时需要注意：

              - 1.保持仓库的整洁：

                  - 不要上传二进制文件，保证仓库的大小只因为代码字符串的改动而增大。

                  - 不要上传生成的代码。

              - 2.日志要有含义：

                  - 题目用jira编号：[IOTDB-jira号]

                  - 题目用github的ISSUE编号：[ISSUE-issue号]

                      - 内容里要写#XXXX用于关联
          * 4.创建 PR

              在浏览器切换到自己的 github 仓库页面，切换分支到提交的分支 <your_branch_name> ，依次点击 `New pull request` 和 `Create pull request` 按钮进行创建，如果您解决的是 [issues](https://issues.apache.org/jira/projects/IOTDB/issues)，需要在开头加上 [IOTDB-xxx]。
              至此，您的 PR 创建完成，更多关于 PR 请阅读 [collaborating-with-issues-and-pull-requests](https://help.github.com/categories/collaborating-with-issues-and-pull-requests/)
           * 5. 冲突解决

              提交 PR 时的代码冲突一般是由于多人编辑同一个文件引起的，解决冲突主要通过以下步骤即可：

              步骤一：切换至主分支

              ```
              git checkout master
              ```

              步骤二：同步远端主分支至本地

              ```
              git pull upstream master
              ```

              步骤三：切换回刚才的分支（假设分支名为 fix）

              ```
              git checkout fix
              ```

              步骤四：进行 rebase

              ```
              git rebase -i master
              ```

              此时会弹出修改记录的文件，一般直接保存即可。然后会提示哪些文件出现了冲突，此时可打开冲突文件对冲突部分进行修改，将提示的所有冲突文件的冲突都解决后，执行

              ```
              git add .
              git rebase --continue
              ```

              依此往复，直至屏幕出现类似 *rebase successful* 字样即可，此时您可以进行往提交 PR 的分支进行更新：

              ```
              git push -f origin fix
              ```
  - 2.需提交的内容：（昊男确认）
    - Issue 类型：New Feature
  
      - 1.提交中英文版本的用户手册和代码修改的 pr。
  
          用户手册主要描述功能定义和使用方式，以便用户使用。用户手册建议包括：场景描述，配置方法，接口功能描述，使用示例。官网的用户手册内容放置在 apache/iotdb-docs 仓库 src 目录下，英文版放在 src/UserGuide ，中文版放在 src/zh/UserGuide 。
          如果需要更新用户手册，包括新增或删除文档和修改文档名，需要在 main 分支的 src/.vuepress/sidebar 中做相应修改。
        
      - 2.提交单元测试UT或集成测试IT
  
           需要增加单元测试UT 或集成测试IT，尽量覆盖多的用例。可以参考 xxTest（路径：iotdb/server/src/test/java/org/apache/iotdb/db/query/aggregation/）， xxIT（路径：iotdb/integration/src/test/java/org/apache/iotdb/db/integration/）。
    - Issue 类型：Improvement
  
      * 提交代码和 UT，一般不需要修改用户手册。最好提交相关实验结果，其中包含量化的改进效果和带来的副作用。
    - Issue 类型：Bug

      * 需要编写能够复现此 bug 的 单元测试 UT 或集成测试 IT。
  * 3.代码管理 （昊男确认）
    * a.分支管理：

       * IoTDB 版本命名方式为：0.大版本.小版本。例如 0.12.4，12 就是大版本，4 是小版本。

          master 分支作为当前主开发分支，对应下一个未发布的大版本，每个大版本发布时会切出一个单独的分支归档，如 0.12.x 系列版本的代码处于 rel/0.12 分支下。

          后续如果发现并修复了某发布版本的 bug。对这些 bug 的修复都需要往大于等于该版本对应的归档分支提 pr。如一个关于 0.11.x 版本 bug 修复的 pr 需要同时向 rel/0.11、rel/0.12 和 master 分支提交。

     * b.代码格式化:
       * 提交 PR 前需要使用 mvn spotless:apply 将代码格式化，再 commit，不然会导致 ci 代码格式化检查失败。

     * c.注意事项:
       *  iotdb-datanode.properties 和 IoTDBConfig 默认值需要保持一致。
       *  如果需要对配置参数进行改动。以下文件需要同时修改：
           1. 配置文件：iotdb-core/datanode/src/assembly/resources/conf/iotdb-datanode.properties
           2. 代码：IoTDBDescriptor、IoTDBConfig
           3. 文档：apache/iotdb-docs/src/UserGuide/{version}/Reference/DataNode-Config-Manual.md、apache/iotdb-docs/src/zh/UserGuide/{version}/Reference/DataNode-Config-Manual.md
            如果你想要在 IT 和 UT 文件中对配置参数进行修改，你需要在 @Before 修饰的方法里修改，并且在 @After 修饰的方法里重置，来避免对其他测试的影响。合并模块的参数统一放在CompactionConfigRestorer 文件里。
  * 4. PR 命名 
    * 命名方式：分支标签-Jira 标签-PR 名

      示例： [To rel/0.12] [TsFile-1907] implement customized sync process: sender

    * 分支标签

      如果是向非 master 分支提 pr，如 rel/0.13 分支，需要在 pr 名写上 [To rel/0.13]。如果是指向master分支，则不需要写分支标签。

    * Jira 标签

      以 JIRA 号开头，如：[TsFile-1907] implement customized sync process: sender。这样创建 PR 后，机器人会将 PR 链接自动链到对应 issue 上。

        注：如果创建 PR 时忘记添加 JIRA 号，或 JIRA 号不规范，则 PR 不会被自动链接到 Jira 上，需要先改正 PR 命名，并手动将 PR 链接贴到 issue 页面（通过留言或链接框）。 
  * 5. PR 描述
      通常 PR 名无法涵盖所有改动，需要添加具体描述，改动了哪些内容。对于较难理解的地方给予一定的解释。

      修改 bug 的 pr 需要描述 bug 出现的原因，以及解决方法，另外还需要描述UT/IT测试用例添加的情况和负面效果的描述。
  * 6.提交 PR 后

    向邮件列表 dev@tsfile.apache.org 发送一封邮件，主要介绍 PR 的工作。重视每个审阅者的意见，一一回复，并对达成一致的建议进行修改。

#### 审阅PR
- 注意事项：
  - 1. PR命名是否规范，新功能和bug修复类型的pr是否带了JIRA 号。
  - 2. PR 描述是否清晰。
  - 3. 功能测试用例或性能测试报告是否附上。
  - 4. 新功能是否有用户手册。
  - 5. 尽量不夹带其他问题的代码修改，将不相关的修改拆分到其他PR。
- 审阅流程：(更换截图)
  - 第一步： 点击 PR 的 Files changed 
<img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/zh/development/howtocontributecode/01.png">
  - 第二步：对于有问题的行，移动到左侧，会出现加号，点击加号，然后评论，点击 Start a review，此时，所有的 Review 意见都会暂存起来，别人看不到。
  <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/zh/development/howtocontributecode/02.png">
  - 第三步： 所有评论加完后，需要点击 Review changes，选择你的意见，已经可以合并的选择 Approve，有 Bug 需要改的选择 Request changes 或者 Comment，不确定的选择 Comment。最后 Submit review 提交审阅意见，提 PR 的人才能看见此意见。
  <img style="width:100%; max-width:800px; max-height:600px; margin-left:auto; margin-right:auto; display:block;" src="https://alioss.timecho.com/docs/img/zh/development/howtocontributecode/03.png">
#### 合并PR
- 确认所有审阅意见均已回复且有1个以上 committer 的Approval。

- 选择 squash merge （当且仅当作者仅有一个提交记录，且记录的commitlog清晰，可选择rebase）。

- 到 JIRA 上关闭对应的 issue，标记修复或完成的版本【注意，解决或关闭 issue 都需要对 issue 添加 pr 或描述，通过 issue 要能够追踪这个任务的变动】。

### 贡献文档

贡献用户手册和贡献代码的流程是一样的，只是修改的文件不同。
用户手册的英文版放在 tsfile/docs/src/UserGuide ,  中文版放在 src/UserGuide/zh 下。
如果需要更新用户手册目录，包括新增或删除md文档、修改md文档名，需要在 main 分支的 src/.vuepress/sidebar 中做相应修改。


### 新功能、Bug 反馈、改进等
所有希望 TsFile 做的功能或修的 bug，都可以在 Jira 上提 issue

可以选择 issue 类型：bug、improvement、new feature 等。新建的 issue 会自动向邮件列表中同步邮件，之后的讨论可在 jira 上留言，也可以在邮件列表进行。当问题解决后请关闭 issue。

### 邮件讨论内容
请使用英文进行讨论：
  * 第一次参与邮件列表可以简单介绍一下自己。（Hi, I'm xxx ...)

  * 开发功能前可以发邮件声明一下自己想做的任务。（Hi，I'm working on issue TsFile-XXX，My plan is ...）


## 常见资料 (xy)
- IoTDB 官网：https://iotdb.apache.org/

- 代码库：https://github.com/apache/iotdb/tree/master

- Go语言的代码库：https://github.com/apache/iotdb-client-go

- 文档库：https://github.com/apache/iotdb-docs

- 资源库（包含项目文件等）：https://github.com/apache/iotdb-bin-resources

- 快速上手：https://iotdb.apache.org/zh/UserGuide/V1.1.x/QuickStart/QuickStart.html

- Jira 任务管理：https://issues.apache.org/jira/projects/IOTDB/issues

- Wiki 文档管理：https://cwiki.apache.org/confluence/display/IOTDB/Home

- 邮件列表: https://lists.apache.org/list.html?dev@iotdb.apache.org

- 每日构建: https://ci-builds.apache.org/job/IoTDB/job/IoTDB-Pipe/job/master/

- Slack: https://apacheiotdb.slack.com/join/shared_invite/zt-qvso1nj8-7715TpySZtZqmyG5qXQwpg#/shared-invite/email