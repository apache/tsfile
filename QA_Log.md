# 学习问答日志（QA_Log）

> 概念性 / 学习性问题的问答记录，从新到旧倒序。

## 2026-06-03 ｜ 分析一下现在这个方案是否符合 unix 的一贯用法

- **答案核心**：对照 Unix CLI 惯例分析 `tsfile-cli write`（CSV/TSV→tsfile）设计——
  - **符合**：stdin 输入 + `-` 约定、`-o` 给二进制输出（同 gcc/tar/zip/parquet/sqlite）、data 无 stdout 诊断走 stderr + 0/1/2/3 退出码、`--columns` 冒号逗号小语法（同 mount -o/ps -o/cut）、`-o` 静默覆盖（同 cp/sort -o）。
  - **被格式逼出来的合理偏离**：`write` 是 sink 不是 filter（输出只能落文件、不能进 stdout/续管道），因为 `.tsfile` 尾部写 footer 需 seek、pipe 不可 seek——与 tar/zip/parquet 同理，不必改。
  - **真正值得改（唯一）**：成功时打印摘要违反 *silence is golden*；应默认静默、摘要放 `-v/--verbose`。
  - **次要可商榷**：三个必填 flag + 不推断 + 跳过表头不校验 + 首列隐含为时间，是「正确性换便利」的取舍（惯例内）；「按表头名校验」可作可选 `--header-match`。
- **相关引用**：`docs/superpowers/specs/2026-06-03-tsfile-cli-write-design.md`；读侧 `2026-06-02-tsfile-cli-design.md` §8（stdout/stderr、退出码）；调研 `各文件格式CLI工具调研.md`（Parquet convert-csv / ORC convert / Avro fromjson 写路径）。

- **答案核心**：`.claude/skills/tsfile-cli/SKILL.md` 这类 project skill 是**给模型（Claude）用的说明书**，不是用户手动运行的命令。机制：
  - Claude Code 在**会话启动**时扫描 `.claude/skills/*/SKILL.md`，只载入 frontmatter 的 `name` + `description`；
  - 当用户请求与 `description` 的触发条件匹配时，模型用 **Skill 工具**把整篇正文拉进上下文再执行；
  - 用户用法：① 提相关需求自动触发（如“看 X.tsfile 的 schema/行数”），② 显式“用 tsfile-cli skill …”强制触发；
  - **刚创建的 skill 要新开会话才会被注册**；且必须在该仓库、含此文件的分支（当前在 `feat/tsfile-cli`，未合回 develop）下才可见；
  - 验证：新会话里问“列一下可用 skills”或丢个 `.tsfile` 让模型查，看是否声明 `Using tsfile-cli skill`。
- **相关引用**：`.claude/skills/tsfile-cli/SKILL.md`；superpowers `writing-skills`（CSO：`description` 只写“何时用”、不写流程）；本仓库 `docs/superpowers/specs/2026-06-02-tsfile-cli-design.md`。
