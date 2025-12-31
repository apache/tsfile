---
home: true
icon: home
title: Apache TsFile
heroImage: home_icon.svg
bgImage: bg.svg
bgImageDark: bg.svg
bgImageStyle:
  background-attachment: fixed
  # background-repeat: repeat
  # background-size: initial
heroText: 物联网时序数据文件格式
tagline: TsFile 是一种为工业时序数据设计的高性能列式存储文件格式，支持多语言接口、高压缩比、高读写吞吐、快速随机访问。适用于构建高质量数据集、数据资产管理、数据分析与 AI 模型训练等场景。
heroFullScreen: true
actions:
  - text: 下载
    link: ./Download/
    type: primary

  - text: 快速上手
    link: ./UserGuide/latest/QuickStart/QuickStart
    type: primary

highlights:
  - header: 主要特点
    # description: 
    # image: 
    # bgImage: https://theme-hope-assets.vuejs.press/bg/2-light.svg
    # bgImageDark: https://theme-hope-assets.vuejs.press/bg/2-dark.svg
    bgImage: /bg.svg
    bgImageDark: /bg.svg
    bgImageStyle:
      background-attachment: fixed
    features:
      - title: 高效的存储和压缩
        details: TsFile采用了先进的压缩技术来最大限度地减少存储需求，从而减少了磁盘空间消耗并提高了系统效率。

      - title: 灵活元数据组织管理
        details: TsFile允许在不预先定义模式的情况下直接写入数据，支持数据灵活获取。

      - title: 高性能时间范围查询
        details: TsFile对设备、传感器和时间维度进行了索引，以加快查询性能，实现对时间序列数据的快速过滤和检索。

      - title: 大数据生态无缝集成
        details: TsFile能够与现有的时间序列数据库（如IoTDB）、数据处理框架（如Spark和Flink）无缝集成。

copyright: false
---

