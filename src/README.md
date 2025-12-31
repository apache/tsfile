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
heroText: Open-source File Format for Industrial Time-Series Datasets
tagline: TsFile is a high-performance columnar storage file format designed for industrial time-series data, featuring multi-language interfaces, high compression ratios, high read/write throughput, and fast random access capabilities. It is ideal for scenarios such as building high-quality datasets, managing data assets, conducting data analytics, and training AI models.
heroFullScreen: true
actions:
  - text: Download
    link: ./Download/
    type: primary

  - text: QuickStart
    link: ./UserGuide/latest/QuickStart/QuickStart
    type: primary

highlights:
  - header: Main Features
    # description: 
    # image: 
    # bgImage: https://theme-hope-assets.vuejs.press/bg/2-light.svg
    # bgImageDark: https://theme-hope-assets.vuejs.press/bg/2-dark.svg
    bgImage: bg.svg
    bgImageDark: bg.svg
    bgImageStyle:
      background-attachment: fixed
    features:
      - title: Efficient Storage and Compression
        details: TsFile employs advanced compression techniques to minimize storage requirements, resulting in reduced disk space consumption and improved system efficiency.

      - title: Flexible Schema and Metadata Management
        details: TsFile allows for directly write data without pre defining the schema, which is flexible for data aquisition.

      - title: High Query Performance with time range
        details: TsFile has indexed devices, sensors and time dimensions to accelerate query performance, enabling fast filtering and retrieval of time series data.

      - title: Seamless Integration
        details: TsFile is designed to seamlessly integrate with existing time series databases such as IoTDB, data processing frameworks, such as Spark and Flink.

copyright: false
---


