[English](README.md) | 中文

# tRPC-Python Database 插件

[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-green.svg)](TODO)

在日常的开发过程中，开发者经常会访问 MySQL、Redis、Kafka 等存储进行数据库的读写。直接使用开源的 SDK 虽然可以满足访问数据库的需求，但是用户需要自己负责路由寻址、监控上报、配置的开发。

考虑到 tRPC-Python 提供了多种多样的路由寻址、监控上报、配置管理的插件，我们可以封装开源 SDK，复用 tRPC-Python 的路由寻址、监控上报和配置管理能力，避免重复实现。

| 数据库 | 描述 |
| :----: | :----   |
| redis | 封装内存数据库 [redis-py](https://github.com/redis/redis-py) |
| kafka | 封装开源消息队列 Kafka SDK [aiokafka](https://github.com/aio-libs/aiokafka) |
| mysql | 封装开源数据库 [aiomysql](https://github.com/aio-libs/aiomysql) |
