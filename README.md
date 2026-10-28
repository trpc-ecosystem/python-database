English | [中文](README.zh_CN.md)

# tRPC-Python Database Plugin

[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-green.svg)](TODO)

In daily development, developers often read and write storage systems such as MySQL, Redis, and Kafka. Open-source SDKs can access those databases, but developers still have to implement naming and routing, metrics reporting, and configuration themselves.

tRPC-Python already provides plugins for naming and routing, metrics reporting, and configuration management. Wrapping those open-source SDKs reuses these tRPC-Python capabilities and avoids duplicated implementations.

| Database | Description |
| :-------: | :---------- |
| redis | Wraps the in-memory database [redis-py](https://github.com/redis/redis-py) |
| kafka | Wraps the open-source Kafka message queue SDK [aiokafka](https://github.com/aio-libs/aiokafka) |
| mysql | Wraps the open-source database [aiomysql](https://github.com/aio-libs/aiomysql) |

## Copyright

The copyright notice pertaining to the Tencent code in this repo was previously in the name of “THL A29 Limited.”  That entity has now been de-registered.  You should treat all previously distributed copies of the code as if the copyright notice was in the name of “Tencent.”
