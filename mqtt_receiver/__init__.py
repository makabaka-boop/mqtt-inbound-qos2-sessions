"""MQTT 3.1.1 QoS2 入站接收器（非消息代理）。

模块组成：
- codec:  手写的 MQTT 3.1.1 报文编解码与流式分包；
- store:  SQLite 持久化（会话、QoS2 交换状态、业务收件账）；
- server: TCP 入口与 QoS2 接收方状态机；
- query:  只读收件查询 CLI。
"""

__version__ = "1.0.0"
