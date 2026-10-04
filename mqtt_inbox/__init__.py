"""MQTT 3.1.1 入站接收器：把 QoS 2 消息落进 SQLite 业务收件账。

仅实现入站接收状态机（CONNECT / QoS2 PUBLISH / PUBREL / PINGREQ / DISCONNECT），
不做订阅、不做转发、不是消息代理。不依赖任何 MQTT broker 库。
"""

__version__ = "1.0.0"
