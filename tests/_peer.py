"""测试用原始 MQTT 对端：直接发手工构造的字节，支持任意拆包/粘连。

刻意不使用任何 MQTT 客户端库，以便：
- 按字节、按任意 chunk 边界发送（真实 TCP 拆包）；
- 多个报文合并一次 send（粘连）；
- 发送带非法标志/非法类型的报文。
"""

from __future__ import annotations

import socket
import time

from mqtt_inbox import framing


class MqttPeer:
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.reader = framing.FrameReader(self.sock)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    # -- 原始发送（可拆包/粘连） --------------------------------------------

    def raw(self, data: bytes) -> None:
        self.sock.sendall(data)

    def chunked(self, packets: list[bytes], chunk_size: int) -> None:
        blob = b"".join(packets)
        for i in range(0, len(blob), chunk_size):
            self.sock.sendall(blob[i:i + chunk_size])
            time.sleep(0.003)

    def coalesced(self, packets: list[bytes]) -> None:
        self.sock.sendall(b"".join(packets))

    # -- 读取 ---------------------------------------------------------------

    def read_packet(self):
        first, body = self.reader.read_packet()
        return first, body

    def expect_closed(self, timeout: float = 5.0) -> bool:
        """服务器关闭连接时返回 True。"""
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(1)
            return data == b""
        except socket.timeout:
            return False
        finally:
            self.sock.settimeout(timeout)

    def expect_connack(self):
        first, body = self.reader.read_packet()
        assert (first & 0xF0) >> 4 == framing.CONNACK
        return framing.parse_connack(body)

    def expect_pubrec(self, packet_id: int):
        first, body = self.reader.read_packet()
        pid = framing.parse_pid_packet(first, body, framing.PUBREC, 0x00)
        assert pid == packet_id
        return pid

    def expect_pubcomp(self, packet_id: int):
        first, body = self.reader.read_packet()
        pid = framing.parse_pid_packet(first, body, framing.PUBCOMP, 0x00)
        assert pid == packet_id
        return pid

    # -- 高层步骤 -----------------------------------------------------------

    def connect(self, client_id: str, *, clean: bool = True, keepalive: int = 0,
                **kwargs) -> tuple[bool, int]:
        self.raw(framing.encode_connect(
            client_id, clean_session=clean, keepalive=keepalive, **kwargs))
        return self.expect_connack()

    def publish(self, topic: str, payload: bytes, packet_id: int, *,
                dup: bool = False, qos: int = 2, retain: bool = False) -> None:
        self.raw(framing.encode_publish(
            topic, payload, packet_id, dup=dup, qos=qos, retain=retain))

    def publish_chunked(self, topic: str, payload: bytes, packet_id: int,
                        chunk_size: int = 1) -> None:
        packet = framing.encode_publish(topic, payload, packet_id)
        # 先把 CONNECT 后的这一帧按 chunk 拆开发
        for i in range(0, len(packet), chunk_size):
            self.sock.sendall(packet[i:i + chunk_size])
            time.sleep(0.002)

    def pubrel(self, packet_id: int) -> None:
        self.raw(framing.encode_pubrel(packet_id))

    def ping(self) -> None:
        self.raw(framing.encode_pingreq())
        first, body = self.reader.read_packet()
        assert (first & 0xF0) >> 4 == framing.PINGRESP

    def disconnect(self) -> None:
        self.raw(framing.encode_disconnect())

    # -- 构造非法报文 --------------------------------------------------------

    @staticmethod
    def raw_packet(first_byte: int, body: bytes = b"") -> bytes:
        return bytes([first_byte]) + framing.encode_remaining_length(len(body)) + body

    @staticmethod
    def malformed_remaining_length(prefix: bytes) -> bytes:
        """例如 4 个 0x80 续位字节（无终止字节）。"""
        return prefix
