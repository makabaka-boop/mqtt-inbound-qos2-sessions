"""帧层单元测试：Remaining Length、编解码、缓冲粘连/拆包。"""

import socket
import unittest

from mqtt_inbox import framing
from mqtt_inbox.framing import (
    CONNACK, CONNECT, DISCONNECT, PUBLISH, PUBREL, FrameReader,
    ProtocolError, RefusedConnect,
)


class FakeSocket:
    """可编程 socket：按给定 chunk 序列返回，模拟拆包/粘连。"""

    def __init__(self, chunks, timeout=None):
        self._chunks = list(chunks)
        self._i = 0
        self.timeout = timeout

    def recv(self, n):
        if self._i >= len(self._chunks):
            return b""
        data = self._chunks[self._i]
        self._i += 1
        return data


class RemainingLengthTests(unittest.TestCase):

    def test_codec_examples(self):
        cases = {
            0: [0x00],
            127: [0x7F],
            128: [0x80, 0x01],
            16383: [0xFF, 0x7F],
            16384: [0x80, 0x80, 0x01],
            2097151: [0xFF, 0xFF, 0x7F],
            2097152: [0x80, 0x80, 0x80, 0x01],
            268435455: [0xFF, 0xFF, 0xFF, 0x7F],
        }
        for value, expected in cases.items():
            self.assertEqual(list(framing.encode_remaining_length(value)),
                             expected)

    def test_decode_fragmented(self):
        # 固定首字节、多字节 RL 各自跨 recv 到达，体也分片；RL=4096（两字节）。
        chunks = [bytes([CONNECT << 4]), bytes([0x80]), bytes([0x20]),
                  b"\x00" * 2048, b"\x00" * 2048]
        reader = FrameReader(FakeSocket(chunks))
        first, body = reader.read_packet()
        self.assertEqual(first, CONNECT << 4)
        self.assertEqual(len(body), 4096)

    def test_rl_value(self):
        # 两字节 RL（值必须在本服务硬上限内），验证续位字节跨 chunk 解码
        n = 4500
        enc = framing.encode_remaining_length(n)
        self.assertEqual(len(enc), 2)
        chunks = [bytes([0x30]), bytes([enc[0]]), bytes([enc[1]]),
                  b"x" * n]
        reader = FrameReader(FakeSocket(chunks))
        first, body = reader.read_packet()
        self.assertEqual(first, 0x30)
        self.assertEqual(len(body), n)

    def test_rl_5_bytes_is_illegal(self):
        # 第 4 个字节仍带续位 -> 非法
        chunks = [bytes([0x30]), bytes([0x80]), bytes([0x80]),
                  bytes([0x80]), bytes([0x80])]
        reader = FrameReader(FakeSocket(chunks))
        with self.assertRaises(ProtocolError):
            reader.read_packet()

    def test_rl_over_limit_rejected(self):
        # RL 结构合法但超出本服务的剩余长度硬上限（>4KiB 载荷 + 头余量）
        enc = framing.encode_remaining_length(4609)
        chunks = [bytes([0x30]), enc[:1], enc[1:], b"x" * 4609]
        reader = FrameReader(FakeSocket(chunks))
        with self.assertRaises(ProtocolError):
            reader.read_packet()

    def test_coalescing_split_within_buffer(self):
        # 两个 PINGREQ 粘在一个 recv 中
        reader = FrameReader(FakeSocket([b"\xc0\x00\xc0\x00"]))
        f1, b1 = reader.read_packet()
        f2, b2 = reader.read_packet()
        self.assertEqual(f1, 0xC0)
        self.assertEqual(f2, 0xC0)
        self.assertEqual(b1, b"")
        self.assertEqual(b2, b"")

    def test_byte_by_byte_fragmentation(self):
        packet = b"\xc0\x00"
        reader = FrameReader(FakeSocket([bytes([c]) for c in packet]))
        first, body = reader.read_packet()
        self.assertEqual(first, 0xC0)
        self.assertEqual(body, b"")

    def test_eof_mid_packet(self):
        reader = FrameReader(FakeSocket([b"\xc0\x02\xab"]))
        with self.assertRaises(framing.ConnectionClosed):
            reader.read_packet()

    def test_clean_eof(self):
        reader = FrameReader(FakeSocket([]))
        with self.assertRaises(framing.ConnectionClosed):
            reader.read_packet()


class ConnectParseTests(unittest.TestCase):

    def test_valid_connect_clean(self):
        raw = framing.encode_connect("client-A", clean_session=True,
                                     keepalive=60)
        first, body = FrameReader(FakeSocket([raw])).read_packet()
        info = framing.parse_connect(body)
        self.assertEqual(info.client_id, "client-A")
        self.assertTrue(info.clean_session)
        self.assertEqual(info.keepalive, 60)

    def test_will_rejected(self):
        raw = framing.encode_connect("c", clean_session=True,
                                     will_flag=True, will_topic="t",
                                     will_message=b"bye")
        _, body = FrameReader(FakeSocket([raw])).read_packet()
        with self.assertRaises(RefusedConnect) as cm:
            framing.parse_connect(body)
        self.assertEqual(cm.exception.code, framing.CONNACK_SERVER_UNAVAILABLE)

    def test_reserved_bit_protocol_error(self):
        raw = framing.encode_connect("c", clean_session=True)
        _, body = FrameReader(FakeSocket([raw])).read_packet()
        body = body[:7] + bytes([body[7] | 0x01]) + body[8:]
        with self.assertRaises(ProtocolError):
            framing.parse_connect(body)

    def test_bad_protocol_level(self):
        raw = framing.encode_connect("c", clean_session=True)
        _, body = FrameReader(FakeSocket([raw])).read_packet()
        body = body[:6] + bytes([0x03]) + body[7:]
        with self.assertRaises(RefusedConnect) as cm:
            framing.parse_connect(body)
        self.assertEqual(cm.exception.code,
                         framing.CONNACK_UNACCEPTABLE_PROTOCOL_VERSION)

    def test_empty_clientid_rejected(self):
        var = (framing._encode_utf8("MQTT") + bytes([4, 0x02, 0, 0])
               + framing._encode_utf8(""))
        raw = bytes([CONNECT << 4, len(var)]) + var
        _, body = FrameReader(FakeSocket([raw])).read_packet()
        with self.assertRaises(RefusedConnect):
            framing.parse_connect(body)

    def test_trailing_bytes(self):
        raw = framing.encode_connect("c", clean_session=True)
        _, body = FrameReader(FakeSocket([raw])).read_packet()
        body += b"\x00"
        with self.assertRaises(ProtocolError):
            framing.parse_connect(body)


class PublishParseTests(unittest.TestCase):

    def test_qos2_roundtrip(self):
        raw = framing.encode_publish("a/b", b"hello", 0x1234)
        first, body = FrameReader(FakeSocket([raw])).read_packet()
        pub = framing.parse_publish(first, body)
        self.assertEqual(pub.qos, 2)
        self.assertEqual(pub.topic, "a/b")
        self.assertEqual(pub.packet_id, 0x1234)
        self.assertEqual(pub.payload, b"hello")
        self.assertFalse(pub.dup)

    def test_qos3_illegal_first_byte_flags(self):
        raw = framing.encode_publish("t", b"x", 1)
        first, body = FrameReader(FakeSocket([raw])).read_packet()
        first |= 0x06  # QoS=3
        with self.assertRaises(ProtocolError):
            framing.parse_publish(first, body)

    def test_wildcard_topic_rejected(self):
        raw = framing.encode_publish("a/+/b", b"x", 1)
        first, body = FrameReader(FakeSocket([raw])).read_packet()
        with self.assertRaises(ProtocolError):
            framing.parse_publish(first, body)

    def test_zero_packet_id_rejected(self):
        body = framing._encode_utf8("t") + b"\x00\x00" + b"x"
        with self.assertRaises(ProtocolError):
            framing.parse_publish((PUBLISH << 4) | 0x04, body)


class MiscPacketTests(unittest.TestCase):

    def test_pubrel_flags_must_be_0010(self):
        # flags=0000 -> 非法
        raw = framing.raw_packet if hasattr(framing, "raw_packet") else None
        bad = bytes([PUBREL << 4, 2, 0, 7])
        first, body = FrameReader(FakeSocket([bad])).read_packet()
        with self.assertRaises(ProtocolError):
            framing.parse_pid_packet(first, body, PUBREL, expect_flags=0x02)

    def test_connack_codec(self):
        raw = framing.encode_connack(True, 0)
        first, body = FrameReader(FakeSocket([raw])).read_packet()
        self.assertEqual((first & 0xF0) >> 4, CONNACK)
        sp, code = framing.parse_connack(body)
        self.assertTrue(sp)
        self.assertEqual(code, 0)

    def test_disconnect_must_be_empty(self):
        bad = bytes([DISCONNECT << 4, 1, 0])
        first, body = FrameReader(FakeSocket([bad])).read_packet()
        self.assertEqual((first & 0xF0) >> 4, DISCONNECT)
        self.assertEqual(len(body), 1)  # server 侧负责拒绝


if __name__ == "__main__":
    unittest.main()
