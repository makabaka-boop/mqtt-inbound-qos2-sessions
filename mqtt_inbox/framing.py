"""MQTT 3.1.1 报文帧读写与编解码（手工实现，无第三方库）。

覆盖：
- Remaining Length 的变长解码（1~4 字节，0x80 续位），跨 recv 边界读取；
- CONNECT / CONNACK / PUBLISH(QoS2) / PUBREC / PUBREL / PUBCOMP /
  PINGREQ / PINGRESP / DISCONNECT 的解析与编码；
- 多报文粘连由 :class:`FrameReader` 的内部缓冲自然切分。

任何违反协议的结构都抛 :class:`ProtocolError`，调用方负责关闭连接。
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

CONNECT = 1
CONNACK = 2
PUBLISH = 3
PUBACK = 4
PUBREC = 5
PUBREL = 6
PUBCOMP = 7
SUBSCRIBE = 8
SUBACK = 9
UNSUBSCRIBE = 10
UNSUBACK = 11
PINGREQ = 12
PINGRESP = 13
DISCONNECT = 14

# CONNACK 返回码
CONNACK_ACCEPTED = 0x00
CONNACK_UNACCEPTABLE_PROTOCOL_VERSION = 0x01
CONNACK_IDENTIFIER_REJECTED = 0x02
CONNACK_SERVER_UNAVAILABLE = 0x03
CONNACK_BAD_USERNAME_OR_PASSWORD = 0x04
CONNACK_NOT_AUTHORIZED = 0x05

# 单条应用载荷上限 4 KiB；剩余长度硬上限留出变长头/ClientId/Topic 的余量。
MAX_PAYLOAD = 4 * 1024
MAX_REMAINING_LENGTH = MAX_PAYLOAD + 512

PROTOCOL_NAME = "MQTT"
PROTOCOL_LEVEL = 0x04


class ProtocolError(Exception):
    """报文违反 MQTT 3.1.1 语法/语义约束，接收方必须关闭连接。"""


class ConnectionClosed(Exception):
    """对端正常关闭（EOF）或连接已不可用。"""


class RefusedConnect(Exception):
    """CONNECT 可被 CONNACK 非零返回码拒绝，随后关闭连接。"""

    def __init__(self, code: int):
        super().__init__(f"CONNECT refused code={code:#x}")
        self.code = code


# ---------------------------------------------------------------------------
# Remaining Length
# ---------------------------------------------------------------------------

def encode_remaining_length(length: int) -> bytes:
    """按 MQTT 3.1.1 可变字节整数编码 Remaining Length。"""
    if length < 0:
        raise ValueError("negative remaining length")
    out = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        out.append(byte)
        if length == 0:
            return bytes(out)


def _iter_remaining_length_bytes(value: int):
    """编码后的逐字节（供测试/对端使用）。"""
    return list(encode_remaining_length(value))


# ---------------------------------------------------------------------------
# 底层带缓冲帧读取器：处理拆包与粘连
# ---------------------------------------------------------------------------

class FrameReader:
    """在一条 TCP 连接上读取完整 MQTT 控制报文。

    - 内部维护接收缓冲，一次 recv 取回多个报文时，后续报文留在缓冲里（粘连）；
    - Remaining Length 与报文体都按“读满所需字节数”工作（拆包）；
    - 超时时直接把 socket.timeout 抛给上层（keepalive 判定）。
    """

    def __init__(self, sock):
        self._sock = sock
        self._buf = bytearray()

    def _read_exact(self, n: int) -> bytes:
        while len(self._buf) < n:
            chunk = self._sock.recv(4096)
            if not chunk:
                if self._buf:
                    raise ConnectionClosed("EOF in the middle of a packet")
                raise ConnectionClosed("EOF")
            self._buf.extend(chunk)
        data = bytes(self._buf[:n])
        del self._buf[:n]
        return data

    def read_packet(self) -> tuple[int, bytes]:
        """返回 (固定报头首字节, 剩余部分即报文体)。阻塞直到读完整帧。"""
        first = self._read_exact(1)[0]

        # Remaining Length：最多 4 字节，每字节低 7 位有效，最高位为续位。
        remaining = 0
        multiplier = 1
        for i in range(4):
            byte = self._read_exact(1)[0]
            remaining += (byte & 0x7F) * multiplier
            if byte & 0x80 == 0:
                break
            multiplier *= 128
        else:  # 第 4 个字节仍带续位 -> 非法
            raise ProtocolError("remaining length too long (>4 bytes)")

        if remaining > MAX_REMAINING_LENGTH:
            raise ProtocolError(f"remaining length {remaining} exceeds limit")

        body = self._read_exact(remaining) if remaining else b""
        return first, body


# ---------------------------------------------------------------------------
# 基础解码工具
# ---------------------------------------------------------------------------

def _u16(data: bytes, offset: int) -> int:
    if offset + 2 > len(data):
        raise ProtocolError("truncated u16")
    return (data[offset] << 8) | data[offset + 1]


def _decode_utf8(data: bytes, offset: int, field: str) -> tuple[str, int]:
    """读取长度前缀 UTF-8 字符串；MQTT 1.5.3 禁止 U+0000。"""
    length = _u16(data, offset)
    offset += 2
    end = offset + length
    if end > len(data):
        raise ProtocolError(f"truncated UTF-8 field: {field}")
    raw = data[offset:end]
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ProtocolError(f"invalid UTF-8 in {field}: {exc}") from None
    if "\x00" in text:
        raise ProtocolError(f"U+0000 is forbidden in {field}")
    return text, end


def _encode_utf8(text: str) -> bytes:
    if "\x00" in text:
        raise ProtocolError("U+0000 is forbidden in MQTT strings")
    raw = text.encode("utf-8")
    if len(raw) > 0xFFFF:
        raise ProtocolError("string too long")
    return len(raw).to_bytes(2, "big") + raw


# ---------------------------------------------------------------------------
# CONNECT
# ---------------------------------------------------------------------------

class ConnectInfo:
    def __init__(self, client_id, clean_session, keepalive,
                 username=None, password=None):
        self.client_id = client_id
        self.clean_session = clean_session
        self.keepalive = keepalive
        self.username = username
        self.password = password


def parse_connect(body: bytes) -> ConnectInfo:
    off = 0
    try:
        name, off = _decode_utf8(body, off, "Protocol Name")
    except ProtocolError:
        raise
    if name != PROTOCOL_NAME:
        # MQTT 3.1.2-1：协议名不符可以 CONNACK 0x01 后关连接。
        raise RefusedConnect(CONNACK_UNACCEPTABLE_PROTOCOL_VERSION)
    if off + 1 > len(body):
        raise ProtocolError("CONNECT missing protocol level")
    level = body[off]
    off += 1
    if level != PROTOCOL_LEVEL:
        raise RefusedConnect(CONNACK_UNACCEPTABLE_PROTOCOL_VERSION)

    if off + 3 > len(body):
        raise ProtocolError("CONNECT missing flags/keepalive")
    flags = body[off]
    keepalive = (body[off + 1] << 8) | body[off + 2]
    off += 3

    user_flag = bool(flags & 0x80)
    password_flag = bool(flags & 0x40)
    will_retain = bool(flags & 0x20)
    will_qos = (flags >> 3) & 0x03
    will_flag = bool(flags & 0x04)
    clean_session = bool(flags & 0x02)
    reserved = flags & 0x01

    if reserved:
        raise ProtocolError("CONNECT reserved flag bit must be 0")
    if password_flag and not user_flag:
        raise ProtocolError("password flag set without username flag")
    if will_qos == 3:
        raise ProtocolError("will QoS 3 is illegal")
    if not will_flag and (will_retain or will_qos != 0):
        raise ProtocolError("will retain/qos set while will flag is 0")

    client_id, off = _decode_utf8(body, off, "ClientId")
    if len(client_id) == 0:
        # 本服务不分配 ClientId：空 ClientId 一律拒绝（3.1.3-6 允许服务端拒绝）。
        raise RefusedConnect(CONNACK_IDENTIFIER_REJECTED)

    if will_flag:
        # 解析掉遗嘱字段以确认结构合法，但本接收器不支持遗嘱。
        _, off = _decode_utf8(body, off, "Will Topic")
        will_msg_len = _u16(body, off)
        off += 2 + will_msg_len
        if off > len(body):
            raise ProtocolError("truncated will message")
        raise RefusedConnect(CONNACK_SERVER_UNAVAILABLE)

    username = None
    password = None
    if user_flag:
        username, off = _decode_utf8(body, off, "Username")
    if password_flag:
        password_len = _u16(body, off)
        off += 2
        if off + password_len > len(body):
            raise ProtocolError("truncated password")
        password = body[off:off + password_len]
        off += password_len

    if off != len(body):
        raise ProtocolError("CONNECT has trailing bytes")

    return ConnectInfo(client_id, clean_session, keepalive, username, password)


def encode_connect(client_id: str, *, clean_session: bool, keepalive: int = 0,
                   username: str | None = None, password: bytes | None = None,
                   will_flag: bool = False, will_qos: int = 0,
                   will_retain: bool = False,
                   will_topic: str | None = None,
                   will_message: bytes = b"",
                   bad_password_without_username: bool = False) -> bytes:
    flags = (0x02 if clean_session else 0x00)
    if will_flag:
        flags |= 0x04 | ((will_qos & 3) << 3) | (0x20 if will_retain else 0)

    payload = bytearray()
    if bad_password_without_username:
        flags |= 0x40
    else:
        if username is not None:
            flags |= 0x80
        if password is not None:
            flags |= 0x40

    var = bytearray()
    var += _encode_utf8(PROTOCOL_NAME)
    var.append(PROTOCOL_LEVEL)
    var.append(flags)
    var += keepalive.to_bytes(2, "big")

    payload += _encode_utf8(client_id)
    if will_flag:
        payload += _encode_utf8(will_topic or "")
        payload += len(will_message).to_bytes(2, "big") + will_message
    if username is not None:
        payload += _encode_utf8(username)
    if password is not None:
        payload += len(password).to_bytes(2, "big") + password

    body = bytes(var) + bytes(payload)
    return bytes([CONNECT << 4]) + encode_remaining_length(len(body)) + body


# ---------------------------------------------------------------------------
# CONNACK
# ---------------------------------------------------------------------------

def encode_connack(session_present: bool, return_code: int) -> bytes:
    return bytes([
        CONNACK << 4, 0x02,
        0x01 if session_present else 0x00,
        return_code,
    ])


def parse_connack(body: bytes) -> tuple[bool, int]:
    if len(body) != 2:
        raise ProtocolError("CONNACK must be 2 bytes")
    if body[0] not in (0, 1):
        raise ProtocolError("bad CONNACK acknowledge flags")
    sp = bool(body[0] & 0x01)
    return sp, body[1]


# ---------------------------------------------------------------------------
# PUBLISH（本服务仅 QoS 2）
# ---------------------------------------------------------------------------

class PublishInfo:
    def __init__(self, dup, qos, retain, topic, packet_id, payload):
        self.dup = dup
        self.qos = qos
        self.retain = retain
        self.topic = topic
        self.packet_id = packet_id
        self.payload = payload


def parse_publish(first: int, body: bytes) -> PublishInfo:
    dup = (first >> 3) & 0x01
    qos = (first >> 1) & 0x03
    retain = first & 0x01
    if qos == 3:
        raise ProtocolError("PUBLISH QoS 3 is illegal")
    topic, off = _decode_utf8(body, 0, "Topic Name")
    if not topic:
        raise ProtocolError("PUBLISH topic must not be empty")
    if "+" in topic or "#" in topic:
        raise ProtocolError("PUBLISH topic name must not contain wildcards")

    packet_id = None
    if qos > 0:
        packet_id = _u16(body, off)
        off += 2
        if packet_id == 0:
            raise ProtocolError("packet identifier must not be zero")

    payload = body[off:]
    return PublishInfo(bool(dup), qos, bool(retain), topic, packet_id, payload)


def encode_publish(topic: str, payload: bytes, packet_id: int, *,
                   dup: bool = False, qos: int = 2, retain: bool = False) -> bytes:
    if qos == 2:
        first = (PUBLISH << 4) | (0x08 if dup else 0) | (2 << 1) | (0x01 if retain else 0)
    else:
        first = (PUBLISH << 4) | (0x08 if dup else 0) | (qos << 1) | (0x01 if retain else 0)
    var = _encode_utf8(topic)
    if qos > 0:
        var += packet_id.to_bytes(2, "big")
    body = var + payload
    return bytes([first]) + encode_remaining_length(len(body)) + body


# ---------------------------------------------------------------------------
# 带 Packet Identifier 的确认报文
# ---------------------------------------------------------------------------

def _encode_pid_packet(packet_type: int, flags: int, packet_id: int) -> bytes:
    if not 0 <= packet_id <= 0xFFFF:
        raise ValueError("packet id out of range")
    return bytes([(packet_type << 4) | flags, 0x02]) + packet_id.to_bytes(2, "big")


def encode_pubrec(packet_id: int) -> bytes:
    return _encode_pid_packet(PUBREC, 0x00, packet_id)


def encode_pubrel(packet_id: int) -> bytes:
    return _encode_pid_packet(PUBREL, 0x02, packet_id)


def encode_pubcomp(packet_id: int) -> bytes:
    return _encode_pid_packet(PUBCOMP, 0x00, packet_id)


def parse_pid_packet(first: int, body: bytes, expect_type: int,
                     expect_flags: int | None = None) -> int:
    ptype = (first & 0xF0) >> 4
    if ptype != expect_type:
        raise ProtocolError(f"expected packet type {expect_type}, got {ptype}")
    if expect_flags is not None and (first & 0x0F) != expect_flags:
        raise ProtocolError(f"packet type {expect_type} has illegal flags")
    if len(body) != 2:
        raise ProtocolError("packet identifier packet must have 2 byte body")
    pid = (body[0] << 8) | body[1]
    if pid == 0:
        raise ProtocolError("packet identifier must not be zero")
    return pid


def encode_empty(packet_type: int) -> bytes:
    return bytes([packet_type << 4, 0x00])


def encode_pingreq() -> bytes:
    return encode_empty(PINGREQ)


def encode_pingresp() -> bytes:
    return encode_empty(PINGRESP)


def encode_disconnect() -> bytes:
    return encode_empty(DISCONNECT)
