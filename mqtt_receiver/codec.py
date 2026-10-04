"""手写的 MQTT 3.1.1 报文编解码与流式分包。

不依赖任何 MQTT broker/客户端库，只实现入站接收器需要的部分：
CONNECT / CONNACK / PUBLISH(QoS2) / PUBREC / PUBREL / PUBCOMP /
PINGREQ / PINGRESP / DISCONNECT。

设计约束（与需求对应）：
- 不支持遗嘱（Will Flag 必须为 0）、不支持保留消息（RETAIN 必须为 0）、
  不做订阅与转发、QoS 仅允许 2；
- 单条 PUBLISH 载荷上限 4 KiB；
- Remaining Length 允许跨任意读取边界到达，一个 read 也允许携带多个报文
  （多报文粘连）。
"""

from __future__ import annotations

from dataclasses import dataclass

# ---------------------------------------------------------------------------
# 报文类型
# ---------------------------------------------------------------------------

CONNECT = 0x1
CONNACK = 0x2
PUBLISH = 0x3
PUBACK = 0x4
PUBREC = 0x5
PUBREL = 0x6
PUBCOMP = 0x7
SUBSCRIBE = 0x8
SUBACK = 0x9
UNSUBSCRIBE = 0xA
UNSUBACK = 0xB
PINGREQ = 0xC
PINGRESP = 0xD
DISCONNECT = 0xE

# CONNACK 返回码
CONNACK_ACCEPTED = 0
CONNACK_UNACCEPTABLE_PROTOCOL = 1
CONNACK_IDENTIFIER_REJECTED = 2
CONNACK_MALFORMED = 5

# 接收器自身限制
MAX_PAYLOAD = 4 * 1024  # 单条 PUBLISH 载荷 4 KiB
MAX_REMAINING = 128 * 1024  # 其它报文 Remaining Length 上限，防恶意超大报文
MAX_CLIENT_ID = 23  # MQTT 3.1.1 允许服务端自行限制；空 ClientId 另行处理


class ProtocolError(Exception):
    """违反协议且无法回 CONNACK 的错误：收到方必须关闭连接。"""


class ConnectReject(Exception):
    """CONNECT 语义上被拒绝：先回 CONNACK(return_code) 再关闭连接。"""

    def __init__(self, return_code: int):
        super().__init__(f"CONNECT rejected: {return_code}")
        self.return_code = return_code


# ---------------------------------------------------------------------------
# 报文数据模型
# ---------------------------------------------------------------------------


@dataclass
class Connect:
    client_id: str
    clean_session: bool
    keep_alive: int
    has_username: bool = False
    username: bytes | None = None
    has_password: bool = False
    password: bytes | None = None


@dataclass
class Connack:
    session_present: bool
    return_code: int


@dataclass
class Publish:
    dup: bool
    qos: int
    retain: bool
    topic: str
    packet_id: int
    payload: bytes


@dataclass
class PacketIdOnly:
    """PUBREC / PUBREL / PUBCOMP 的公共结构。"""

    packet_id: int
    kind: int = 0  # 控制报文类型（PUBREL/PUBREC/PUBCOMP），用于运行时分发


# ---------------------------------------------------------------------------
# 基础读写原语
# ---------------------------------------------------------------------------


def encode_remaining_length(length: int) -> bytes:
    out = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 0x80
        out.append(byte)
        if length == 0:
            return bytes(out)


def _read_u16(buf: bytes, offset: int) -> tuple[int, int]:
    if offset + 2 > len(buf):
        raise ProtocolError("报文被截断：缺少 2 字节整数")
    return (buf[offset] << 8) | buf[offset + 1], offset + 2


def _read_utf8(buf: bytes, offset: int) -> tuple[str, int]:
    """读取 UTF-8 编码字符串（MQTT 风格：2 字节长度前缀）。

    严格按 3.1.1 规范校验 UTF-8；禁止 NUL 字符（通配符/长度由上层判断）。
    """
    length, offset = _read_u16(buf, offset)
    if offset + length > len(buf):
        raise ProtocolError("报文被截断：字符串体不足")
    raw = buf[offset : offset + length]
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        # MQTT 3.1.1 允许 CONNECT 中非法 UTF-8 以 CONNACK 5 拒绝；
        # 但此函数也用于 PUBLISH 主题，调用方负责区分，统一抛 ProtocolError，
        # CONNECT 解析处会捕获转换。
        raise ProtocolError("非法 UTF-8 字符串")
    if "\u0000" in text:
        raise ProtocolError("MQTT 字符串禁止包含 NUL 字符")
    return text, offset + length


def _read_binary(buf: bytes, offset: int) -> tuple[bytes, int]:
    """读取二进制串（2 字节长度前缀，用于密码）。"""
    length, offset = _read_u16(buf, offset)
    if offset + length > len(buf):
        raise ProtocolError("报文被截断：二进制串体不足")
    return bytes(buf[offset : offset + length]), offset + length


# ---------------------------------------------------------------------------
# 各类报文解析
# ---------------------------------------------------------------------------


def _parse_connect(remaining: bytes) -> Connect:
    off = 0
    try:
        protocol_name, off = _read_utf8(remaining, off)
    except ProtocolError:
        raise ProtocolError("CONNECT 中协议名字段非法")

    if protocol_name != "MQTT":
        # MQTT 3.1 使用 "MQIsdp"（level 3），本接收器只实现 3.1.1。
        raise ConnectReject(CONNACK_UNACCEPTABLE_PROTOCOL)
    if off + 1 > len(remaining):
        raise ProtocolError("CONNECT 被截断：缺少协议级别")
    level = remaining[off]
    off += 1
    if level != 4:
        raise ConnectReject(CONNACK_UNACCEPTABLE_PROTOCOL)

    if off + 1 > len(remaining):
        raise ProtocolError("CONNECT 被截断：缺少 Connect Flags")
    flags = remaining[off]
    off += 1

    # bit0 保留位必须为 0
    if flags & 0x01:
        raise ProtocolError("CONNECT Connect Flags 保留位置 1")
    clean_session = bool(flags & 0x02)
    will_flag = bool(flags & 0x04)
    will_qos = (flags >> 3) & 0x03
    will_retain = bool(flags & 0x20)
    has_password = bool(flags & 0x40)
    has_username = bool(flags & 0x80)

    # 本接收器明确不支持遗嘱
    if will_flag:
        raise ProtocolError("本接收器不支持遗嘱（Will）")
    if will_qos != 0 or will_retain:
        raise ProtocolError("Will 相关标志与 Will Flag=0 矛盾")
    # 用户名/密码位不能脱离载荷存在，载荷长度后面统一校验；
    # 密码位为 1 而用户名位为 0 本身合法，不做额外限制。

    keep_alive, off = _read_u16(remaining, off)

    try:
        client_id, off = _read_utf8(remaining, off)
    except ProtocolError:
        raise ConnectReject(CONNACK_MALFORMED)

    if client_id == "":
        # 空 ClientId 只有 CleanSession=1 才合法
        if not clean_session:
            raise ConnectReject(CONNACK_IDENTIFIER_REJECTED)
    elif len(client_id.encode("utf-8")) > MAX_CLIENT_ID:
        raise ConnectReject(CONNACK_IDENTIFIER_REJECTED)

    # 存在 Will 时这里应读 Will Topic/Message，但 Will 已被拒绝，无需处理。
    username: bytes | None = None
    password: bytes | None = None
    try:
        if has_username:
            username, off = _read_binary(remaining, off)
        if has_password:
            password, off = _read_binary(remaining, off)
    except ProtocolError:
        raise ProtocolError("CONNECT 被截断：用户名/密码字段不足")

    if off != len(remaining):
        raise ProtocolError("CONNECT 存在多余的尾部字节")

    return Connect(
        client_id=client_id,
        clean_session=clean_session,
        keep_alive=keep_alive,
        has_username=has_username,
        username=username,
        has_password=has_password,
        password=password,
    )


def _parse_connack(flags: int, remaining: bytes) -> Connack:
    if flags != 0x0 or len(remaining) != 2:
        raise ProtocolError("CONNACK 非法")
    sp = remaining[0]
    rc = remaining[1]
    if sp not in (0, 1):
        raise ProtocolError("CONNACK Session Present 标志非法")
    if sp == 1 and rc != 0:
        raise ProtocolError("CONNACK 非零返回码时 Session Present 必须为 0")
    return Connack(session_present=bool(sp), return_code=rc)


def _parse_publish(first_byte: int, remaining: bytes) -> Publish:
    dup = bool(first_byte & 0x08)
    qos = (first_byte >> 1) & 0x03
    retain = bool(first_byte & 0x01)

    if qos != 2:
        # 接收器只交付 QoS2 消息；QoS0/1/3 一律视为非法报文并断连。
        raise ProtocolError("仅接受 QoS 2 的 PUBLISH")
    if retain:
        # 明确不支持保留消息
        raise ProtocolError("本接收器不支持保留消息")

    off = 0
    try:
        topic, off = _read_utf8(remaining, off)
    except ProtocolError:
        raise ProtocolError("PUBLISH 主题非法")
    if topic == "":
        raise ProtocolError("PUBLISH 主题不能为空")
    if "+" in topic or "#" in topic:
        raise ProtocolError("PUBLISH 主题不能包含通配符")

    # QoS2 必须带 Packet Identifier
    packet_id, off = _read_u16(remaining, off)
    if packet_id == 0:
        raise ProtocolError("Packet Identifier 不能为 0")

    payload = bytes(remaining[off:])
    if len(payload) > MAX_PAYLOAD:
        raise ProtocolError("载荷超过 4 KiB 上限")
    return Publish(
        dup=dup,
        qos=qos,
        retain=retain,
        topic=topic,
        packet_id=packet_id,
        payload=payload,
    )


def _parse_packet_id_only(
    kind: str, first_byte: int, remaining: bytes, packet_type: int
) -> PacketIdOnly:
    if len(remaining) != 2:
        raise ProtocolError(f"{kind} 长度必须为 2")
    packet_id = (remaining[0] << 8) | remaining[1]
    if packet_id == 0:
        raise ProtocolError(f"{kind} 的 Packet Identifier 不能为 0")
    return PacketIdOnly(packet_id, packet_type)


def parse_packet(first_byte: int, remaining: bytes):
    """按固定首字节分发解析一个完整报文。"""
    packet_type = first_byte >> 4
    flags = first_byte & 0x0F

    if packet_type == CONNECT:
        if flags != 0x0:
            raise ProtocolError("CONNECT 固定头保留标志非法")
        return _parse_connect(remaining)

    if packet_type == CONNACK:
        return _parse_connack(flags, remaining)

    if packet_type == PUBLISH:
        # DUP(bit3) 任意；QoS 必须 2（0b10）；RETAIN 必须 0
        # => 合法首字节低 4 位为 0b0100 或 0b1100
        if flags not in (0x4, 0xC):
            raise ProtocolError("PUBLISH 固定头标志非法（仅 QoS2、无 RETAIN）")
        return _parse_publish(first_byte, remaining)

    if packet_type == PUBREL:
        # PUBREL 固定头低 4 位必须为 0b0010
        if flags != 0x2:
            raise ProtocolError("PUBREL 固定头保留标志非法")
        return _parse_packet_id_only("PUBREL", first_byte, remaining, PUBREL)

    if packet_type == PUBREC:
        if flags != 0x0:
            raise ProtocolError("PUBREC 固定头保留标志非法")
        return _parse_packet_id_only("PUBREC", first_byte, remaining, PUBREC)

    if packet_type == PUBCOMP:
        if flags != 0x0:
            raise ProtocolError("PUBCOMP 固定头保留标志非法")
        return _parse_packet_id_only("PUBCOMP", first_byte, remaining, PUBCOMP)

    if packet_type == PINGREQ:
        if flags != 0x0 or len(remaining) != 0:
            raise ProtocolError("PINGREQ 非法")
        return PINGREQ

    if packet_type == PINGRESP:
        if flags != 0x0 or len(remaining) != 0:
            raise ProtocolError("PINGRESP 非法")
        return PINGRESP

    if packet_type == DISCONNECT:
        if flags != 0x0 or len(remaining) != 0:
            raise ProtocolError("DISCONNECT 非法")
        return DISCONNECT

    if packet_type in (SUBSCRIBE, UNSUBACK, SUBACK, UNSUBSCRIBE, PUBACK, CONNACK):
        # 入站接收器不做订阅；这些报文在当前角色下没有合法位置。
        raise ProtocolError(f"不支持的报文类型: {packet_type:#x}")

    raise ProtocolError(f"保留/未知报文类型: {packet_type:#x}")


# ---------------------------------------------------------------------------
# 出站编码
# ---------------------------------------------------------------------------


def _fixed_header(packet_type: int, flags: int, remaining_length: int) -> bytes:
    return bytes([(packet_type << 4) | flags]) + encode_remaining_length(
        remaining_length
    )


def encode_connack(session_present: bool, return_code: int) -> bytes:
    sp = 1 if session_present else 0
    return _fixed_header(CONNACK, 0x0, 2) + bytes([sp, return_code])


def encode_pubrec(packet_id: int) -> bytes:
    return _fixed_header(PUBREC, 0x0, 2) + packet_id.to_bytes(2, "big")


def encode_pubrel(packet_id: int) -> bytes:
    # 服务端不会主动发 PUBREL，但测试/完整性需要时可用。
    return _fixed_header(PUBREL, 0x2, 2) + packet_id.to_bytes(2, "big")


def encode_pubcomp(packet_id: int) -> bytes:
    return _fixed_header(PUBCOMP, 0x0, 2) + packet_id.to_bytes(2, "big")


def encode_pingresp() -> bytes:
    return _fixed_header(PINGRESP, 0x0, 0)


def encode_connect(
    client_id: str,
    clean_session: bool,
    keep_alive: int = 0,
    username: bytes | None = None,
    password: bytes | None = None,
) -> bytes:
    payload = b""
    cid = client_id.encode("utf-8")
    payload += len(cid).to_bytes(2, "big") + cid
    flags = 0
    if clean_session:
        flags |= 0x02
    if username is not None:
        flags |= 0x80
        payload += len(username).to_bytes(2, "big") + username
    if password is not None:
        flags |= 0x40
        payload += len(password).to_bytes(2, "big") + password

    var_header = b""
    name = b"MQTT"
    var_header += len(name).to_bytes(2, "big") + name
    var_header += bytes([4, flags])
    var_header += keep_alive.to_bytes(2, "big")

    body = var_header + payload
    return _fixed_header(CONNECT, 0x0, len(body)) + body


def encode_publish(
    topic: str,
    packet_id: int,
    payload: bytes,
    dup: bool = False,
    qos: int = 2,
    retain: bool = False,
) -> bytes:
    topic_b = topic.encode("utf-8")
    var_header = len(topic_b).to_bytes(2, "big") + topic_b
    if qos > 0:
        var_header += packet_id.to_bytes(2, "big")
    first = (PUBLISH << 4) | ((1 if dup else 0) << 3) | (qos << 1) | (
        1 if retain else 0
    )
    body = var_header + payload
    return bytes([first]) + encode_remaining_length(len(body)) + body


def encode_disconnect() -> bytes:
    return _fixed_header(DISCONNECT, 0x0, 0)


def encode_pingreq() -> bytes:
    return _fixed_header(PINGREQ, 0x0, 0)


# ---------------------------------------------------------------------------
# 流式分包缓冲
# ---------------------------------------------------------------------------


class PacketBuffer:
    """累积任意到达的 TCP 字节，按 MQTT 帧边界产出完整报文。

    - Remaining Length 跨多次 read：逐字节尝试推进；
    - 多个报文粘连在一次 read：循环产出，直到数据不足一个完整报文；
    - 非法帧（首字节、Remaining Length、长度上限）抛 ProtocolError。
    """

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes):
        self._buf.extend(data)
        packets = []
        while True:
            pkt = self._try_extract()
            if pkt is None:
                return packets
            packets.append(pkt)

    def _try_extract(self):
        if not self._buf:
            return None

        # 解析 Remaining Length，允许它和固定头跨读取边界。
        multiplier = 1
        value = 0
        idx = 1
        while True:
            if idx >= len(self._buf):
                return None  # Remaining Length 不完整，等更多数据
            byte = self._buf[idx]
            value += (byte & 0x7F) * multiplier
            idx += 1
            if not (byte & 0x80):
                break
            if idx > 4:
                raise ProtocolError("Remaining Length 超过 4 字节")
            multiplier *= 128

        if value > MAX_REMAINING:
            raise ProtocolError("Remaining Length 超过接收器上限")

        first_byte = self._buf[0]
        total = idx + value
        if len(self._buf) < total:
            return None  # 报文体还没到齐

        remaining = bytes(self._buf[idx:total])
        del self._buf[:total]
        return parse_packet(first_byte, remaining)
