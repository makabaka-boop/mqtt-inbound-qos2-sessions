"""TCP 入口：MQTT 3.1.1 入站接收器（不是消息代理）。

每个 TCP 连接一个线程；会话状态全部以 SQLite 为准，内存中只保留
"当前活动连接" 的注册表，用于同名连接接管。

并发模型
========
- 每个 ClientId 一把可重入锁（会话锁）。所有会改变会话/交换状态的动作
  （begin_connect、PUBLISH 落库、PUBREL 入账、断连清理、新连接接管）都在
  会话锁内串行化，因此新连接完成接管后，旧连接不可能再改动会话。
- Store 自身另有一把进程内写锁，保证单连接 SQLite 连接的多线程使用安全。
- 网络发送在会话锁之外进行，但发送前校验 "本连接仍是活动连接"；接管时
  shutdown 旧 socket，使旧线程在 recv/send 上立即解除阻塞并退出。
"""

from __future__ import annotations

import argparse
import logging
import socket
import threading
import uuid

from . import codec
from .codec import (
    CONNACK_ACCEPTED,
    DISCONNECT,
    PINGREQ,
    PUBREL,
    Connect,
    ConnectReject,
    PacketBuffer,
    PacketIdOnly,
    ProtocolError,
    Publish,
)
from .store import Store

log = logging.getLogger("mqtt_receiver")

CONNECT_GRACE_TIMEOUT = 10.0  # 建立 TCP 后等待 CONNECT 的宽限时间（秒）


class ConnectionTaken(Exception):
    """本连接已被同名新连接接管，处理循环应立即停止。"""


class Registry:
    """ClientId -> 当前活动连接；并持有每 ClientId 的会话锁。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: dict[str, "ClientConn"] = {}
        self._session_locks: dict[str, threading.RLock] = {}

    def session_lock(self, client_id: str) -> threading.RLock:
        with self._lock:
            lk = self._session_locks.get(client_id)
            if lk is None:
                lk = threading.RLock()
                self._session_locks[client_id] = lk
            return lk

    def attach(self, conn: "ClientConn") -> None:
        """登记新连接并接管旧连接。调用方必须持有该 client 的会话锁。"""
        assert conn.internal_cid is not None
        with self._lock:
            old = self._active.get(conn.internal_cid)
            self._active[conn.internal_cid] = conn
        if old is not None and old is not conn:
            old.mark_taken_and_shutdown()

    def detach(self, conn: "ClientConn") -> None:
        assert conn.internal_cid is not None
        with self._lock:
            if self._active.get(conn.internal_cid) is conn:
                del self._active[conn.internal_cid]

    def is_owner(self, conn: "ClientConn") -> bool:
        with self._lock:
            return self._active.get(conn.internal_cid) is conn


class ClientConn:
    def __init__(
        self,
        sock: socket.socket,
        peer,
        store: Store,
        registry: Registry,
    ) -> None:
        self.sock = sock
        self.peer = peer
        self.store = store
        self.registry = registry

        self.client_id: str | None = None  # 外部 ClientId（空 ClientId 时为 ""）
        self.internal_cid: str | None = None  # 会话/注册表内部键
        self.clean_session: bool = False
        self.connected = False  # 是否已完成 CONNECT/CONNACK
        self.graceful_disconnect = False  # 对端主动 DISCONNECT
        self.anonymous = False

        self._active = True  # 未被接管且未关闭
        self._send_lock = threading.Lock()

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def mark_taken_and_shutdown(self) -> None:
        with self._send_lock:
            self._active = False
        try:
            self.sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def _close(self) -> None:
        with self._send_lock:
            self._active = False
        try:
            self.sock.close()
        except OSError:
            pass

    def try_send(self, data: bytes) -> bool:
        """仅当本连接仍是活动连接时发送。被接管则静默丢弃。"""
        with self._send_lock:
            if not self._active:
                return False
            try:
                self.sock.sendall(data)
                return True
            except OSError:
                self._active = False
                return False

    # ------------------------------------------------------------------
    # 主处理循环
    # ------------------------------------------------------------------

    def run(self) -> None:
        buffer = PacketBuffer()
        rejected: ConnectReject | None = None
        try:
            self.sock.settimeout(CONNECT_GRACE_TIMEOUT)

            # 整个连接只用一个报文迭代器：CONNECT 之前那次 feed 若已粘连了
            # 后续报文（CONNECT+PUBLISH+PUBREL 同段到达），它们会留在迭代器
            # 中继续被服务循环消费，而不是被丢弃。
            packet_iter = self._iter_packets(buffer)

            connect = self._read_connect(packet_iter)
            self._handle_connect(connect)

            self._serve_loop(packet_iter)
        except ConnectReject as exc:
            # 可在 CONNACK 中表达的拒绝：先回 CONNACK(rc, SP=0) 再关连接。
            rejected = exc
        except ConnectionTaken:
            pass
        except ProtocolError:
            pass  # 无法在 CONNACK 表达的违例：直接关闭连接
        except (socket.timeout, TimeoutError):
            log.info("client=%s 超时关闭", self.client_id)
        except EOFError:
            log.info("client=%s 对端关闭 TCP", self.client_id)
        except OSError:
            pass  # RST / broken pipe / 对端关闭
        finally:
            if rejected is not None:
                self.try_send(
                    codec.encode_connack(False, rejected.return_code)
                )
            self._cleanup()

    def _iter_packets(self, buffer: PacketBuffer):
        """报文生成器：跨 read 产出完整报文，处理拆包与粘连。"""
        while True:
            try:
                data = self.sock.recv(4096)
            except (socket.timeout, TimeoutError):
                raise
            except OSError:
                if not self._active:
                    raise ConnectionTaken()
                raise
            if data == b"":
                raise EOFError("对端关闭 TCP")
            for pkt in buffer.feed(data):
                yield pkt

    def _read_connect(self, packet_iter) -> Connect:
        for pkt in packet_iter:
            if not isinstance(pkt, Connect):
                # CONNECT 之前任何报文都是协议错误，直接关连接
                raise ProtocolError("首个报文不是 CONNECT")
            return pkt
        raise ProtocolError("未收到 CONNECT")

    def _handle_connect(self, connect: Connect) -> None:
        if connect.client_id == "":
            # 空 ClientId 仅 CleanSession=1 时合法（codec 已强制）。
            # 协议（§3.1.3-6）要求服务端为其分配唯一标识，避免多个匿名
            # 连接在活动连接注册表中相互 "接管"。
            self.internal_cid = "anon-" + uuid.uuid4().hex
            self.anonymous = True
        else:
            self.internal_cid = connect.client_id
            self.anonymous = False

        cid = self.internal_cid
        self.client_id = connect.client_id  # 对外展示用（可能为空）
        self.clean_session = connect.clean_session
        sess_lock = self.registry.session_lock(cid)

        with sess_lock:
            # 先接管旧连接：旧连接的在途临界区在本锁上排队，
            # 锁被本线程拿到意味着旧线程此后不可能再进入会话修改流程。
            self.registry.attach(self)

            present = self.store.begin_connect(cid, connect.clean_session)

            connack = codec.encode_connack(present, CONNACK_ACCEPTED)
            if not self.try_send(connack):
                raise ConnectionTaken()

        self.connected = True

        # Keep Alive：3.1.1 §3.1.3，服务端 1.5 倍超时是常见宽容实现
        if connect.keep_alive > 0:
            self.sock.settimeout(max(connect.keep_alive * 1.5, 1.0))
        else:
            self.sock.settimeout(None)

        log.info(
            "CONNECT client=%r clean=%d present=%d keepalive=%d",
            connect.client_id,
            int(connect.clean_session),
            int(present),
            connect.keep_alive,
        )

    def _serve_loop(self, packet_iter) -> None:
        for pkt in packet_iter:
            if not self._active:
                raise ConnectionTaken()

            if isinstance(pkt, Connect):
                # 同一 TCP 连接不允许第二个 CONNECT
                raise ProtocolError("连接已建立后再次收到 CONNECT")

            if isinstance(pkt, Publish):
                self._handle_publish(pkt)
            elif isinstance(pkt, PacketIdOnly):
                if pkt.kind == PUBREL:
                    self._handle_pubrel(pkt.packet_id)
                else:
                    # 入站接收器从不发送 QoS2 消息，因此客户端方向的
                    # PUBREC/PUBCOMP 没有合法位置，按非法报文处理。
                    raise ProtocolError(
                        f"该角色下不允许的报文类型: {pkt.kind:#x}"
                    )
            elif pkt is PINGREQ:
                if not self.try_send(codec.encode_pingresp()):
                    raise ConnectionTaken()
            elif pkt is codec.PINGRESP:
                raise ProtocolError("客户端不得发送 PINGRESP")
            elif pkt is DISCONNECT:
                log.info("DISCONNECT client=%r", self.client_id)
                self.graceful_disconnect = True
                return
            else:
                # 入站接收器不发送 PUBLISH/PUBREL，因此客户端来的
                # PUBREC/PUBCOMP/PUBACK 方向非法；订阅类报文同样非法。
                raise ProtocolError(f"该角色下不允许的报文: {pkt!r}")

    # ------------------------------------------------------------------
    # QoS 2 接收方状态机
    # ------------------------------------------------------------------

    def _handle_publish(self, publish: Publish) -> None:
        assert self.internal_cid is not None
        cid = self.internal_cid
        pid = publish.packet_id
        sess_lock = self.registry.session_lock(cid)

        with sess_lock:
            if not self.registry.is_owner(self):
                raise ConnectionTaken()
            action = self.store.persist_publish(
                cid,
                pid,
                publish.topic,
                publish.payload,
                publish.dup,
                inbox_client_id=self.client_id or "",
            )

        # PUBREC 发送允许晚于持久化（崩溃后客户端重传即可），
        # 但绝不允许早于持久化——persist_publish 已在锁内完成提交。
        if action == "resend_comp":
            response = codec.encode_pubcomp(pid)
            log.info(
                "PUBLISH(dup) client=%r pid=%d 命中已完成交换，重发 PUBCOMP",
                cid,
                pid,
            )
        else:
            response = codec.encode_pubrec(pid)
            log.info(
                "PUBLISH client=%r pid=%d topic=%r action=%s -> PUBREC",
                cid,
                pid,
                publish.topic,
                action,
            )

        if not self.try_send(response):
            raise ConnectionTaken()

    def _handle_pubrel(self, packet_id: int) -> None:
        assert self.internal_cid is not None
        cid = self.internal_cid
        sess_lock = self.registry.session_lock(cid)

        with sess_lock:
            if not self.registry.is_owner(self):
                raise ConnectionTaken()
            result = self.store.complete_pubrel(cid, packet_id)

        # 收件账写入 + 状态结束已在同一事务提交，此刻才能发 PUBCOMP。
        # already_rel / unknown 都只重发 PUBCOMP，绝不重复入账。
        if not self.try_send(codec.encode_pubcomp(packet_id)):
            raise ConnectionTaken()
        log.info(
            "PUBREL client=%r pid=%d result=%s -> PUBCOMP",
            cid,
            packet_id,
            result,
        )

    # ------------------------------------------------------------------
    # 断连清理
    # ------------------------------------------------------------------

    def _cleanup(self) -> None:
        if not self.connected or self.internal_cid is None:
            self._close()
            return

        cid = self.internal_cid
        sess_lock = self.registry.session_lock(cid)
        with sess_lock:
            owner = self.registry.is_owner(self)
            if owner:
                if self.clean_session:
                    # CleanSession=1：连接结束即清除本会话的协议交换状态。
                    self.store.cleanup_ephemeral(cid)
                else:
                    self.store.touch_session(cid)
                self.registry.detach(self)
            # 已被接管：不得删除任何状态，新连接是会话的新主人。
        self._close()
        log.info("连接关闭 client=%r taken_over=%s", cid, not owner)


# ---------------------------------------------------------------------------
# TCP 服务器
# ---------------------------------------------------------------------------


class TcpServer:
    def __init__(self, store: Store, host: str = "127.0.0.1", port: int = 0):
        self.store = store
        self.registry = Registry()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((host, port))
        self._sock.listen(128)
        self._stop = threading.Event()

    @property
    def address(self):
        return self._sock.getsockname()

    def shutdown(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass

    def serve_forever(self) -> None:
        log.info("MQTT 接收器监听 %s", self.address)
        self._sock.settimeout(0.5)
        while not self._stop.is_set():
            try:
                client_sock, peer = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            client_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            conn = ClientConn(client_sock, peer, self.store, self.registry)
            t = threading.Thread(
                target=conn.run, name=f"mqtt-{peer[0]}:{peer[1]}", daemon=True
            )
            t.start()
        log.info("接收器停止接受新连接")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="MQTT 3.1.1 QoS2 入站接收器")
    parser.add_argument("--db", required=True, help="SQLite 数据库路径")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=1883)
    parser.add_argument(
        "--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING")
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = Store(args.db)
    server = TcpServer(store, host=args.host, port=args.port)

    import signal

    def _stop(signum, frame):
        log.info("收到信号 %s，开始停止", signum)
        server.shutdown()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    try:
        server.serve_forever()
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
