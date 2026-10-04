"""TCP 入口：手工驱动的 MQTT 3.1.1 入站接收状态机。

支持的控制报文：CONNECT、QoS 2 PUBLISH、PUBREL、PINGREQ、DISCONNECT；
服务端发送：CONNACK、PUBREC、PUBCOMP、PINGRESP。

不支持（收到即按非法报文关闭连接或在 CONNECT 阶段拒绝）：
遗嘱、Retained 消息、QoS 0/1、订阅、PUBACK/PUBREC 等客户端不该发的报文。
"""

from __future__ import annotations

import argparse
import socket
import threading
import time

from . import framing
from .framing import (
    CONNACK, CONNECT, DISCONNECT, PINGREQ, PUBACK, PUBCOMP, PUBLISH, PUBREC,
    PUBREL, SUBACK, SUBSCRIBE, UNSUBACK, UNSUBSCRIBE,
    CONNACK_ACCEPTED, CONNACK_SERVER_UNAVAILABLE,
)
from .storage import SessionTakenOver, Storage

CONNECT_WAIT_TIMEOUT = 10.0


class SessionRegistry:
    """ClientId -> 当前在线连接。同名新连接接管时强制 shutdown 旧 socket。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._live: dict[str, tuple[int, socket.socket]] = {}

    def take_over(self, client_id: str, epoch: int,
                  new_sock: socket.socket) -> None:
        with self._lock:
            old = self._live.get(client_id)
            self._live[client_id] = (epoch, new_sock)
        if old is not None:
            old_epoch, old_sock = old
            if old_epoch != epoch:
                try:
                    old_sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def remove(self, client_id: str, epoch: int) -> None:
        with self._lock:
            cur = self._live.get(client_id)
            if cur is not None and cur[0] == epoch:
                del self._live[client_id]


class ClientHandler:
    def __init__(self, sock: socket.socket, addr, db_path: str,
                 registry: SessionRegistry):
        self.sock = sock
        self.addr = addr
        self.db_path = db_path
        self.registry = registry
        self.reader = framing.FrameReader(sock)
        self.send_lock = threading.Lock()

        self.client_id: str | None = None
        self.epoch: int | None = None
        self.clean: bool = True
        self.established = False  # CONNECT 已成功并登记
        self.finished = False     # DISCONNECT 已正常收尾

    # -- 发送 ---------------------------------------------------------------

    def send_all(self, data: bytes) -> None:
        with self.send_lock:
            try:
                self.sock.sendall(data)
            except OSError:
                raise framing.ConnectionClosed("send failed")

    # -- 主流程 -------------------------------------------------------------

    def run(self) -> None:
        storage = Storage(self.db_path)
        try:
            self.sock.settimeout(CONNECT_WAIT_TIMEOUT)
            first, body = self._read_one()
            ptype = (first & 0xF0) >> 4
            if ptype != CONNECT:
                # 3.1.0-1：首报文必须是 CONNECT，否则关连接。
                raise framing.ProtocolError("first packet is not CONNECT")
            self._handle_connect(storage, body)

            while True:
                first, body = self._read_one()
                self._dispatch(storage, first, body)
        except framing.ProtocolError as exc:
            # 非法报文：直接关闭（不发送任何响应，MQTT 4.8 允许服务端关连接）。
            pass
        except framing.RefusedConnect:
            # CONNACK 已在 _handle_connect 内发出。
            pass
        except (framing.ConnectionClosed, OSError, socket.timeout,
                SessionTakenOver):
            pass
        finally:
            if self.established and not self.finished:
                try:
                    storage.mark_disconnected(
                        self.client_id, self.epoch, self.clean)
                except SessionTakenOver:
                    pass
                except Exception:
                    pass
                self.registry.remove(self.client_id, self.epoch)
            try:
                self.sock.close()
            except OSError:
                pass
            storage.close()

    def _read_one(self) -> tuple[int, bytes]:
        return self.reader.read_packet()

    # -- CONNECT ------------------------------------------------------------

    def _handle_connect(self, storage: Storage, body: bytes) -> None:
        try:
            info = framing.parse_connect(body)
        except framing.RefusedConnect as exc:
            # 3.2.2-3：返回码非 0 时 Session Present 必须为 0。
            self.send_all(framing.encode_connack(False, exc.code))
            raise framing.ConnectionClosed("connect refused")

        # 先让同名旧连接无法继续收发，再提交新 epoch。
        # （旧连接可能正在一个写事务中；SQLite 写锁串行化，那一个事务会
        #  原子完成，其后的一切写都因 epoch 失效被拒。）
        epoch, session_present = storage.begin_connect(
            info.client_id, info.clean_session)
        self.registry.take_over(info.client_id, epoch, self.sock)

        self.client_id = info.client_id
        self.epoch = epoch
        self.clean = info.clean_session
        self.established = True

        # 3.1.2-21：CleanSession=0 且存在旧会话时 Session Present=1。
        self.send_all(framing.encode_connack(
            session_present=session_present,
            return_code=CONNACK_ACCEPTED))

        # Keepalive（3.1.2-24）：1.5 倍；keepalive=0 表示不做空闲检测。
        if info.keepalive > 0:
            self.sock.settimeout(max(1.0, info.keepalive * 1.5))
        else:
            self.sock.settimeout(None)

    # -- 分发 ---------------------------------------------------------------

    def _dispatch(self, storage: Storage, first: int, body: bytes) -> None:
        ptype = (first & 0xF0) >> 4
        flags = first & 0x0F

        if ptype == PUBLISH:
            self._handle_publish(storage, first, body)
        elif ptype == PUBREL:
            pid = framing.parse_pid_packet(first, body, PUBREL,
                                           expect_flags=0x02)
            # 首次：业务账写入与交换结束同一事务提交；重放：不重复入账。
            storage.finish_on_pubrel(self.client_id, self.epoch, pid)
            # 事务提交之后才发 PUBCOMP。
            self.send_all(framing.encode_pubcomp(pid))
        elif ptype == PINGREQ:
            if flags != 0 or len(body) != 0:
                raise framing.ProtocolError("malformed PINGREQ")
            self.send_all(framing.encode_pingresp())
        elif ptype == DISCONNECT:
            if flags != 0 or len(body) != 0:
                raise framing.ProtocolError("malformed DISCONNECT")
            storage.mark_disconnected(self.client_id, self.epoch, self.clean)
            self.finished = True
            self.registry.remove(self.client_id, self.epoch)
            raise framing.ConnectionClosed("client disconnect")
        elif ptype in (CONNECT, PUBACK, PUBREC, PUBCOMP, SUBSCRIBE, SUBACK,
                       UNSUBSCRIBE, UNSUBACK, CONNACK):
            # 第二条 CONNECT、客户端发送仅服务端可发的报文、订阅类：
            # 本服务一律按非法报文关闭连接。
            raise framing.ProtocolError(
                f"unexpected/unsupported packet type {ptype}")
        else:
            raise framing.ProtocolError(f"reserved packet type {ptype}")

    def _handle_publish(self, storage: Storage, first: int,
                        body: bytes) -> None:
        pub = framing.parse_publish(first, body)
        if pub.retain:
            # 本接收器不支持保留消息（4.1 限定）。
            raise framing.ProtocolError("retained messages are not supported")
        if pub.qos != 2:
            # 仅接收 QoS 2 入站消息。
            raise framing.ProtocolError("only QoS 2 PUBLISH is accepted")
        if len(pub.payload) > framing.MAX_PAYLOAD:
            raise framing.ProtocolError("payload exceeds 4 KiB")
        # parse_publish 已保证 qos>0 时 packet_id 非 0。

        # PUBREC 之前必须先完成持久化（含崩溃注入点）。
        storage.save_pending(self.client_id, self.epoch, pub.packet_id,
                             pub.topic, pub.payload, pub.dup)
        self.send_all(framing.encode_pubrec(pub.packet_id))


# ---------------------------------------------------------------------------
# 服务主循环
# ---------------------------------------------------------------------------

def serve(db_path: str, host: str = "0.0.0.0", port: int = 1883) -> None:
    boot = Storage(db_path)
    boot.recover_after_restart()
    boot.close()

    registry = SessionRegistry()

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host, port))
    srv.listen(128)

    bound_host, bound_port = srv.getsockname()
    print(f"LISTENING {bound_host} {bound_port}", flush=True)

    try:
        while True:
            conn, addr = srv.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            handler = ClientHandler(conn, addr, db_path, registry)
            t = threading.Thread(target=handler.run, daemon=True)
            t.start()
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description="MQTT 3.1.1 入站 QoS2 -> SQLite 接收器")
    p.add_argument("--db", required=True, help="SQLite 数据库文件路径")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--port", type=int, default=1883)
    args = p.parse_args(argv)
    serve(args.db, args.host, args.port)


if __name__ == "__main__":
    main()
