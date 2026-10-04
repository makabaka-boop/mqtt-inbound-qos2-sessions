"""测试公共工具：字节级 MQTT 客户端与崩溃可注入的子进程服务器。

刻意不使用任何 MQTT 客户端库——测试要能逐字节控制 TCP 写入
（拆包、粘连、非法首字节等），库会把这些细节藏起来。
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from contextlib import contextmanager

from mqtt_receiver import codec

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SERVER_SCRIPT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_server.py")


# ---------------------------------------------------------------------------
# 字节级 MQTT 客户端
# ---------------------------------------------------------------------------


class RawMQTTClient:
    def __init__(self, host: str, port: int, timeout: float = 5.0):
        self.host = host
        self.port = port
        self.sock = socket.create_connection((host, port), timeout=timeout)
        self.sock.settimeout(timeout)
        self._buf = bytearray()
        # 关闭 Nagle，保证我们的逐字节发送不会被攒包
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def close(self) -> None:
        try:
            self.sock.close()
        except OSError:
            pass

    # -- 底层读写 -----------------------------------------------------------

    def send_raw(self, data: bytes, delay: float = 0.0) -> None:
        """逐字节发送以强制真实 TCP 拆包（delay 为每字节间隔）。"""
        for i, b in enumerate(data):
            if delay and i:
                time.sleep(delay)
            self.sock.sendall(bytes([b]))

    def send_raw_chunks(self, chunks: list[bytes], delay: float = 0.0) -> None:
        for i, chunk in enumerate(chunks):
            if delay and i:
                time.sleep(delay)
            self.sock.sendall(chunk)

    def recv_exactly(self, n: int) -> bytes:
        out = bytearray()
        while len(out) < n:
            data = self.sock.recv(n - len(out))
            if not data:
                raise ConnectionError("连接被对端关闭")
            out.extend(data)
        return bytes(out)

    def read_packet(self):
        """读取一个完整 MQTT 报文（服务端可能多个响应粘连，缓冲剩余部分）。"""
        # 等首字节
        while not self._buf:
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionError("连接被对端关闭")
            self._buf.extend(data)

        multiplier = 1
        value = 0
        idx = 1
        while True:
            while idx >= len(self._buf):
                data = self.sock.recv(4096)
                if not data:
                    raise ConnectionError("连接被对端关闭")
                self._buf.extend(data)
            byte = self._buf[idx]
            value += (byte & 0x7F) * multiplier
            idx += 1
            if not (byte & 0x80):
                break
            multiplier *= 128

        total = idx + value
        while len(self._buf) < total:
            data = self.sock.recv(4096)
            if not data:
                raise ConnectionError("连接被对端关闭")
            self._buf.extend(data)

        first = self._buf[0]
        remaining = bytes(self._buf[idx:total])
        del self._buf[:total]
        return codec.parse_packet(first, remaining)

    def expect_closed(self, timeout: float = 3.0) -> bool:
        """断言对端已关闭连接（读到 EOF 或 RST）。"""
        self.sock.settimeout(timeout)
        try:
            data = self.sock.recv(1)
            return data == b""
        except (ConnectionResetError, ConnectionError, socket.timeout, OSError):
            return False
        finally:
            self.sock.settimeout(5.0)

    # -- MQTT 高层操作 ------------------------------------------------------

    def connect(
        self,
        client_id: str,
        clean_session: bool = True,
        keep_alive: int = 0,
        raw: bytes | None = None,
    ) -> codec.Connack:
        if raw is not None:
            packet = raw
        else:
            packet = codec.encode_connect(client_id, clean_session, keep_alive)
        self.sock.sendall(packet)
        ack = self.read_packet()
        assert isinstance(ack, codec.Connack), f"期望 CONNACK，得到 {ack!r}"
        return ack

    def publish_qos2(
        self,
        topic: str,
        packet_id: int,
        payload: bytes,
        dup: bool = False,
    ) -> None:
        self.sock.sendall(
            codec.encode_publish(topic, packet_id, payload, dup=dup, qos=2)
        )

    def expect_pubrec(self, packet_id: int) -> None:
        pkt = self.read_packet()
        assert isinstance(pkt, codec.PacketIdOnly) and pkt.kind == codec.PUBREC, (
            f"期望 PUBREC {packet_id}，得到 {pkt!r}"
        )
        assert pkt.packet_id == packet_id

    def pubrel(self, packet_id: int) -> None:
        self.sock.sendall(codec.encode_pubrel(packet_id))

    def expect_pubcomp(self, packet_id: int) -> None:
        pkt = self.read_packet()
        assert isinstance(pkt, codec.PacketIdOnly) and pkt.kind == codec.PUBCOMP, (
            f"期望 PUBCOMP {packet_id}，得到 {pkt!r}"
        )
        assert pkt.packet_id == packet_id

    def ping(self) -> None:
        self.sock.sendall(codec.encode_pingreq())
        pkt = self.read_packet()
        assert pkt is codec.PINGRESP

    def disconnect(self) -> None:
        self.sock.sendall(codec.encode_disconnect())

    def deliver_qos2(
        self, topic: str, packet_id: int, payload: bytes, dup: bool = False
    ) -> None:
        """完整走完一次 QoS2 接收方握手。"""
        self.publish_qos2(topic, packet_id, payload, dup=dup)
        self.expect_pubrec(packet_id)
        self.pubrel(packet_id)
        self.expect_pubcomp(packet_id)


# ---------------------------------------------------------------------------
# 子进程服务器（可被 os._exit 杀死后重启）
# ---------------------------------------------------------------------------


class ServerProcess:
    def __init__(self, db_path: str, env: dict | None = None):
        self.db_path = db_path
        self.port_file = db_path + ".port"
        self.proc: subprocess.Popen | None = None
        self._base_env = dict(env or {})
        self.host = "127.0.0.1"
        self.port = 0
        if os.path.exists(self.port_file):
            os.unlink(self.port_file)

    def start(self, env_extra: dict | None = None, wait_ready: bool = True):
        env = dict(os.environ)
        env.update(self._base_env)
        if env_extra:
            env.update(env_extra)
        self.proc = subprocess.Popen(
            [
                sys.executable,
                SERVER_SCRIPT,
                self.db_path,
                self.port_file,
                "--host",
                self.host,
            ],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if wait_ready:
            self._wait_ready()
        return self

    def _wait_ready(self, timeout: float = 10.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc and self.proc.poll() is not None:
                raise RuntimeError(f"服务器提前退出，code={self.proc.returncode}")
            try:
                with open(self.port_file) as f:
                    host, port = f.read().strip().split(":")
                self.host, self.port = host, int(port)
                # 确认端口可连
                with socket.create_connection((self.host, self.port), timeout=0.5):
                    pass
                return
            except (OSError, ValueError):
                time.sleep(0.02)
        raise TimeoutError("等待服务器就绪超时")

    def restart(self, env_extra: dict | None = None):
        assert self.proc is not None
        # 正常重启：SIGTERM（崩溃场景由 kill -9 / 注入点自行处理）
        self.stop()
        self.start(env_extra=env_extra)
        return self

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=5)
        self.proc = None

    def kill(self, sig: int = 9) -> None:
        """外部强杀（模拟确认丢失场景）。"""
        assert self.proc is not None
        self.proc.send_signal(sig)
        self.proc.wait(timeout=5)
        self.proc = None

    def wait_exit(self, timeout: float = 5.0) -> int:
        """等待进程自行退出（崩溃注入点触发时）。"""
        assert self.proc is not None
        code = self.proc.wait(timeout=timeout)
        self.proc = None
        return code

    def client(self, timeout: float = 5.0) -> RawMQTTClient:
        return RawMQTTClient(self.host, self.port, timeout=timeout)


@contextmanager
def server_process(db_path: str, env: dict | None = None):
    sp = ServerProcess(db_path, env=env)
    sp.start()
    try:
        yield sp
    finally:
        sp.stop()
