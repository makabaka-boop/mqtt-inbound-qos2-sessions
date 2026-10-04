"""子进程方式启动真实 TCP 服务器，支持崩溃注入后重启重放。"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time


class ServerProcess:
    def __init__(self, db_path: str, crash: str = "", port: int = 0):
        self.db_path = db_path
        self.port = port
        self.proc: subprocess.Popen | None = None
        self.crash = crash

    def start(self, timeout: float = 10.0) -> int:
        env = os.environ.copy()
        env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if self.crash:
            env["MQTT_INBOX_CRASH"] = self.crash

        args = [sys.executable, "-m", "mqtt_inbox.server",
                "--db", self.db_path, "--host", "127.0.0.1",
                "--port", str(self.port)]
        self.proc = subprocess.Popen(
            args, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True)

        deadline = time.time() + timeout
        while time.time() < deadline:
            line = self.proc.stdout.readline()
            if line.startswith("LISTENING"):
                _, _host, port = line.split()
                self.port = int(port)
                return self.port
            if self.proc.poll() is not None:
                err = self.proc.stderr.read()
                raise RuntimeError(f"server exited early: {err}")
            time.sleep(0.02)
        self.kill()
        raise RuntimeError("server did not start in time")

    def kill(self) -> int:
        """SIGKILL 强制终止（模拟进程被杀死），返回退出码。"""
        code = -1
        if self.proc and self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait(timeout=10)
        if self.proc is not None:
            code = self.proc.returncode
            for stream in (self.proc.stdout, self.proc.stderr):
                try:
                    stream.close()
                except OSError:
                    pass
        self.proc = None
        return code

    def terminate(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self.proc is not None:
            for stream in (self.proc.stdout, self.proc.stderr):
                try:
                    stream.close()
                except OSError:
                    pass
        self.proc = None

    def stderr_tail(self) -> str:
        if not self.proc:
            return ""
        try:
            return self.proc.stderr.read()
        except Exception:
            return ""


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
