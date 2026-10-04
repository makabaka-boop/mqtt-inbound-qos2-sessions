"""SQLite 持久层：会话、QoS 2 交换状态与业务收件账。

关键事务边界：
- ``save_pending``：PUBREC 发出之前，把待交付消息持久化（崩溃后重连仍在）；
- ``finish_on_pubrel``：业务收件账写入 + 交换状态翻为 done，必须在同一个
  SQLite 事务内提交，提交之后才允许发 PUBCOMP；
- 所有写事务都带 epoch 条件，被同名新连接接管的旧连接（旧 epoch）无法改账。

``qos2_flows.state``：
  pending  收到 PUBLISH，PUBREC 已发/待发，尚未在收到 PUBREL 后入账；
  done     交换完成的墓碑：用于重复 PUBREL/PUBLISH(DUP=1) 重放去重；
           CleanSession=0 下跨重启保留；新 PUBLISH(DUP=0) 可顶替复用该 PacketId，
           因此不会永久去重该编号。
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time
from dataclasses import dataclass

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    client_id  TEXT PRIMARY KEY,
    clean      INTEGER NOT NULL,
    epoch      INTEGER NOT NULL,
    connected  INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS qos2_flows (
    client_id  TEXT NOT NULL,
    packet_id  INTEGER NOT NULL,
    state      TEXT NOT NULL CHECK (state IN ('pending', 'done')),
    topic      TEXT NOT NULL,
    payload    BLOB NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (client_id, packet_id)
);
CREATE TABLE IF NOT EXISTS inbox (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id    TEXT NOT NULL,
    topic        TEXT NOT NULL,
    payload      BLOB NOT NULL,
    packet_id    INTEGER NOT NULL,
    delivered_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_inbox_client ON inbox(client_id);
"""

# ---------------------------------------------------------------------------
# 崩溃注入点（仅测试使用）
#
# MQTT_INBOX_CRASH 取值：
#   before_pending_commit / after_pending_commit
#   before_pubrel_commit  / after_pubrel_commit
# 进程立即以 137 退出，模拟“持久提交前后、响应发出前”被杀。
# ---------------------------------------------------------------------------

_CRASH_POINT = os.environ.get("MQTT_INBOX_CRASH", "")


def _crash_if(point: str) -> None:
    if _CRASH_POINT == point:
        sys.stderr.write(f"[crash-inject] {point}\n")
        sys.stderr.flush()
        os._exit(137)


@dataclass
class InboxItem:
    id: int
    client_id: str
    topic: str
    payload: bytes
    packet_id: int
    delivered_at: float


class SessionTakenOver(Exception):
    """当前连接对应的 epoch 已失效（同名新连接接管），必须立刻停止处理。"""


class _Transaction:
    """BEGIN IMMEDIATE 事务上下文，保证异常时只回滚一次。"""

    def __init__(self, conn):
        self.conn = conn
        self.closed = False

    def __enter__(self):
        self.conn.execute("BEGIN IMMEDIATE")
        return self

    def commit(self):
        if not self.closed:
            self.conn.execute("COMMIT")
            self.closed = True

    def rollback(self):
        if not self.closed:
            self.conn.execute("ROLLBACK")
            self.closed = True

    def __exit__(self, exc_type, exc, tb):
        if exc_type is not None and not self.closed:
            self.conn.execute("ROLLBACK")
            self.closed = True
        return False


class Storage:
    """每个连接持有一个 Storage；读查询 CLI 也用同一文件（WAL 允许并发读）。"""

    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path, timeout=5, isolation_level=None,
                                    check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)

    def close(self) -> None:
        self.conn.close()

    # -- 启动恢复 -----------------------------------------------------------

    def recover_after_restart(self) -> None:
        """进程重启：上次所有 connected=1 的会话实际上都已断线。"""
        self.conn.execute("UPDATE sessions SET connected=0")

    def _tx(self) -> _Transaction:
        return _Transaction(self.conn)

    @staticmethod
    def _check_epoch(conn, client_id: str, epoch: int, tx: _Transaction) -> None:
        row = conn.execute(
            "SELECT epoch FROM sessions WHERE client_id=? AND epoch=?",
            (client_id, epoch)).fetchone()
        if row is None:
            tx.rollback()
            raise SessionTakenOver()

    # -- 连接生命周期 -------------------------------------------------------

    def begin_connect(self, client_id: str, clean: bool):
        """处理 CONNECT 的全部持久化操作（单事务）。

        clean=1：删除该 ClientId 旧会话与全部 QoS2 交换状态（3.1.4-4）。
                 收件账是业务账，不删除。
        clean=0：旧会话保留，Session Present 取决于旧会话是否存在。
        返回 (epoch, session_present)。新 epoch 使同名旧连接的写全部失效。
        """
        with self._tx() as tx:
            row = self.conn.execute(
                "SELECT epoch FROM sessions WHERE client_id=?", (client_id,)
            ).fetchone()

            if clean:
                self.conn.execute(
                    "DELETE FROM qos2_flows WHERE client_id=?", (client_id,))
                epoch = (row[0] + 1) if row is not None else 1
                self.conn.execute(
                    "INSERT INTO sessions(client_id, clean, epoch, connected) "
                    "VALUES(?,1,?,1) ON CONFLICT(client_id) DO UPDATE SET "
                    "clean=1, epoch=excluded.epoch, connected=1",
                    (client_id, epoch))
                sp = False
            else:
                if row is not None:
                    epoch = row[0] + 1
                    sp = True
                    self.conn.execute(
                        "UPDATE sessions SET clean=0, epoch=?, connected=1 "
                        "WHERE client_id=?", (epoch, client_id))
                else:
                    epoch = 1
                    sp = False
                    self.conn.execute(
                        "INSERT INTO sessions(client_id, clean, epoch, connected) "
                        "VALUES(?,0,1,1)", (client_id,))
            tx.commit()
            return epoch, sp

    def mark_disconnected(self, client_id: str, epoch: int, clean: bool) -> None:
        """正常/异常断线。CleanSession=1 删除协议会话，CleanSession=0 保留。"""
        with self._tx() as tx:
            if clean:
                # 收件账是业务账，永不随会话删除；协议会话与交换状态全部清除。
                self.conn.execute(
                    "DELETE FROM qos2_flows WHERE client_id=?", (client_id,))
                cur = self.conn.execute(
                    "DELETE FROM sessions WHERE client_id=? AND epoch=?",
                    (client_id, epoch))
            else:
                cur = self.conn.execute(
                    "UPDATE sessions SET connected=0 "
                    "WHERE client_id=? AND epoch=?", (client_id, epoch))
            if cur.rowcount == 0:
                # 已被新连接接管：不得改动任何会话状态。
                tx.rollback()
                raise SessionTakenOver()
            tx.commit()

    # -- QoS 2 --------------------------------------------------------------

    def save_pending(self, client_id: str, epoch: int, packet_id: int,
                     topic: str, payload: bytes, dup: bool) -> str:
        """PUBREC 前持久化 PUBLISH。

        返回动作：
          'stored'  新消息或 PacketId 复用（DUP=0 顶替旧墓碑），已落盘；
          'repeat'  重复 PUBLISH（pending 重传，或对已完成交换的 DUP=1 重发），
                    不覆盖已存内容，仍应重发 PUBREC。
        epoch 不符 -> SessionTakenOver，旧连接无法改账。
        """
        now = time.time()
        with self._tx() as tx:
            self._check_epoch(self.conn, client_id, epoch, tx)

            flow = self.conn.execute(
                "SELECT state FROM qos2_flows WHERE client_id=? AND packet_id=?",
                (client_id, packet_id)).fetchone()

            if flow is not None and (flow[0] == "pending" or dup):
                # 重传 PUBLISH（4.3.3 DUP 重发）或对已完成交换的 DUP=1 重发：
                # 保留原内容，幂等；调用方仍重发 PUBREC。
                tx.commit()
                return "repeat"

            _crash_if("before_pending_commit")
            self.conn.execute(
                "INSERT INTO qos2_flows(client_id, packet_id, state, topic, "
                "payload, updated_at) VALUES(?,?, 'pending', ?, ?, ?) "
                "ON CONFLICT(client_id, packet_id) DO UPDATE SET "
                "state='pending', topic=excluded.topic, payload=excluded.payload, "
                "updated_at=excluded.updated_at",
                (client_id, packet_id, topic, sqlite3.Binary(payload), now))
            tx.commit()
        _crash_if("after_pending_commit")
        return "stored"

    def finish_on_pubrel(self, client_id: str, epoch: int,
                         packet_id: int) -> str:
        """收到 PUBREL：业务账写入 + 交换结束，必须同一事务，提交后才 PUBCOMP。

        返回：
          'delivered' 首次处理 PUBREL，inbox 新增一条；
          'repeat'    PUBREL 重放（done 墓碑或本连接无该交换）：不重复入账，
                      调用方仍按 4.2.2 重发 PUBCOMP。
        """
        now = time.time()
        with self._tx() as tx:
            self._check_epoch(self.conn, client_id, epoch, tx)

            flow = self.conn.execute(
                "SELECT state, topic, payload FROM qos2_flows "
                "WHERE client_id=? AND packet_id=?",
                (client_id, packet_id)).fetchone()

            if flow is None or flow["state"] == "done":
                # 4.2.2：响应 PUBCOMP；已完成的消息不重复交付。
                tx.commit()
                return "repeat"

            _crash_if("before_pubrel_commit")
            # —— 同一事务：业务账写入与交换状态结束 ——
            self.conn.execute(
                "INSERT INTO inbox(client_id, topic, payload, packet_id, "
                "delivered_at) VALUES(?,?,?,?,?)",
                (client_id, flow["topic"], flow["payload"], packet_id, now))
            self.conn.execute(
                "UPDATE qos2_flows SET state='done', updated_at=? "
                "WHERE client_id=? AND packet_id=?",
                (now, client_id, packet_id))
            tx.commit()
        _crash_if("after_pubrel_commit")
        return "delivered"

    # -- 只读查询 -----------------------------------------------------------

    def query_inbox(self, client_id: str | None = None,
                    limit: int = 100) -> list[InboxItem]:
        if client_id is None:
            rows = self.conn.execute(
                "SELECT id, client_id, topic, payload, packet_id, delivered_at "
                "FROM inbox ORDER BY id LIMIT ?", (limit,)).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT id, client_id, topic, payload, packet_id, delivered_at "
                "FROM inbox WHERE client_id=? ORDER BY id LIMIT ?",
                (client_id, limit)).fetchall()
        return [
            InboxItem(r["id"], r["client_id"], r["topic"], bytes(r["payload"]),
                      r["packet_id"], r["delivered_at"])
            for r in rows
        ]

    def flow_state(self, client_id: str, packet_id: int) -> str | None:
        row = self.conn.execute(
            "SELECT state FROM qos2_flows WHERE client_id=? AND packet_id=?",
            (client_id, packet_id)).fetchone()
        return row[0] if row else None

    def inbox_count(self, client_id: str, packet_id: int) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM inbox WHERE client_id=? AND packet_id=?",
            (client_id, packet_id)).fetchone()[0]
