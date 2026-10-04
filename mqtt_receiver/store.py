"""SQLite 持久化层：会话、QoS2 交换状态、业务收件账。

关键保证
========
1. PUBREC 之前，待交付消息已经在事务中落盘（REC 状态）；
2. 收到 PUBREL 后，"业务收件账写入" 与 "交换状态结束（REL）" 在同一个
   SQLite 事务内提交，之后才允许发送 PUBCOMP；
3. 每次 PUBLISH 落库都分配一个单调递增的 exchange_seq；收件账以
   exchange_seq 为唯一键——同一交换绝不重复交付，而完成交换后复用同一个
   Packet Identifier 会产生新的 exchange_seq，可以正常再次入帐；
4. 会话以 ClientId 为键；CleanSession=0 的未完成交换跨进程重启保留，
   CleanSession=1 不建立持久会话。

崩溃注入
========
以下环境变量用于测试在特定点强杀进程（值为 ``client_id:packet_id``，
packet_id 省略时匹配该客户端的全部报文）：

- MQTT_RECEIVER_CRASH_BEFORE_PUBLISH_COMMIT
- MQTT_RECEIVER_CRASH_AFTER_PUBLISH_COMMIT
- MQTT_RECEIVER_CRASH_BEFORE_PUBREL_COMMIT
- MQTT_RECEIVER_CRASH_AFTER_PUBREL_COMMIT
"""

from __future__ import annotations

import os
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    client_id     TEXT PRIMARY KEY,
    clean_session INTEGER NOT NULL,
    created_at    TEXT NOT NULL,
    last_seen     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS qos2_exchanges (
    exchange_seq  INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id     TEXT NOT NULL,
    inbox_client_id TEXT NOT NULL,
    packet_id     INTEGER NOT NULL,
    state         TEXT NOT NULL CHECK (state IN ('REC', 'REL')),
    topic         TEXT NOT NULL,
    payload       BLOB NOT NULL,
    created_at    TEXT NOT NULL,
    updated_at    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    client_id    TEXT NOT NULL,
    topic        TEXT NOT NULL,
    payload      BLOB NOT NULL,
    exchange_seq INTEGER NOT NULL UNIQUE,
    received_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_exchange_lookup
    ON qos2_exchanges(client_id, packet_id);
CREATE INDEX IF NOT EXISTS idx_inbox_client
    ON inbox(client_id, id);
"""

STATE_REC = "REC"  # 已落库待交付，已发/待发 PUBREC
STATE_REL = "REL"  # PUBREL 已处理：已入账，交换结束，已发/待发 PUBCOMP


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _crash_matches(spec: Optional[str], client_id: str, packet_id: int) -> bool:
    if not spec:
        return False
    if ":" in spec:
        cid, pid = spec.split(":", 1)
        return cid == client_id and pid == str(packet_id)
    return spec == client_id


class Store:
    def __init__(self, path: str):
        self.path = path
        # isolation_level=None：显式 BEGIN/COMMIT，事务边界完全可控。
        self._conn = sqlite3.connect(
            path, isolation_level=None, check_same_thread=False
        )
        self._write_lock = threading.RLock()
        self._configure()
        self._init_schema()

    def _configure(self) -> None:
        # WAL：写入持久化时读查询不被阻塞；NORMAL 同步级别在 WAL 下
        # 事务提交仍跨崩溃安全（fsync 于 checkpoint）。
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")

    def _init_schema(self) -> None:
        # executescript 在 isolation_level=None 下会自行提交；
        # 建表语句带 IF NOT EXISTS，幂等且只需执行一次。
        with self._write_lock:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._write_lock:
            # WAL checkpoint，让 -wal 内容并入主库，方便测试后直接核对。
            try:
                self._conn.execute("PRAGMA wal_checkpoint(FULL)")
            except sqlite3.DatabaseError:
                pass
            self._conn.close()

    # ------------------------------------------------------------------
    # 崩溃注入钩子（测试专用）
    # ------------------------------------------------------------------

    @staticmethod
    def _maybe_crash(point: str, client_id: str, packet_id: int) -> None:
        spec = os.environ.get(point)
        if spec and _crash_matches(spec, client_id, packet_id):
            # 立即终止进程：无 finally、无 flush，模拟 SIGKILL / 掉电。
            os._exit(91)

    # ------------------------------------------------------------------
    # 会话
    # ------------------------------------------------------------------

    def begin_connect(self, client_id: str, clean_session: bool) -> bool:
        """处理新连接的会话归属，返回 CONNACK 的 Session Present。

        - CleanSession=1：清除该 ClientId 的旧会话与全部交换状态，
          且不新建持久会话行，Session Present 恒为 0；
        - CleanSession=0：存在旧会话则 Session Present=1，否则建立新会话。
        """
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT 1 FROM sessions WHERE client_id=?", (client_id,)
                ).fetchone()

                if clean_session:
                    self._conn.execute(
                        "DELETE FROM qos2_exchanges WHERE client_id=?",
                        (client_id,),
                    )
                    self._conn.execute(
                        "DELETE FROM sessions WHERE client_id=?", (client_id,)
                    )
                    present = False
                else:
                    if row is None:
                        # 新的持久会话：清掉可能残留的崩溃 CleanSession 交换。
                        self._conn.execute(
                            "DELETE FROM qos2_exchanges WHERE client_id=?",
                            (client_id,),
                        )
                        self._conn.execute(
                            "INSERT INTO sessions"
                            "(client_id, clean_session, created_at, last_seen) "
                            "VALUES (?, 0, ?, ?)",
                            (client_id, _now(), _now()),
                        )
                    else:
                        self._conn.execute(
                            "UPDATE sessions SET last_seen=? WHERE client_id=?",
                            (_now(), client_id),
                        )
                    present = row is not None

                self._conn.execute("COMMIT")
                return present
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def touch_session(self, client_id: str) -> None:
        with self._write_lock:
            self._conn.execute(
                "UPDATE sessions SET last_seen=? WHERE client_id=?",
                (_now(), client_id),
            )

    def cleanup_ephemeral(self, client_id: str) -> None:
        """CleanSession=1 连接结束时清掉本次连接产生的交换状态。

        CleanSession=1 没有 sessions 行；若进程在交换中途被杀，残留行会在
        下一次同 ClientId 连接（任意 CleanSession）时被 begin_connect 清除。
        """
        with self._write_lock:
            self._conn.execute(
                "DELETE FROM qos2_exchanges WHERE client_id=?", (client_id,)
            )

    # ------------------------------------------------------------------
    # QoS2 交换
    # ------------------------------------------------------------------

    def get_exchange_state(
        self, client_id: str, packet_id: int
    ) -> Optional[str]:
        row = self._conn.execute(
            "SELECT state FROM qos2_exchanges WHERE client_id=? AND packet_id=?",
            (client_id, packet_id),
        ).fetchone()
        return row[0] if row else None

    def persist_publish(
        self,
        client_id: str,
        packet_id: int,
        topic: str,
        payload: bytes,
        dup: bool,
        inbox_client_id: str | None = None,
    ) -> str:
        """PUBREC 之前持久化待交付消息。

        ``client_id`` 是会话键（空 ClientId 时为服务端分配的内部键）；
        ``inbox_client_id`` 是最终写入收件账的对外 ClientId。

        返回值决定服务端动作：
        - ``'stored'``    ：新消息已落库，发送 PUBREC；
        - ``'resend_rec'``：已有 REC 交换（重复 PUBLISH），重发 PUBREC；
        - ``'resend_comp'``：交换已完成且收到 DUP=1 重传，重发 PUBCOMP。

        DUP=0 的 PUBLISH 命中已完成交换视为 Packet Identifier 被复用：
        用新消息覆盖旧交换（新 exchange_seq），正常走新一轮交付。
        """
        if inbox_client_id is None:
            inbox_client_id = client_id
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT state FROM qos2_exchanges WHERE client_id=? AND packet_id=?",
                    (client_id, packet_id),
                ).fetchone()

                if row is not None:
                    state = row[0]
                    if state == STATE_REC:
                        self._conn.execute("ROLLBACK")
                        return "resend_rec"
                    if dup:
                        self._conn.execute("ROLLBACK")
                        return "resend_comp"
                    # REL + DUP=0：编号复用，覆盖旧交换（新 exchange_seq），
                    # DELETE 与 INSERT 在同一事务内原子完成。
                    self._conn.execute(
                        "DELETE FROM qos2_exchanges WHERE client_id=? AND packet_id=?",
                        (client_id, packet_id),
                    )

                now = _now()
                self._conn.execute(
                    "INSERT INTO qos2_exchanges"
                    "(client_id, inbox_client_id, packet_id, state, topic, payload, "
                    " created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        client_id,
                        inbox_client_id,
                        packet_id,
                        STATE_REC,
                        topic,
                        sqlite3.Binary(payload),
                        now,
                        now,
                    ),
                )
                self._maybe_crash(
                    "MQTT_RECEIVER_CRASH_BEFORE_PUBLISH_COMMIT",
                    client_id,
                    packet_id,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        self._maybe_crash(
            "MQTT_RECEIVER_CRASH_AFTER_PUBLISH_COMMIT", client_id, packet_id
        )
        return "stored"

    def complete_pubrel(self, client_id: str, packet_id: int) -> str:
        """处理 PUBREL：收件账写入与交换结束在同一事务内完成。

        返回：
        - ``'completed'``   ：本调用完成入账，调用方随后发 PUBCOMP；
        - ``'already_rel'`` ：入账此前已完成（PUBCOMP 丢失/重复 PUBREL），
                              只重发 PUBCOMP，绝不重复入账；
        - ``'unknown'``     ：无此交换。按 MQTT 3.1.1 §3.3.4/§3.6.3 的
                              重传语义，仍回复 PUBCOMP 保持状态机自洽，
                              但不写收件账。
        """
        with self._write_lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT state, exchange_seq, topic, payload, inbox_client_id "
                    "FROM qos2_exchanges WHERE client_id=? AND packet_id=?",
                    (client_id, packet_id),
                ).fetchone()

                if row is None:
                    self._conn.execute("COMMIT")
                    return "unknown"

                state, exchange_seq, topic, payload, inbox_cid = row
                if state == STATE_REL:
                    self._conn.execute("COMMIT")
                    return "already_rel"

                # 原子动作一：交换状态结束；动作二：写业务收件账。
                # 二者必须同时可见，随后才能发 PUBCOMP。
                self._conn.execute(
                    "UPDATE qos2_exchanges SET state=?, updated_at=? "
                    "WHERE client_id=? AND packet_id=? AND state=?",
                    (STATE_REL, _now(), client_id, packet_id, STATE_REC),
                )
                self._conn.execute(
                    "INSERT INTO inbox"
                    "(client_id, topic, payload, exchange_seq, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        inbox_cid,
                        topic,
                        payload,
                        exchange_seq,
                        _now(),
                    ),
                )
                self._maybe_crash(
                    "MQTT_RECEIVER_CRASH_BEFORE_PUBREL_COMMIT",
                    client_id,
                    packet_id,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

        self._maybe_crash(
            "MQTT_RECEIVER_CRASH_AFTER_PUBREL_COMMIT", client_id, packet_id
        )
        return "completed"

    # ------------------------------------------------------------------
    # 只读收件查询
    # ------------------------------------------------------------------

    def list_inbox(
        self, client_id: Optional[str] = None, limit: int = 100
    ) -> list[dict]:
        if client_id is None:
            rows = self._conn.execute(
                "SELECT id, client_id, topic, payload, exchange_seq, received_at "
                "FROM inbox ORDER BY id ASC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT id, client_id, topic, payload, exchange_seq, received_at "
                "FROM inbox WHERE client_id=? ORDER BY id ASC LIMIT ?",
                (client_id, limit),
            ).fetchall()
        return [
            {
                "id": r[0],
                "client_id": r[1],
                "topic": r[2],
                "payload": bytes(r[3]),
                "exchange_seq": r[4],
                "received_at": r[5],
            }
            for r in rows
        ]

    def count_inbox(self, client_id: Optional[str] = None) -> int:
        if client_id is None:
            row = self._conn.execute("SELECT COUNT(*) FROM inbox").fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) FROM inbox WHERE client_id=?", (client_id,)
            ).fetchone()
        return int(row[0])

    def list_sessions(self) -> list[str]:
        return [
            r[0]
            for r in self._conn.execute(
                "SELECT client_id FROM sessions ORDER BY client_id"
            ).fetchall()
        ]
