"""真实 TCP 端到端测试（仅依赖标准库 unittest）。

运行：
    python -m unittest tests.test_mqtt_receiver -v
或：
    python tests/run_tests.py

覆盖：
1. CONNECT/CONNACK、Session Present、CleanSession=1 清旧会话；
2. QoS2 完整握手、4KiB 边界、PINGREQ/PINGRESP、DISCONNECT；
3. 真实拆包（逐字节发送，含 Remaining Length 跨边界）与多报文粘连；
4. 非法报文关闭连接；无遗嘱/无保留/QoS 限制；
5. 重复 PUBLISH、PUBREL 重发、PUBREC/PUBCOMP 丢失重连不重复交付；
6. 同名新连接接管，旧连接不能再改变会话；
7. Packet Identifier 完成交换后可复用、不永久去重；
8. 持久化提交前/后、响应发出前 os._exit 强杀进程，重启重放：
   账目不重不丢，Session Present 正确。
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tempfile
import time
import unittest

from mqtt_receiver import codec
from mqtt_receiver.store import STATE_REC, STATE_REL, Store

from helpers import ServerProcess, server_process

# 崩溃注入环境变量
CRASH_BEFORE_PUB_COMMIT = "MQTT_RECEIVER_CRASH_BEFORE_PUBLISH_COMMIT"
CRASH_AFTER_PUB_COMMIT = "MQTT_RECEIVER_CRASH_AFTER_PUBLISH_COMMIT"
CRASH_BEFORE_REL_COMMIT = "MQTT_RECEIVER_CRASH_BEFORE_PUBREL_COMMIT"
CRASH_AFTER_REL_COMMIT = "MQTT_RECEIVER_CRASH_AFTER_PUBREL_COMMIT"


# ---------------------------------------------------------------------------
# 只读核对
# ---------------------------------------------------------------------------


def inbox_rows(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT client_id, topic, payload FROM inbox ORDER BY id"
        ).fetchall()
        return [(r[0], r[1], bytes(r[2])) for r in rows]
    finally:
        conn.close()


def exchange_states(db_path, client_id):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        rows = conn.execute(
            "SELECT packet_id, state FROM qos2_exchanges WHERE client_id=?",
            (client_id,),
        ).fetchall()
        return {r[0]: r[1] for r in rows}
    finally:
        conn.close()


def session_count(db_path):
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        return conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    finally:
        conn.close()


class ReceiverTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="mqtt-test-")
        self.db = os.path.join(self.tmp, "r.db")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)


# ---------------------------------------------------------------------------
# 1. CONNECT / CONNACK / Session Present
# ---------------------------------------------------------------------------


class TestConnect(ReceiverTestCase):
    def test_clean_first_connect_present_zero(self):
        with server_process(self.db) as sp:
            c = sp.client()
            ack = c.connect("dev1", clean_session=True)
            self.assertEqual(ack.return_code, 0)
            self.assertFalse(ack.session_present)

    def test_persistent_first_present_zero_second_present_one(self):
        with server_process(self.db) as sp:
            c1 = sp.client()
            ack = c1.connect("persist", clean_session=False)
            self.assertFalse(ack.session_present)
            c1.publish_qos2("t/a", 1, b"pending")
            c1.expect_pubrec(1)
            c1.close()  # 无 DISCONNECT，模拟断线

            c2 = sp.client()
            ack = c2.connect("persist", clean_session=False)
            self.assertTrue(ack.session_present)
            self.assertEqual(exchange_states(self.db, "persist"), {1: STATE_REC})

    def test_clean_session_clears_old_protocol_session(self):
        with server_process(self.db) as sp:
            c1 = sp.client()
            c1.connect("persist", clean_session=False)
            c1.deliver_qos2("t/a", 1, b"hello")
            c1.publish_qos2("t/b", 2, b"pending")
            c1.expect_pubrec(2)
            c1.disconnect()
            time.sleep(0.15)
            self.assertEqual(session_count(self.db), 1)

            # CleanSession=1 必须清除旧协议会话
            c2 = sp.client()
            ack = c2.connect("persist", clean_session=True)
            self.assertFalse(ack.session_present)
            self.assertEqual(session_count(self.db), 0)
            self.assertEqual(exchange_states(self.db, "persist"), {})
            # 已入收件账的业务记录不受影响（账目不是协议会话状态）
            self.assertEqual(inbox_rows(self.db), [("persist", "t/a", b"hello")])
            c2.disconnect()
            time.sleep(0.15)

            # 之后 CleanSession=0 重连，会话是全新的：present=0
            c3 = sp.client()
            ack = c3.connect("persist", clean_session=False)
            self.assertFalse(ack.session_present)

    def test_empty_client_id_requires_clean(self):
        with server_process(self.db) as sp:
            c = sp.client()
            ack = c.connect("", clean_session=True)
            self.assertEqual(ack.return_code, 0)
            c.disconnect()

            c2 = sp.client()
            ack = c2.connect("", clean_session=False)
            self.assertEqual(ack.return_code, codec.CONNACK_IDENTIFIER_REJECTED)
            self.assertTrue(c2.expect_closed())

    def test_two_anonymous_connections_do_not_take_over_each_other(self):
        """两个空 ClientId 连接各自独立：后连者不能踢掉先连者，各自能交付。"""
        with server_process(self.db) as sp:
            a = sp.client()
            a.connect("", clean_session=True)
            b = sp.client()
            b.connect("", clean_session=True)
            time.sleep(0.2)

            # 两条连接都仍然存活
            a.ping()
            b.ping()
            a.deliver_qos2("anon/a", 1, b"from-a")
            b.deliver_qos2("anon/b", 1, b"from-b")
            a.disconnect()
            b.disconnect()
            time.sleep(0.2)

        topics = sorted((t, p) for _, t, p in inbox_rows(self.db))
        self.assertEqual(
            topics, [("anon/a", b"from-a"), ("anon/b", b"from-b")]
        )

    def test_protocol_level_3_rejected(self):
        with server_process(self.db) as sp:
            c = sp.client()
            name = b"MQIsdp"
            vh = (
                len(name).to_bytes(2, "big")
                + name
                + bytes([3, 0x02])
                + (0).to_bytes(2, "big")
            )
            payload = (0).to_bytes(2, "big")
            body = vh + payload
            raw = bytes([0x10]) + codec.encode_remaining_length(len(body)) + body
            c.sock.sendall(raw)
            ack = c.read_packet()
            self.assertEqual(
                ack.return_code, codec.CONNACK_UNACCEPTABLE_PROTOCOL
            )
            self.assertTrue(c.expect_closed())

    def test_second_connect_on_same_tcp_rejected(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=True)
            c.sock.sendall(codec.encode_connect("dev", clean_session=True))
            self.assertTrue(c.expect_closed())


# ---------------------------------------------------------------------------
# 2. QoS2 正常交付
# ---------------------------------------------------------------------------


class TestQos2Delivery(ReceiverTestCase):
    def test_full_handshake_and_inbox(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=False)
            c.deliver_qos2("orders/new", 100, b'{"amount":42}')
            c.deliver_qos2("orders/new", 101, b"second")
            c.disconnect()
            time.sleep(0.15)

        self.assertEqual(
            inbox_rows(self.db),
            [
                ("dev", "orders/new", b'{"amount":42}'),
                ("dev", "orders/new", b"second"),
            ],
        )

    def test_payload_4k_boundary(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=False)
            c.deliver_qos2("big", 5, b"A" * 4096)  # 恰好 4 KiB
            c.publish_qos2("big2", 6, b"B" * 4097)  # 超限：断连
            self.assertTrue(c.expect_closed())
            time.sleep(0.1)

        self.assertEqual(inbox_rows(self.db), [("dev", "big", b"A" * 4096)])

    def test_pingreq_pingresp(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=True)
            c.ping()
            c.ping()
            c.disconnect()

    def test_will_rejected(self):
        with server_process(self.db) as sp:
            c = sp.client()
            name = b"MQTT"
            # Will flag=1, qos=0, retain=0 => flags 0b00000100
            vh = (
                len(name).to_bytes(2, "big")
                + name
                + bytes([4, 0x04])
                + (0).to_bytes(2, "big")
            )
            cid = b"dev"
            payload = len(cid).to_bytes(2, "big") + cid
            body = vh + payload
            raw = bytes([0x10]) + codec.encode_remaining_length(len(body)) + body
            c.sock.sendall(raw)
            self.assertTrue(c.expect_closed())

    def test_retain_and_qos01_rejected(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=True)
            p = codec.encode_publish("t", 1, b"x", retain=True)
            c.sock.sendall(p)
            self.assertTrue(c.expect_closed())

        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=True)
            # QoS0 PUBLISH：0x30（无 packet id）
            body = b"\x00\x01t" + b"x"
            c.sock.sendall(
                bytes([0x30]) + codec.encode_remaining_length(len(body)) + body
            )
            self.assertTrue(c.expect_closed())

    def test_subscribe_rejected(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dev", clean_session=True)
            body = b"\x00\x01t\x00"  # SUBSCRIBE: topic filter + qos
            c.sock.sendall(
                bytes([0x82]) + codec.encode_remaining_length(len(body)) + body
            )
            self.assertTrue(c.expect_closed())


# ---------------------------------------------------------------------------
# 3. 真实 TCP 拆包与粘连
# ---------------------------------------------------------------------------


class TestFraming(ReceiverTestCase):
    def test_byte_by_byte_connect_and_publish(self):
        """每个字节一次 write：Remaining Length 跨大量读取边界。"""
        with server_process(self.db) as sp:
            c = sp.client()
            c.send_raw(codec.encode_connect("frag", False), delay=0.002)
            ack = c.read_packet()
            self.assertEqual(ack.return_code, 0)

            p = codec.encode_publish("frag/topic", 77, b"fragmented payload")
            rel = codec.encode_pubrel(77)
            c.send_raw(p + rel, delay=0.002)
            c.expect_pubrec(77)
            c.expect_pubcomp(77)
            c.disconnect()
            time.sleep(0.15)

        self.assertEqual(
            inbox_rows(self.db), [("frag", "frag/topic", b"fragmented payload")]
        )

    def test_remaining_length_split_across_reads(self):
        """特意在固定头与 Remaining Length 之间切开。"""
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("rl", False)
            packet = codec.encode_publish("t", 9, b"x" * 300)
            c.send_raw_chunks([packet[0:1], packet[1:3], packet[3:]])
            c.expect_pubrec(9)
            rel = codec.encode_pubrel(9)
            c.send_raw_chunks([rel[0:1], rel[1:2], rel[2:]])
            c.expect_pubcomp(9)

    def test_coalesced_packets(self):
        """多个报文粘连在同一个 TCP 段。"""
        with server_process(self.db) as sp:
            c = sp.client()
            stream = (
                codec.encode_connect("coal", False)
                + codec.encode_publish("a", 1, b"m1")
                + codec.encode_pubrel(1)
            )
            c.sock.sendall(stream)
            ack = c.read_packet()
            self.assertIsInstance(ack, codec.Connack)
            c.expect_pubrec(1)
            c.expect_pubcomp(1)

            stream2 = (
                codec.encode_publish("b", 2, b"m2")
                + codec.encode_pubrel(2)
                + codec.encode_pingreq()
                + codec.encode_disconnect()
            )
            c.sock.sendall(stream2)
            c.expect_pubrec(2)
            c.expect_pubcomp(2)
            pkt = c.read_packet()
            self.assertIs(pkt, codec.PINGRESP)
            time.sleep(0.2)

        self.assertEqual(
            inbox_rows(self.db),
            [("coal", "a", b"m1"), ("coal", "b", b"m2")],
        )

    def test_half_packet_then_rest(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("half", False)
            p = codec.encode_publish("tt", 3, b"0123456789")
            mid = len(p) // 2
            c.sock.sendall(p[:mid])
            time.sleep(0.2)
            c.sock.sendall(p[mid:])
            c.expect_pubrec(3)
            c.pubrel(3)
            c.expect_pubcomp(3)

    def test_malformed_remaining_length_5_bytes_closes(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("bad", True)
            c.sock.sendall(bytes([0x30, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]))
            self.assertTrue(c.expect_closed())

    def test_unknown_packet_type_closes(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("bad", True)
            c.sock.sendall(bytes([0xF0, 0x00]))  # 保留类型 15
            self.assertTrue(c.expect_closed())


# ---------------------------------------------------------------------------
# 4/5. 幂等与重传
# ---------------------------------------------------------------------------


class TestIdempotency(ReceiverTestCase):
    def test_duplicate_publish_resends_pubrec_no_double_delivery(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dup", clean_session=False)
            c.publish_qos2("t", 1, b"once")
            c.expect_pubrec(1)
            c.publish_qos2("t", 1, b"once", dup=True)  # 重发
            c.expect_pubrec(1)
            c.pubrel(1)
            c.expect_pubcomp(1)
            c.disconnect()
            time.sleep(0.15)

        self.assertEqual(inbox_rows(self.db), [("dup", "t", b"once")])

    def test_duplicate_pubrel_no_double_delivery(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dup", clean_session=False)
            c.publish_qos2("t", 1, b"once")
            c.expect_pubrec(1)
            c.pubrel(1)
            c.expect_pubcomp(1)
            c.pubrel(1)  # PUBCOMP 丢失假想：重发 PUBREL
            c.expect_pubcomp(1)
            c.pubrel(1)
            c.expect_pubcomp(1)
            c.disconnect()
            time.sleep(0.15)

        self.assertEqual(inbox_rows(self.db), [("dup", "t", b"once")])

    def test_pubrec_lost_reconnect_resume(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("resume", clean_session=False)
            c.publish_qos2("t", 1, b"resume-me")
            c.expect_pubrec(1)
            c.close()  # 模拟 PUBREC 丢失、未发 PUBREL 即断线
            time.sleep(0.15)

            c2 = sp.client()
            ack = c2.connect("resume", clean_session=False)
            self.assertTrue(ack.session_present)
            c2.publish_qos2("t", 1, b"resume-me", dup=True)
            c2.expect_pubrec(1)
            c2.pubrel(1)
            c2.expect_pubcomp(1)
            c2.disconnect()
            time.sleep(0.15)

        self.assertEqual(inbox_rows(self.db), [("resume", "t", b"resume-me")])
        self.assertEqual(exchange_states(self.db, "resume"), {1: STATE_REL})

    def test_packet_id_reuse_after_complete(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("reuse", clean_session=False)
            c.deliver_qos2("t", 42, b"first")
            c.deliver_qos2("t2", 42, b"second")  # 编号复用
            c.disconnect()
            time.sleep(0.15)

        self.assertEqual(
            inbox_rows(self.db),
            [("reuse", "t", b"first"), ("reuse", "t2", b"second")],
        )

    def test_dup_after_complete_replays_pubcomp_not_redeliver(self):
        """PUBCOMP 丢失后重连同 pid 的 DUP PUBLISH：回 PUBCOMP，不二次入账。"""
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("dc", clean_session=False)
            c.deliver_qos2("t", 42, b"only")
            c.publish_qos2("t", 42, b"only", dup=True)
            pkt = c.read_packet()
            self.assertEqual(pkt.kind, codec.PUBCOMP)
            self.assertEqual(pkt.packet_id, 42)
            c.disconnect()
            time.sleep(0.15)

        self.assertEqual(inbox_rows(self.db), [("dc", "t", b"only")])


# ---------------------------------------------------------------------------
# 6. 会话接管
# ---------------------------------------------------------------------------


class TestTakeover(ReceiverTestCase):
    def test_new_connection_takes_over_old(self):
        with server_process(self.db) as sp:
            old = sp.client()
            old.connect("same", clean_session=False)
            old.publish_qos2("t", 1, b"x")
            old.expect_pubrec(1)

            new = sp.client()
            ack = new.connect("same", clean_session=False)
            self.assertTrue(ack.session_present)
            time.sleep(0.2)

            self.assertTrue(old.expect_closed())

            new.pubrel(1)
            new.expect_pubcomp(1)
            new.disconnect()
            time.sleep(0.2)

        self.assertEqual(inbox_rows(self.db), [("same", "t", b"x")])

    def test_old_connection_cannot_finish_exchange_after_takeover(self):
        """接管后旧连接即使发出 PUBREL 也不能写账。"""
        with server_process(self.db) as sp:
            old = sp.client()
            old.connect("race", clean_session=False)
            old.publish_qos2("t", 1, b"x")
            old.expect_pubrec(1)

            new = sp.client()
            new.connect("race", clean_session=False)
            time.sleep(0.3)  # 确保接管完成、old socket 已 shutdown

            try:
                old.sock.sendall(codec.encode_pubrel(1))
                sent = True
            except OSError:
                sent = False
            self.assertTrue(old.expect_closed() or not sent)

            new.pubrel(1)
            new.expect_pubcomp(1)
            new.disconnect()
            time.sleep(0.2)

        self.assertEqual(inbox_rows(self.db), [("race", "t", b"x")])

    def test_clean_session_takeover_clears_pending(self):
        with server_process(self.db) as sp:
            old = sp.client()
            old.connect("tk", clean_session=False)
            old.publish_qos2("t", 1, b"pending")
            old.expect_pubrec(1)

            new = sp.client()
            ack = new.connect("tk", clean_session=True)
            self.assertFalse(ack.session_present)
            time.sleep(0.2)
            self.assertEqual(exchange_states(self.db, "tk"), {})
            self.assertTrue(old.expect_closed())
            new.disconnect()
            time.sleep(0.15)

        self.assertEqual(inbox_rows(self.db), [])


# ---------------------------------------------------------------------------
# 7/8. 崩溃恢复（os._exit 强杀，重启重放）
# ---------------------------------------------------------------------------


class TestCrashOnPublish(ReceiverTestCase):
    CID = "crashpub"
    PID = 11

    def _run(self, crash_env):
        cid, pid = self.CID, self.PID
        sp = ServerProcess(self.db, env={crash_env: f"{cid}:{pid}"}).start()
        c = sp.client()
        ack = c.connect(cid, clean_session=False)
        self.assertFalse(ack.session_present)

        c.publish_qos2("t", pid, b"durable?")
        code = sp.wait_exit()  # 在落库路径上强杀
        self.assertEqual(code, 91)
        c.close()

        # 重启（关闭注入点）
        sp.start(env_extra={crash_env: ""})
        c2 = sp.client()
        ack = c2.connect(cid, clean_session=False)
        self.assertTrue(ack.session_present)

        if crash_env == CRASH_BEFORE_PUB_COMMIT:
            self.assertEqual(exchange_states(self.db, cid), {})
            c2.publish_qos2("t", pid, b"durable?")
        else:
            self.assertEqual(exchange_states(self.db, cid), {pid: STATE_REC})
            c2.publish_qos2("t", pid, b"durable?", dup=True)

        c2.expect_pubrec(pid)
        c2.pubrel(pid)
        c2.expect_pubcomp(pid)
        c2.disconnect()
        time.sleep(0.15)
        sp.stop()

        self.assertEqual(inbox_rows(self.db), [(cid, "t", b"durable?")])
        self.assertEqual(exchange_states(self.db, cid), {pid: STATE_REL})

    def test_crash_before_publish_commit(self):
        self._run(CRASH_BEFORE_PUB_COMMIT)

    def test_crash_after_publish_commit_before_response(self):
        self._run(CRASH_AFTER_PUB_COMMIT)


class TestCrashOnPubrel(ReceiverTestCase):
    CID = "crashrel"
    PID = 22

    def _run(self, crash_env):
        cid, pid = self.CID, self.PID
        sp = ServerProcess(self.db).start()
        c = sp.client()
        c.connect(cid, clean_session=False)
        c.publish_qos2("t", pid, b"money")
        c.expect_pubrec(pid)

        # 带注入点重启后发 PUBREL
        sp.stop()
        sp = ServerProcess(self.db, env={crash_env: f"{cid}:{pid}"}).start()
        c.close()
        c2 = sp.client()
        ack = c2.connect(cid, clean_session=False)
        self.assertTrue(ack.session_present)
        c2.pubrel(pid)
        code = sp.wait_exit()
        self.assertEqual(code, 91)
        c2.close()

        rows = inbox_rows(self.db)
        states = exchange_states(self.db, cid)
        if crash_env == CRASH_BEFORE_REL_COMMIT:
            self.assertEqual(rows, [])
            self.assertEqual(states, {pid: STATE_REC})
        else:
            # 提交后、PUBCOMP 发出前崩溃：账目与 REL 状态原子可见
            self.assertEqual(rows, [(cid, "t", b"money")])
            self.assertEqual(states, {pid: STATE_REL})

        # 重启重放
        sp.start(env_extra={crash_env: ""})
        c3 = sp.client()
        ack = c3.connect(cid, clean_session=False)
        self.assertTrue(ack.session_present)
        c3.pubrel(pid)
        c3.expect_pubcomp(pid)
        c3.disconnect()
        time.sleep(0.15)
        sp.stop()

        self.assertEqual(inbox_rows(self.db), [(cid, "t", b"money")])
        self.assertEqual(exchange_states(self.db, cid), {pid: STATE_REL})

    def test_crash_before_pubrel_commit(self):
        self._run(CRASH_BEFORE_REL_COMMIT)

    def test_crash_after_pubrel_commit_before_response(self):
        self._run(CRASH_AFTER_REL_COMMIT)


class TestCrashExternalKill(ReceiverTestCase):
    def test_pubcomp_sent_then_sigkill_restart_no_double_delivery(self):
        """入账完成后 SIGKILL：客户端未收到 PUBCOMP，重启重发 PUBREL。"""
        sp = ServerProcess(self.db).start()
        c = sp.client()
        c.connect("killed", clean_session=False)
        c.publish_qos2("t", 3, b"payload")
        c.expect_pubrec(3)
        c.pubrel(3)
        c.expect_pubcomp(3)
        sp.kill(9)  # 响应刚发出后掉电/强杀
        c.close()

        sp.start()
        c2 = sp.client()
        ack = c2.connect("killed", clean_session=False)
        self.assertTrue(ack.session_present)
        self.assertEqual(exchange_states(self.db, "killed"), {3: STATE_REL})
        c2.pubrel(3)  # 客户端视角未完成，重放
        c2.expect_pubcomp(3)
        c2.disconnect()
        time.sleep(0.15)
        sp.stop()

        self.assertEqual(inbox_rows(self.db), [("killed", "t", b"payload")])


# ---------------------------------------------------------------------------
# 只读查询
# ---------------------------------------------------------------------------


class TestReadOnlyQuery(ReceiverTestCase):
    def test_query_mode_ro_blocks_writes(self):
        with server_process(self.db):
            pass
        ro = sqlite3.connect(f"file:{self.db}?mode=ro", uri=True)
        with self.assertRaises(sqlite3.OperationalError):
            ro.execute("INSERT INTO inbox(client_id,topic) VALUES('x','y')")
        ro.close()

    def test_store_list_inbox(self):
        with server_process(self.db) as sp:
            c = sp.client()
            c.connect("q", clean_session=False)
            c.deliver_qos2("t1", 1, b"a")
            c.deliver_qos2("t2", 2, b"b")
            c.disconnect()
            time.sleep(0.15)

        store = Store(self.db)
        try:
            rows = store.list_inbox("q")
            self.assertEqual([r["topic"] for r in rows], ["t1", "t2"])
            self.assertEqual(store.count_inbox(), 2)
            self.assertEqual(store.count_inbox("other"), 0)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
