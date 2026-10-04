"""真实 TCP 集成测试。

- 服务器以独立子进程运行（python -m mqtt_inbox.server）；
- 对端手工发字节，按 1 字节/任意边界拆包，或多报文粘连一次发送；
- 通过 MQTT_INBOX_CRASH 在提交前后、响应发出前 os._exit(137) 强杀，
  重启后重放握手，核对收件账与 Session Present。
"""

import os
import tempfile
import time
import unittest

from mqtt_inbox import framing
from mqtt_inbox.storage import Storage

from tests._peer import MqttPeer
from tests._server import ServerProcess, free_port


class TcpTestBase(unittest.TestCase):

    crash = ""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "inbox.db")
        self.port = free_port()
        self.server = ServerProcess(self.db_path, crash=self.crash,
                                    port=self.port)
        self.server.start()
        self.peers: list[MqttPeer] = []

    def tearDown(self):
        for p in self.peers:
            p.close()
        if getattr(self, "server", None):
            self.server.terminate()

    def peer(self) -> MqttPeer:
        p = MqttPeer("127.0.0.1", self.server.port)
        self.peers.append(p)
        return p

    def restart(self, crash: str | None = None) -> None:
        self.server.terminate()
        for p in self.peers:
            p.close()
        self.peers.clear()
        self.server = ServerProcess(
            self.db_path,
            crash=crash if crash is not None else self.crash,
            port=self.port)
        self.server.start()

    def db(self) -> Storage:
        s = Storage(self.db_path)
        self.addCleanup(s.close)
        return s


# ---------------------------------------------------------------------------
# 基本会话
# ---------------------------------------------------------------------------

class SessionTests(TcpTestBase):

    def test_connect_session_present_lifecycle(self):
        p = self.peer()
        sp, code = p.connect("dev-1", clean=True)
        self.assertEqual(code, 0)
        self.assertFalse(sp)
        p.disconnect()

        p2 = self.peer()
        sp, code = p2.connect("dev-1", clean=False)
        self.assertEqual(code, 0)
        self.assertFalse(sp)  # 上次 clean=1，无旧会话
        p2.disconnect()

        p3 = self.peer()
        sp, code = p3.connect("dev-1", clean=False)
        self.assertEqual(code, 0)
        self.assertTrue(sp)  # clean=0 会话已保留
        p3.disconnect()

        p4 = self.peer()
        sp, code = p4.connect("dev-1", clean=True)
        self.assertFalse(sp)  # 3.1.2-22：clean=1 时 SP 必须为 0

    def test_will_connect_refused_and_closed(self):
        p = self.peer()
        raw = framing.encode_connect("c", clean_session=True, will_flag=True,
                                     will_topic="t", will_message=b"bye")
        p.raw(raw)
        sp, code = p.expect_connack()
        self.assertEqual(code, framing.CONNACK_SERVER_UNAVAILABLE)
        self.assertFalse(sp)
        self.assertTrue(p.expect_closed())

    def test_empty_clientid_refused(self):
        p = self.peer()
        var = (framing._encode_utf8("MQTT") + bytes([4, 0x02, 0, 0])
               + framing._encode_utf8(""))
        p.raw(bytes([framing.CONNECT << 4, len(var)]) + var)
        _sp, code = p.expect_connack()
        self.assertEqual(code, framing.CONNACK_IDENTIFIER_REJECTED)
        self.assertTrue(p.expect_closed())

    def test_bad_protocol_name_no_connack_closed(self):
        p = self.peer()
        var = (framing._encode_utf8("MQIsdp") + bytes([4, 0x02, 0, 0])
               + framing._encode_utf8("c"))
        p.raw(bytes([framing.CONNECT << 4, len(var)]) + var)
        # 协议名不符：可以 CONNACK 0x01
        _sp, code = p.expect_connack()
        self.assertEqual(code, framing.CONNACK_UNACCEPTABLE_PROTOCOL_VERSION)

    def test_first_packet_not_connect_closed(self):
        p = self.peer()
        p.raw(framing.encode_pingreq())
        self.assertTrue(p.expect_closed(timeout=5))

    def test_second_connect_closed(self):
        p = self.peer()
        p.connect("c", clean=True)
        p.raw(framing.encode_connect("c", clean_session=True))
        self.assertTrue(p.expect_closed())

    def test_ping(self):
        p = self.peer()
        p.connect("c", clean=True)
        p.ping()
        p.ping()
        p.disconnect()


# ---------------------------------------------------------------------------
# QoS 2 交付
# ---------------------------------------------------------------------------

class Qos2DeliveryTests(TcpTestBase):

    def _qos2(self, pid, payload=b"hello", topic="a/b"):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish(topic, payload, pid)
        p.expect_pubrec(pid)
        p.pubrel(pid)
        p.expect_pubcomp(pid)
        return p

    def test_happy_path_inbox_once(self):
        p = self._qos2(1)
        db = self.db()
        self.assertEqual(db.inbox_count("dev", 1), 1)
        item = db.query_inbox("dev")[0]
        self.assertEqual(item.topic, "a/b")
        self.assertEqual(item.payload, b"hello")
        p.disconnect()

    def test_duplicate_publish_not_redelivered(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"first", 10)
        p.expect_pubrec(10)
        # PUBREC 丢失场景：客户端 DUP=1 重发同一 PUBLISH
        p.publish("t", b"first-again", 10, dup=True)
        p.expect_pubrec(10)
        p.pubrel(10)
        p.expect_pubcomp(10)
        db = self.db()
        self.assertEqual(db.inbox_count("dev", 10), 1)
        self.assertEqual(db.query_inbox("dev")[0].payload, b"first")

    def test_duplicate_pubrel_not_redelivered(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"x", 11)
        p.expect_pubrec(11)
        p.pubrel(11)
        p.expect_pubcomp(11)
        # PUBCOMP 丢失：重放 PUBREL，必须只回 PUBCOMP，不再入账
        p.pubrel(11)
        p.expect_pubcomp(11)
        self.assertEqual(self.db().inbox_count("dev", 11), 1)

    def test_packet_id_reuse_after_completion(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"first", 20)
        p.expect_pubrec(20)
        p.pubrel(20)
        p.expect_pubcomp(20)
        # 同一 PacketId 的新消息（DUP=0）：可再次交付
        p.publish("t", b"second", 20)
        p.expect_pubrec(20)
        p.pubrel(20)
        p.expect_pubcomp(20)
        db = self.db()
        self.assertEqual(db.inbox_count("dev", 20), 2)
        payloads = [i.payload for i in db.query_inbox("dev")]
        self.assertEqual(payloads, [b"first", b"second"])

    def test_old_dup_publish_after_done_is_ignored_for_delivery(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"old", 21)
        p.expect_pubrec(21)
        p.pubrel(21)
        p.expect_pubcomp(21)
        # 旧交换的 DUP=1 重传（理论上客户端不该这样做，但服务器必须稳健）：
        # 回 PUBREC，后续 PUBREL 只回 PUBCOMP，不新增账目
        p.publish("t", b"stale", 21, dup=True)
        p.expect_pubrec(21)
        p.pubrel(21)
        p.expect_pubcomp(21)
        self.assertEqual(self.db().inbox_count("dev", 21), 1)

    def test_4kib_payload_accepted(self):
        p = self._qos2(30, payload=b"z" * 4096, topic="bin")
        db = self.db()
        self.assertEqual(len(db.query_inbox("dev")[0].payload), 4096)
        p.disconnect()

    def test_retained_publish_closed(self):
        p = self.peer()
        p.connect("dev", clean=True)
        p.publish("t", b"x", 1, retain=True)
        self.assertTrue(p.expect_closed())

    def test_qos0_publish_closed(self):
        p = self.peer()
        p.connect("dev", clean=True)
        p.publish("t", b"x", 0, qos=0)
        self.assertTrue(p.expect_closed())

    def test_subscribe_closed(self):
        p = self.peer()
        p.connect("dev", clean=True)
        sub = framing.encode_pubrel(1)  # 借用；下面手工构造 SUBSCRIBE
        body = b"\x00\x01" + b"\x00\x01t\x00"
        p.raw(MqttPeer.raw_packet((framing.SUBSCRIBE << 4) | 0x02, body))
        self.assertTrue(p.expect_closed())


# ---------------------------------------------------------------------------
# TCP 拆包 / 粘连（真实网络边界）
# ---------------------------------------------------------------------------

class FramingOverTcpTests(TcpTestBase):

    def test_byte_at_a_time_connect(self):
        p = self.peer()
        p.chunked([framing.encode_connect("slow", clean_session=True)], 1)
        sp, code = p.expect_connack()
        self.assertEqual(code, 0)

    def test_byte_at_a_time_publish(self):
        p = self.peer()
        p.connect("dev", clean=False)
        pub = framing.encode_publish("t", b"fragmented-message", 40)
        p.chunked([pub], 1)
        p.expect_pubrec(40)
        p.chunked([framing.encode_pubrel(40)], 1)
        p.expect_pubcomp(40)
        self.assertEqual(self.db().inbox_count("dev", 40), 1)

    def test_random_boundaries(self):
        import random
        rng = random.Random(42)
        p = self.peer()
        p.connect("dev", clean=False)
        for pid in range(100, 105):
            pub = framing.encode_publish("t", f"msg{pid}".encode(), pid)
            pubrel = framing.encode_pubrel(pid)
            blob = pub + pubrel  # 客户端会先等 PUBREC，这里不能预发 pubrel
            # 只拆 PUBLISH；PUBREL 等 PUBREC 后发
            pos = 0
            while pos < len(pub):
                step = rng.randint(1, 3)
                p.raw(pub[pos:pos + step])
                pos += step
            p.expect_pubrec(pid)
            p.raw(pubrel)
            p.expect_pubcomp(pid)
        self.assertEqual(len(self.db().query_inbox("dev")), 5)

    def test_two_publishes_coalesced(self):
        p = self.peer()
        p.connect("dev", clean=False)
        # 粘连两个 PUBLISH：服务器必须都处理，分别回 PUBREC
        pub1 = framing.encode_publish("t", b"one", 50)
        pub2 = framing.encode_publish("t", b"two", 51)
        p.coalesced([pub1, pub2])
        p.expect_pubrec(50)
        p.expect_pubrec(51)
        p.coalesced([framing.encode_pubrel(50), framing.encode_pubrel(51)])
        p.expect_pubcomp(50)
        p.expect_pubcomp(51)
        items = self.db().query_inbox("dev")
        self.assertEqual([i.payload for i in items], [b"one", b"two"])

    def test_multibyte_remaining_length_fragmented(self):
        # 载荷 4000 字节 -> Remaining Length 为两字节，且两字节分两次到达
        p = self.peer()
        p.connect("dev", clean=False)
        payload = b"L" * 4000
        pub = framing.encode_publish("t", payload, 60)
        # 固定首字节、RL 首字节先发
        p.raw(pub[:2])
        time.sleep(0.05)
        p.raw(pub[2:])
        p.expect_pubrec(60)
        p.pubrel(60)
        p.expect_pubcomp(60)
        self.assertEqual(len(self.db().query_inbox("dev")[0].payload), 4000)

    def test_malformed_remaining_length_closed(self):
        p = self.peer()
        p.connect("dev", clean=True)
        # 4 个续位字节无终止
        p.raw(bytes([0x30, 0x80, 0x80, 0x80, 0x80]))
        self.assertTrue(p.expect_closed())


# ---------------------------------------------------------------------------
# 崩溃 / 重启 / 重放
# ---------------------------------------------------------------------------

class CrashBeforePendingCommitTests(TcpTestBase):
    crash = "before_pending_commit"

    def test_publish_crash_before_commit_then_replay(self):
        p = self.peer()
        sp, code = p.connect("dev", clean=False)
        self.assertEqual(code, 0)
        p.publish("t", b"crash-before-commit", 1)
        # 进程在 pending 事务提交前被杀，PUBREC 永远不会发出
        self.assertTrue(p.expect_closed(timeout=8))
        code_rc = self.server.proc.wait() if self.server.proc else 137
        self.assertEqual(code_rc, 137)
        # 回收管道，避免 ResourceWarning
        for stream in (self.server.proc.stdout, self.server.proc.stderr):
            try:
                stream.close()
            except OSError:
                pass
        self.server.proc = None

        # 重启：无 pending 残留（事务未提交），Session Present 仍为 1（会话在）
        self.restart(crash="")  # 重启后不再注入崩溃
        p2 = self.peer()
        sp, code = p2.connect("dev", clean=False)
        self.assertEqual(code, 0)
        self.assertTrue(sp)

        db = self.db()
        self.assertIsNone(db.flow_state("dev", 1))
        # 重放原始 PUBLISH（DUP=1 也可，这里按首次重试）-> 正常交付一次
        p2.publish("t", b"crash-before-commit", 1)
        p2.expect_pubrec(1)
        p2.pubrel(1)
        p2.expect_pubcomp(1)
        self.assertEqual(db.inbox_count("dev", 1), 1)
        self.assertEqual(db.query_inbox("dev")[0].payload,
                         b"crash-before-commit")


class CrashAfterPendingCommitTests(TcpTestBase):
    crash = "after_pending_commit"

    def test_pubrec_lost_after_crash_replay_idempotent(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"persisted-then-kill", 2)
        # pending 已提交，PUBREC 发出前进程被杀
        self.assertTrue(p.expect_closed(timeout=8))

        self.restart(crash="")
        db = self.db()
        # 待交付消息跨重启保留
        self.assertEqual(db.flow_state("dev", 2), "pending")
        self.assertEqual(db.inbox_count("dev", 2), 0)

        p2 = self.peer()
        sp, code = p2.connect("dev", clean=False)
        self.assertEqual(code, 0)
        self.assertTrue(sp)
        # 客户端没收到 PUBREC -> DUP=1 重发，不得重复交付
        p2.publish("t", b"persisted-then-kill", 2, dup=True)
        p2.expect_pubrec(2)
        p2.pubrel(2)
        p2.expect_pubcomp(2)
        self.assertEqual(db.inbox_count("dev", 2), 1)


class CrashBeforePubrelCommitTests(TcpTestBase):
    crash = "before_pubrel_commit"

    def test_pubrel_crash_before_ledger_txn_replay(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"ledger-atomic", 3)
        p.expect_pubrec(3)
        p.pubrel(3)
        # 入账事务提交前被杀：无 PUBCOMP
        self.assertTrue(p.expect_closed(timeout=8))

        self.restart(crash="")
        db = self.db()
        self.assertEqual(db.flow_state("dev", 3), "pending")
        self.assertEqual(db.inbox_count("dev", 3), 0)

        p2 = self.peer()
        sp, code = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        # 重放 PUBREL：入账与状态结束同一事务，仅交付一次
        p2.pubrel(3)
        p2.expect_pubcomp(3)
        self.assertEqual(db.inbox_count("dev", 3), 1)
        self.assertEqual(db.flow_state("dev", 3), "done")

        # 再重放一次 PUBREL：仍然不重复入账
        p2.pubrel(3)
        p2.expect_pubcomp(3)
        self.assertEqual(db.inbox_count("dev", 3), 1)


class CrashAfterPubrelCommitTests(TcpTestBase):
    crash = "after_pubrel_commit"

    def test_pubcomp_lost_after_crash_replay(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"committed-then-kill", 4)
        p.expect_pubrec(4)
        p.pubrel(4)
        # 入账事务已提交，PUBCOMP 发出前被杀
        self.assertTrue(p.expect_closed(timeout=8))

        self.restart(crash="")
        db = self.db()
        self.assertEqual(db.inbox_count("dev", 4), 1)
        self.assertEqual(db.flow_state("dev", 4), "done")

        p2 = self.peer()
        sp, code = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.pubrel(4)
        p2.expect_pubcomp(4)  # 重放拿到 PUBCOMP
        self.assertEqual(db.inbox_count("dev", 4), 1)  # 绝不重复交付


class PendingSurvivesNetworkDropTests(TcpTestBase):

    def test_unfinished_exchange_survives_disconnect(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("t", b"network-drop", 5)
        p.expect_pubrec(5)
        # 没收 PUBREL 就断线（模拟掉电/拔网线）
        p.close()
        time.sleep(0.3)

        self.restart(crash="")  # 服务也重启
        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.publish("t", b"network-drop", 5, dup=True)
        p2.expect_pubrec(5)
        p2.pubrel(5)
        p2.expect_pubcomp(5)
        self.assertEqual(self.db().inbox_count("dev", 5), 1)


# ---------------------------------------------------------------------------
# 会话接管 / CleanSession=1
# ---------------------------------------------------------------------------

class TakeoverTests(TcpTestBase):

    def test_new_connection_takes_over_old(self):
        old = self.peer()
        sp, _ = old.connect("dev", clean=False)
        self.assertFalse(sp)

        new = self.peer()
        sp, _ = new.connect("dev", clean=False)
        self.assertTrue(sp)  # 接管已有会话

        # 旧 socket 被 shutdown：读得到 EOF
        self.assertTrue(old.expect_closed(timeout=5))

        # 旧连接尝试 PUBLISH 也无法改变会话（写入走 epoch 校验，
        # 即便 socket 层竞态中还有残留写入，也会被存储层拒绝）
        try:
            old.publish("t", b"from-old", 1)
        except OSError:
            pass
        # 新连接完成一次正常交付
        new.publish("t", b"from-new", 1)
        new.expect_pubrec(1)
        new.pubrel(1)
        new.expect_pubcomp(1)
        items = self.db().query_inbox("dev")
        self.assertEqual([i.payload for i in items], [b"from-new"])

    def test_clean1_takeover_wipes_unfinished_exchange(self):
        old = self.peer()
        old.connect("dev", clean=False)
        old.publish("t", b"unfinished", 2)
        old.expect_pubrec(2)

        new = self.peer()
        sp, code = new.connect("dev", clean=True)
        self.assertEqual(code, 0)
        self.assertFalse(sp)
        db = self.db()
        self.assertIsNone(db.flow_state("dev", 2))

        new.publish("t", b"fresh", 9)
        new.expect_pubrec(9)
        new.pubrel(9)
        new.expect_pubcomp(9)
        self.assertEqual(db.inbox_count("dev", 9), 1)

    def test_clean1_clears_session_present_on_reconnect(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.disconnect()
        p2 = self.peer()
        sp, _ = p2.connect("dev", clean=False)
        self.assertTrue(sp)
        p2.disconnect()
        p3 = self.peer()
        p3.connect("dev", clean=True)
        p3.disconnect()
        p4 = self.peer()
        sp, _ = p4.connect("dev", clean=False)
        self.assertFalse(sp)


# ---------------------------------------------------------------------------
# Keepalive
# ---------------------------------------------------------------------------

class KeepaliveTests(TcpTestBase):

    def test_server_disconnects_after_1_5x_keepalive(self):
        p = self.peer()
        p.connect("dev", clean=True, keepalive=1)
        # 不发任何报文；服务器应在 1.5s 左右关闭
        self.assertTrue(p.expect_closed(timeout=5))

    def test_ping_resets_keepalive(self):
        p = self.peer()
        p.connect("dev", clean=True, keepalive=1)
        for _ in range(3):
            time.sleep(1.0)
            p.ping()
        p.disconnect()


# ---------------------------------------------------------------------------
# 只读查询
# ---------------------------------------------------------------------------

class QueryTests(TcpTestBase):

    def test_query_cli_readonly(self):
        p = self.peer()
        p.connect("dev", clean=False)
        p.publish("a/b", b"payload-A", 1)
        p.expect_pubrec(1)
        p.pubrel(1)
        p.expect_pubcomp(1)

        from mqtt_inbox.query import query
        items = query(self.db_path)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["topic"], "a/b")
        self.assertEqual(items[0]["payload_text"], "payload-A")
        self.assertEqual(items[0]["client_id"], "dev")
        items = query(self.db_path, client_id="other")
        self.assertEqual(items, [])


if __name__ == "__main__":
    unittest.main()
