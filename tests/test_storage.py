"""存储层测试：会话、QoS 2 状态机、同事务入账、epoch 接管、清理。"""

import os
import tempfile
import unittest

from mqtt_inbox.storage import SessionTakenOver, Storage


class StorageTestBase(unittest.TestCase):

    def setUp(self):
        fd, self.path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        os.unlink(self.path)
        self.db = Storage(self.path)

    def tearDown(self):
        self.db.close()
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(self.path + suffix)
            except FileNotFoundError:
                pass


class SessionLifecycleTests(StorageTestBase):

    def test_first_clean_connect_no_session_present(self):
        epoch, sp = self.db.begin_connect("c1", clean=True)
        self.assertFalse(sp)
        self.assertEqual(epoch, 1)

    def test_clean0_persists_and_sp_on_reconnect(self):
        epoch1, sp1 = self.db.begin_connect("c1", clean=False)
        self.assertFalse(sp1)
        self.db.mark_disconnected("c1", epoch1, clean=False)

        db2 = Storage(self.path)
        epoch2, sp2 = db2.begin_connect("c1", clean=False)
        self.assertTrue(sp2)
        self.assertEqual(epoch2, epoch1 + 1)
        db2.close()

    def test_clean1_clears_old_qos_state(self):
        epoch, _ = self.db.begin_connect("c1", clean=False)
        self.db.save_pending("c1", epoch, 9, "t", b"a", dup=False)
        self.db.finish_on_pubrel("c1", epoch, 9)
        self.db.mark_disconnected("c1", epoch, clean=False)

        db2 = Storage(self.path)
        _, sp = db2.begin_connect("c1", clean=True)
        self.assertFalse(sp)
        self.assertIsNone(db2.flow_state("c1", 9))
        # 业务账不受 CleanSession 影响
        self.assertEqual(db2.inbox_count("c1", 9), 1)
        db2.close()

    def test_inbox_survives_clean1_disconnect(self):
        epoch, _ = self.db.begin_connect("c1", clean=True)
        self.db.save_pending("c1", epoch, 3, "t", b"x", dup=False)
        self.db.finish_on_pubrel("c1", epoch, 3)
        self.db.mark_disconnected("c1", epoch, clean=True)
        self.assertEqual(self.db.inbox_count("c1", 3), 1)


class Qos2FlowTests(StorageTestBase):

    def _open(self, cid="c1", clean=False):
        epoch, _ = self.db.begin_connect(cid, clean=clean)
        return epoch

    def test_happy_path_delivers_once(self):
        ep = self._open()
        self.assertEqual(
            self.db.save_pending("c1", ep, 1, "t", b"hello", False), "stored")
        self.assertEqual(self.db.flow_state("c1", 1), "pending")
        self.assertEqual(self.db.inbox_count("c1", 1), 0)
        self.assertEqual(self.db.finish_on_pubrel("c1", ep, 1), "delivered")
        self.assertEqual(self.db.flow_state("c1", 1), "done")
        self.assertEqual(self.db.inbox_count("c1", 1), 1)
        items = self.db.query_inbox("c1")
        self.assertEqual(items[0].payload, b"hello")

    def test_duplicate_publish_pending_is_idempotent(self):
        ep = self._open()
        self.db.save_pending("c1", ep, 1, "t", b"first", False)
        action = self.db.save_pending("c1", ep, 1, "t", b"second", dup=True)
        self.assertEqual(action, "repeat")
        self.db.finish_on_pubrel("c1", ep, 1)
        self.assertEqual(self.db.inbox_count("c1", 1), 1)
        self.assertEqual(self.db.query_inbox("c1")[0].payload, b"first")

    def test_duplicate_pubrel_does_not_redeliver(self):
        ep = self._open()
        self.db.save_pending("c1", ep, 1, "t", b"x", False)
        self.assertEqual(self.db.finish_on_pubrel("c1", ep, 1), "delivered")
        # PUBCOMP 丢失，客户端重放 PUBREL
        self.assertEqual(self.db.finish_on_pubrel("c1", ep, 1), "repeat")
        self.assertEqual(self.db.inbox_count("c1", 1), 1)

    def test_dup_publish_after_done_does_not_replace(self):
        ep = self._open()
        self.db.save_pending("c1", ep, 1, "t", b"old", False)
        self.db.finish_on_pubrel("c1", ep, 1)
        # 旧交换的重传（DUP=1）不得覆盖/入账
        action = self.db.save_pending("c1", ep, 1, "t", b"retransmit", dup=True)
        self.assertEqual(action, "repeat")
        self.assertEqual(self.db.finish_on_pubrel("c1", ep, 1), "repeat")
        self.assertEqual(self.db.inbox_count("c1", 1), 1)
        self.assertEqual(self.db.query_inbox("c1")[0].payload, b"old")

    def test_packet_id_reuse_after_completion(self):
        ep = self._open()
        self.db.save_pending("c1", ep, 1, "t1", b"first", False)
        self.db.finish_on_pubrel("c1", ep, 1)
        # 完成交换后同 PacketId 用于新消息（DUP=0）：必须可再交付一次
        action = self.db.save_pending("c1", ep, 1, "t2", b"second", dup=False)
        self.assertEqual(action, "stored")
        self.assertEqual(self.db.finish_on_pubrel("c1", ep, 1), "delivered")
        self.assertEqual(self.db.inbox_count("c1", 1), 2)
        payloads = [i.payload for i in self.db.query_inbox("c1")]
        self.assertEqual(payloads, [b"first", b"second"])

    def test_pubrel_unknown_pid_no_delivery(self):
        ep = self._open()
        self.assertEqual(self.db.finish_on_pubrel("c1", ep, 77), "repeat")
        self.assertEqual(self.db.inbox_count("c1", 77), 0)

    def test_pending_survives_disconnect_and_restart(self):
        ep = self._open()
        self.db.save_pending("c1", ep, 5, "t", b"persisted", False)
        self.db.mark_disconnected("c1", ep, clean=False)

        db2 = Storage(self.path)
        self.assertEqual(db2.flow_state("c1", 5), "pending")
        ep2, sp = db2.begin_connect("c1", clean=False)
        self.assertTrue(sp)
        db2.finish_on_pubrel("c1", ep2, 5)
        self.assertEqual(db2.inbox_count("c1", 5), 1)
        db2.close()

    def test_pending_clean1_disconnect_is_dropped(self):
        ep = self._open(clean=True)
        self.db.save_pending("c1", ep, 5, "t", b"x", False)
        self.db.mark_disconnected("c1", ep, clean=True)
        self.assertIsNone(self.db.flow_state("c1", 5))


class EpochTakeoverTests(StorageTestBase):

    def test_old_epoch_cannot_mutate(self):
        ep_old, _ = self.db.begin_connect("c1", clean=False)
        self.db.begin_connect("c1", clean=False)  # 同名接管 -> 新 epoch
        with self.assertRaises(SessionTakenOver):
            self.db.save_pending("c1", ep_old, 1, "t", b"x", False)
        with self.assertRaises(SessionTakenOver):
            self.db.finish_on_pubrel("c1", ep_old, 1)
        with self.assertRaises(SessionTakenOver):
            self.db.mark_disconnected("c1", ep_old, clean=False)

    def test_takeover_clean1_wipes_old_flows(self):
        ep_old, _ = self.db.begin_connect("c1", clean=False)
        self.db.save_pending("c1", ep_old, 2, "t", b"old", False)
        # 同名连接以 CleanSession=1 接管：旧交换状态清除
        self.db.begin_connect("c1", clean=True)
        self.assertIsNone(self.db.flow_state("c1", 2))


if __name__ == "__main__":
    unittest.main()
