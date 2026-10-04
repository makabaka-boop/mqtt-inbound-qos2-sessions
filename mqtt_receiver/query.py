"""只读收件查询 CLI。

用法：
    python -m mqtt_receiver.query --db receiver.db list [--client ID] [--limit N]
    python -m mqtt_receiver.query --db receiver.db count [--client ID]
    python -m mqtt_receiver.query --db receiver.db sessions

以只读模式（mode=ro）打开数据库，任何写入尝试都会被 SQLite 直接拒绝。
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys


def _open_ro(path: str) -> sqlite3.Connection:
    # file: URI + mode=ro：从驱动层面保证进程不能写库。
    uri = f"file:{path}?mode=ro"
    return sqlite3.connect(uri, uri=True)


def cmd_list(args) -> int:
    conn = _open_ro(args.db)
    try:
        if args.client is None:
            rows = conn.execute(
                "SELECT id, client_id, topic, payload, exchange_seq, received_at "
                "FROM inbox ORDER BY id ASC LIMIT ?",
                (args.limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, client_id, topic, payload, exchange_seq, received_at "
                "FROM inbox WHERE client_id=? ORDER BY id ASC LIMIT ?",
                (args.client, args.limit),
            ).fetchall()
    finally:
        conn.close()

    for r in rows:
        payload = bytes(r[3])
        if args.hex:
            payload_text = payload.hex()
            encoding = "hex"
        else:
            try:
                payload_text = payload.decode("utf-8")
                encoding = "utf-8"
            except UnicodeDecodeError:
                payload_text = payload.hex()
                encoding = "hex"
        record = {
            "id": r[0],
            "client_id": r[1],
            "topic": r[2],
            "payload": payload_text,
            "payload_encoding": encoding,
            "exchange_seq": r[4],
            "received_at": r[5],
        }
        print(json.dumps(record, ensure_ascii=False))
    return 0


def cmd_count(args) -> int:
    conn = _open_ro(args.db)
    try:
        if args.client is None:
            (n,) = conn.execute("SELECT COUNT(*) FROM inbox").fetchone()
        else:
            (n,) = conn.execute(
                "SELECT COUNT(*) FROM inbox WHERE client_id=?", (args.client,)
            ).fetchone()
    finally:
        conn.close()
    print(n)
    return 0


def cmd_sessions(args) -> int:
    conn = _open_ro(args.db)
    try:
        rows = conn.execute(
            "SELECT client_id, clean_session, created_at, last_seen "
            "FROM sessions ORDER BY client_id"
        ).fetchall()
    finally:
        conn.close()
    for cid, clean, created, last_seen in rows:
        print(f"{cid}\tclean={clean}\tcreated={created}\tlast_seen={last_seen}")
    return 0


def cmd_exchanges(args) -> int:
    """排查用：查看未完成/全部 QoS2 交换（只读）。"""
    conn = _open_ro(args.db)
    try:
        sql = (
            "SELECT exchange_seq, client_id, packet_id, state, topic, "
            "length(payload), updated_at FROM qos2_exchanges"
        )
        params: tuple = ()
        if args.unfinished:
            sql += " WHERE state='REC'"
        if args.client:
            sql += (" WHERE" if "WHERE" not in sql else " AND") + " client_id=?"
            params = (args.client,)
        sql += " ORDER BY exchange_seq"
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()
    for r in rows:
        print("\t".join(str(x) for x in r))
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="MQTT 接收器只读收件查询")
    parser.add_argument("--db", required=True)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_list = sub.add_parser("list", help="列出收件")
    p_list.add_argument("--client")
    p_list.add_argument("--limit", type=int, default=100)
    p_list.add_argument("--hex", action="store_true", help="载荷以 hex 显示")
    p_list.set_defaults(func=cmd_list)

    p_count = sub.add_parser("count", help="收件计数")
    p_count.add_argument("--client")
    p_count.set_defaults(func=cmd_count)

    p_sess = sub.add_parser("sessions", help="列出持久会话")
    p_sess.set_defaults(func=cmd_sessions)

    p_ex = sub.add_parser("exchanges", help="查看 QoS2 交换状态")
    p_ex.add_argument("--client")
    p_ex.add_argument("--unfinished", action="store_true")
    p_ex.set_defaults(func=cmd_exchanges)

    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except sqlite3.OperationalError as exc:
        print(f"查询失败（只读库）: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
