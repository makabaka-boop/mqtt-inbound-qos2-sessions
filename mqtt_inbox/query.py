"""只读收件查询：按 ClientId 或全量查看业务收件账。

以 query_only 方式打开 SQLite，绝不写入。
示例：
  python -m mqtt_inbox.query --db inbox.db
  python -m mqtt_inbox.query --db inbox.db --client dev-1 --json
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time


def query(db_path: str, client_id: str | None = None,
          limit: int = 100) -> list[dict]:
    conn = sqlite3.connect(
        f"file:{db_path}?mode=ro", uri=True, timeout=5,
        check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        if client_id is None:
            rows = conn.execute(
                "SELECT id, client_id, topic, payload, packet_id, "
                "delivered_at FROM inbox ORDER BY id LIMIT ?",
                (limit,)).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, client_id, topic, payload, packet_id, "
                "delivered_at FROM inbox WHERE client_id=? ORDER BY id LIMIT ?",
                (client_id, limit)).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r["id"],
                "client_id": r["client_id"],
                "topic": r["topic"],
                "packet_id": r["packet_id"],
                "delivered_at": r["delivered_at"],
                "payload_hex": bytes(r["payload"]).hex(),
                "payload_text": _maybe_utf8(bytes(r["payload"])),
            })
        return out
    finally:
        conn.close()


def _maybe_utf8(raw: bytes) -> str | None:
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return None


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="只读收件账查询")
    p.add_argument("--db", required=True)
    p.add_argument("--client", default=None, help="按 ClientId 过滤")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--json", action="store_true")
    args = p.parse_args(argv)

    items = query(args.db, args.client, args.limit)
    if args.json:
        json.dump(items, sys.stdout, ensure_ascii=False, indent=2)
        sys.stdout.write("\n")
        return 0

    if not items:
        print("(empty)")
        return 0
    for it in items:
        ts = time.strftime("%Y-%m-%d %H:%M:%S",
                           time.localtime(it["delivered_at"]))
        payload = it["payload_text"]
        if payload is None or any(ord(c) < 32 and c not in "\t" for c in payload):
            payload = "0x" + it["payload_hex"]
        print(f"#{it['id']} {ts} client={it['client_id']} "
              f"pid={it['packet_id']} topic={it['topic']} payload={payload!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
