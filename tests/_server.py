"""独立进程启动接收器（供崩溃/重启测试使用）。

用法: python tests/_server.py <db_path> <port_file> [--host H] [--port P]

固定端口默认 0（由内核分配）；真正监听端口写入 port_file，测试读出后连接。
崩溃注入点通过环境变量传入（见 store.py 文档）。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mqtt_receiver.server import TcpServer  # noqa: E402
from mqtt_receiver.store import Store  # noqa: E402


def main() -> int:
    args = sys.argv[1:]
    db_path = args[0]
    port_file = args[1]
    host = "127.0.0.1"
    port = 0
    i = 2
    while i < len(args):
        if args[i] == "--host":
            host = args[i + 1]
            i += 2
        elif args[i] == "--port":
            port = int(args[i + 1])
            i += 2
        else:
            i += 1

    store = Store(db_path)
    server = TcpServer(store, host=host, port=port)
    bound_host, bound_port = server.address
    # 原子发布监听地址
    tmp = port_file + ".tmp"
    with open(tmp, "w") as f:
        f.write(f"{bound_host}:{bound_port}")
    os.replace(tmp, port_file)

    import logging

    logging.basicConfig(
        level=os.environ.get("MQTT_TEST_LOGLEVEL", "WARNING"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # os._exit 崩溃注入绕 finally；正常路径由父进程 SIGTERM 结束。
    # serve_forever 自身以 0.5s 超时轮询 stop 事件，只需保持主线程存活。
    server.serve_forever()


if __name__ == "__main__":
    raise SystemExit(main())
