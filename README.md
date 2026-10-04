# MQTT 3.1.1 入站 QoS 2 接收器 → SQLite 业务收件账

这不是消息代理（broker）。它是一个 **MQTT 3.1.1 入站接收器**：接受设备通过 TCP
发来的 QoS 2 PUBLISH，按照完整的 QoS 2 四次握手把消息 **恰好一次（exactly-once）**
写入 SQLite 中的业务“收件账”（`inbox` 表）。不支持订阅、不对外转发、不实现遗嘱与
保留消息。接收状态机全部手工实现，**没有使用任何 MQTT broker / client 库**。

## 能力范围

| MQTT 报文 | 入站（客户端→服务端） | 出站（服务端→客户端） |
|---|---|---|
| CONNECT / CONNACK | ✅ 解析与校验（Session Present 正确置位） | ✅ |
| PUBLISH | ✅ **仅 QoS 2**，单条载荷 ≤ 4 KiB | — |
| PUBREC | —（客户端发 PUBREC 属非法，关连接） | ✅ |
| PUBREL / PUBCOMP | ✅ PUBREL | ✅ PUBCOMP |
| PINGREQ / PINGRESP | ✅ | ✅（1.5×Keep Alive 超时关连接） |
| DISCONNECT | ✅ | — |
| SUBSCRIBE / 遗嘱 / Retain / QoS0,1 | ❌ 拒绝/关连接 | — |

TCP 帧处理：
- Remaining Length 1–4 字节变长解码，**跨 recv 边界**读取；
- 报文体读满所需字节数，天然支持 **任意字节边界的拆包**；
- 内部接收缓冲自然切分 **多报文粘连**；
- 任何非法报文（保留位、非法标志、QoS3、通配主题、零 Packet Identifier、
  超长帧等）直接关闭 TCP 连接。

## 运行

```bash
python3 -m mqtt_inbox.server --db /var/lib/mqtt-inbox/inbox.db --host 0.0.0.0 --port 1883
```

启动成功后标准输出一行 `LISTENING <host> <port>`（供测试/编排抓取）。

只读查询收件账：

```bash
python3 -m mqtt_inbox.query --db inbox.db                 # 全量
python3 -m mqtt_inbox.query --db inbox.db --client dev-1  # 按 ClientId
python3 -m mqtt_inbox.query --db inbox.db --json          # JSON
```

查询以 SQLite `mode=ro` 打开，绝不写入。

## 数据模型（SQLite，WAL + synchronous=FULL）

- `sessions(client_id PK, clean, epoch, connected)`：协议会话。
- `qos2_flows(client_id, packet_id, state, topic, payload, updated_at)`：
  QoS 2 交换状态，主键 `(client_id, packet_id)`。
  - `pending`：PUBLISH 已在 **PUBREC 之前**持久化，等待 PUBREL；
  - `done`：交换完成的**墓碑**，用于重复 PUBREL / DUP=1 重传去重。
- `inbox(id, client_id, topic, payload, packet_id, delivered_at)`：业务收件账，
  只增不删（CleanSession 不影响它）。

## Exactly-once 与崩溃安全

关键时序（均在提交事务之后才允许发送响应）：

```
PUBLISH ──► [事务: upsert qos2_flows pending + 载荷] ──COMMIT──► PUBREC
PUBREL  ──► [事务: INSERT inbox + UPDATE qos2_flows SET done] ──COMMIT──► PUBCOMP
```

- **PUBREC 之前**待交付消息已落盘：崩溃/断线/重启后交换仍在；
- PUBREL 的 **业务账写入与交换状态结束在同一个 SQLite 事务**，不会出现
  “入了账但状态没结束（重启后重复入账）”或反过来的中间态；
- 重复 PUBLISH（pending 中重传 / done 后 DUP=1 重发）不覆盖载荷、不重复入账，
  仍重发 PUBREC；
- 重复/重放 PUBREL 不重复入账，仍重发 PUBCOMP（MQTT 3.1.1 §4.2.2/§4.3.3）；
- 完成交换后同一 Packet Identifier **可用于新消息**（新 PUBLISH DUP=0 顶替墓碑，
  可再次交付），不会永久去重该编号；
- `CleanSession=1`：CONNECT 时单事务清除旧协议会话与全部 QoS2 交换状态；
  断线时删除会话；CONNACK 的 Session Present 必为 0；
- `CleanSession=0`：未完成交换跨断线与**服务重启**保留；重连 Session Present=1。

崩溃注入（仅测试）：环境变量 `MQTT_INBOX_CRASH` 取
`before_pending_commit` / `after_pending_commit` /
`before_pubrel_commit` / `after_pubrel_commit`，
命中点进程立即 `_exit(137)`（在提交前/后、响应发出前）。

## 会话接管与 epoch

同名 ClientId 新连接到来时：先 `shutdown()` 旧 socket，再在单事务内将该会话的
`epoch` 加一。所有写事务（pending 落盘、PUBREL 入账、断线标记）都带
`WHERE client_id=? AND epoch=?` 条件，旧连接即便存在竞态写入，也会因 epoch 失效
被拒绝（`SessionTakenOver`），**不能继续改变会话**。

## 测试

真实 TCP、真实子进程、真实 SIGKILL 重启；测试对端也是手工字节（非 broker 客户端）：

```bash
python3 -m unittest discover -s tests -v
```

覆盖：
- Remaining Length 多字节跨边界、5 字节非法长度、超帧拒绝；
- CONNECT 各拒绝码、空 ClientId、遗嘱、保留位、首报文非 CONNECT、二次 CONNECT；
- QoS 2 正常交付、重复 PUBLISH/PUBREL 不重复交付、PacketId 完成后复用；
- 1 字节拆包、随机边界拆包、两报文粘连、4 KiB 载荷、多字节 RL 分两次到达；
- **四个崩溃点强杀进程 → 重启 → 重放握手**，核对账目与 Session Present；
- 未完成交换跨网络掉线 + 服务重启保留；
- 同名接管后旧连接不能改账；CleanSession=1 清除旧协议会话；
- Keepalive 1.5 倍超时断连、PING 保活；只读查询。

## 代码结构

```
mqtt_inbox/
  framing.py   # MQTT 3.1.1 帧读写/编解码（手工状态机，零依赖）
  storage.py   # SQLite 会话/交换状态/收件账，事务边界与 epoch 守卫
  server.py    # TCP 入口、每连接线程、会话注册表、主分发循环
  query.py     # 只读收件查询 CLI（mode=ro）
tests/
  _peer.py     # 手工 MQTT 对端（任意拆包/粘连/非法字节）
  _server.py   # 子进程服务器管理（含崩溃注入重启）
  test_framing.py / test_storage.py / test_tcp.py
```
