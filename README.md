# MQTT 3.1.1 QoS2 入站接收器

一个**只入站**的 MQTT 3.1.1 接收端：接受客户端的 `CONNECT`、QoS 2 `PUBLISH`、
`PUBREL`、`PINGREQ`、`DISCONNECT`，把恰好一次（exactly-once）语义的消息落进
SQLite 业务收件账（`inbox` 表）。**它不是消息代理**：没有订阅树、不向任何
对端转发消息、无遗嘱、无保留消息。

接收状态机完全手写（`mqtt_receiver/codec.py`），不依赖任何 MQTT broker/客户端库。

## 目录

```
mqtt_receiver/
  codec.py    # 手写 MQTT 3.1.1 编解码 + 流式分包（拆包/粘连）
  store.py    # SQLite：sessions / qos2_exchanges / inbox，事务与崩溃注入点
  server.py   # TCP 入口、QoS2 接收方状态机、会话接管
  query.py    # 只读收件查询 CLI（mode=ro 打开数据库）
tests/
  helpers.py               # 字节级 MQTT 客户端 + 子进程服务器（可强杀重启）
  test_mqtt_receiver.py    # 34 个真实 TCP 端到端测试
  _server.py               # 子进程方式启动接收器（供崩溃测试）
```

## 运行

```bash
# 接收器
python3 -m mqtt_receiver.server --db /path/to/inbox.db --host 0.0.0.0 --port 1883

# 只读查询
python3 -m mqtt_receiver.query --db inbox.db list                 # 全部收件
python3 -m mqtt_receiver.query --db inbox.db list --client shop01
python3 -m mqtt_receiver.query --db inbox.db count
python3 -m mqtt_receiver.query --db inbox.db sessions             # 持久会话
python3 -m mqtt_receiver.query --db inbox.db exchanges --unfinished

# 测试（仅需 Python 3.10+ 标准库，不需要 pytest）
python3 tests/run_tests.py -v
```

## 协议范围

| 方向 | 报文 |
|---|---|
| 收 | `CONNECT`、QoS2 `PUBLISH`、`PUBREL`、`PINGREQ`、`DISCONNECT` |
| 发 | `CONNACK`、`PUBREC`、`PUBCOMP`、`PINGRESP` |

- 仅 **QoS 2** 的 PUBLISH（QoS 0/1/3、RETAIN=1 一律按非法报文断连）；
- **无遗嘱**：Will Flag=1 直接断连；
- 不做 SUBSCRIBE/UNSUBSCRIBE，不接受方向错误的 PUBREC/PUBCOMP；
- 单条载荷上限 **4 KiB**；
- Remaining Length 跨任意读取边界、一个 TCP 段粘连多个报文均正常；
- 无法在 CONNACK 中表达的违例直接关闭连接；可表达的（协议级别、ClientId）
  先回对应返回码再关闭。

## 持久化与恰好一次

三张表：

- `sessions(client_id PK, …)`：CleanSession=0 的持久会话；
- `qos2_exchanges(exchange_seq PK AUTOINCREMENT, client_id, packet_id,
  state CHECK(REC/REL), topic, payload, …)`：QoS2 交换状态；
- `inbox(id PK, client_id, topic, payload, exchange_seq UNIQUE, received_at)`：
  业务收件账。

QoS2 接收方流程与崩溃窗口：

```
PUBLISH ──► BEGIN; INSERT exchange(REC, payload); COMMIT;   ──► PUBREC
                    ▲ 窗口①：提交前/提交后、响应发出前可注入崩溃
PUBREL  ──► BEGIN; UPDATE exchange→REL;
                    INSERT inbox(... exchange_seq); COMMIT; ──► PUBCOMP
                    ▲ 窗口②：同窗口②：同一事务内完成，账目与结束状态原子可见
```

关键规则：

1. **PUBREC 永远在 PUBLISH 落库提交之后**才可能发出；
2. **写收件账与交换状态置 REL 在同一 SQLite 事务**，提交后才发 PUBCOMP；
3. 重复 PUBLISH（REC 态）→ 重发 PUBREC，不覆盖消息；
   重复 PUBREL（REL 态）→ 重发 PUBCOMP，**绝不第二次写 inbox**；
4. PUBREC/PUBCOMP 丢失后断线重连重放：依赖 DUP 标志与交换状态收敛，
   每条消息在 `inbox` 中恰好一行；
5. 交换完成后，同一 Packet Identifier 可被 **DUP=0 的新 PUBLISH 复用**——
   去重单位是单调递增的 `exchange_seq`（`inbox.exchange_seq UNIQUE`），
   而不是永久去重 packet id；
6. **CleanSession=1**：连接时删除该 ClientId 的旧会话与交换状态，连接结束再
   清一次；Session Present 恒为 0。已写入 `inbox` 的业务账不属于协议会话，
   不会被删除。

WAL + `synchronous=FULL`；所有写操作用 `BEGIN IMMEDIATE` 串行化。

## 会话与接管

- 会话以 ClientId 为键；空 ClientId（仅 CleanSession=1 合法）由服务端分配
  唯一内部键（`anon-<uuid>`），匿名连接互不影响；
- 每个 ClientId 一把会话锁。新连接在会话锁内先 `attach`（替换活动连接并
  shutdown 旧 socket），再做会话状态变更；旧连接此后：
  - 阻塞在 `recv` 上会被 `shutdown()` 立即唤醒退出；
  - 进入任何处理路径前 `is_owner()` 检查失败，抛 `ConnectionTaken`；
  - 清理时发现自己不是 owner，**不删除、不修改**任何会话状态；
- CleanSession=0 的未完成交换跨断线与进程重启保留，重连 CONNACK 的
  Session Present 与规范一致。

## 崩溃/重启测试怎么做的

`store.py` 在四个点读取环境变量（值 `client_id:packet_id`），命中即
`os._exit(91)`（不执行 finally、不 flush，等价 SIGKILL/掉电）：

| 环境变量 | 注入点 |
|---|---|
| `MQTT_RECEIVER_CRASH_BEFORE_PUBLISH_COMMIT` | REC 插入事务 COMMIT 前 |
| `MQTT_RECEIVER_CRASH_AFTER_PUBLISH_COMMIT` | REC 提交后、PUBREC 发出前 |
| `MQTT_RECEIVER_CRASH_BEFORE_PUBREL_COMMIT` | inbox+REL 事务 COMMIT 前 |
| `MQTT_RECEIVER_CRASH_AFTER_PUBREL_COMMIT` | 提交后、PUBCOMP 发出前 |

测试以独立**子进程**启动服务器（`tests/_server.py`），在注入点杀死后用同一个
数据库文件重启，客户端重放握手，随后以 `mode=ro` 直读 SQLite 核对：

- 提交前崩溃：账目无该行，状态回退到更早阶段，重放后恰好一行；
- 提交后/响应前崩溃：状态已原子可见，重放只重发确认、不重复入账；
- 每种崩溃后都核对 CONNACK 的 Session Present。

另外还有真实 TCP 行为测试：逐字节发送（每字节独立 `send`）、在固定头/
Remaining Length/报文体边界切块、多条报文粘连一个 TCP 段、半报文停顿后续传、
5 字节非法 Remaining Length、保留报文类型、4096/4097 字节载荷边界等。
