# M6 异步与事件化（Kafka 流水线）—— 开发指引与验收文档

> 周期 W11–W12 ｜ 主轨 C（债务阶段 1）
> 一句话目标：**接口从「等 90 秒」变成「100ms 返回」，流水线各步可独立扩容、故障不丢任务。**

---

## 1. 目标与范围

**做**：arq → Kafka 迁移（topic 规划 + 事件契约文档）、分区键设计、幂等消费、重试 + 死信、SSE 进度推送、消费位点/堆积指标、worker 无状态横向扩容。

**不做**：Kafka 集群多副本（单机 KRaft 单节点起步）、Kafka Streams 复杂流处理、事件溯源全量改造（audit topic 只做投递）。

---

## 2. 技术选型详述

### 2.1 Kafka 4.x（KRaft）vs 继续 arq（ADR-0004 补记录）

- **为什么迁 Kafka**：① M1–M5 的 arq（Redis 队列）够用但**面试考点全在 Kafka**——分区/副本/消费者组/offset/堆积；② 批量审查削峰（夜间 10 万份）需要可持久化、可回放的消息通道；③ 事件契约（schema）是团队协作与企业集成的通用语言。
- **KRaft**：4.0 起去 Zookeeper，单节点内存减半（堆 ≤1G），32G 单机可负担。
- **Python 客户端**：`confluent-kafka`（librdkafka，性能与稳定性优于 kafka-python；asyncio 用 `AsyncConsumer` 模式或线程桥接）。M6 起后端仍用 arq 的地方：**短平快的定时任务保留 arq cron**（如超时催办），长流水线走 Kafka——两者并存，ADR 里写清边界。
- **回滚预案（诚实优先）**：Kafka 吃内存到撑不住 → 保留 arq，把 Kafka 设计写成 ADR + 压测预案。**诚实比硬撑有价值。**

### 2.2 Topic 与分区设计

| Topic | 生产者 | 消费者（组） | 分区键 |
|---|---|---|---|
| `contract.parse` | API | parse-worker 组 | `contract_id` |
| `contract.extract` | parse-worker | extract-worker 组 | `contract_id` |
| `contract.review` | extract-worker | review-worker 组 | `contract_id` |
| `contract.notify` | 各阶段 | notify 服务 | `contract_id` |
| `audit.events` | API/worker | 审计归档 | `trace_id` |
| `contract.dlq.*` | 重试耗尽自动 | 人工/告警 | — |

- **分区键选 `contract_id`**：同一合同的消息进同一分区 → 分区内严格有序 → 「解析完成 → 抽取开始」不会乱序。跨合同并行靠多分区。
- **消费者组按阶段划分**：每阶段独立扩容（OCR 慢就多开 parse worker，LLM 慢就多开 review worker）。

### 2.3 幂等消费（至少一次投递的必然代价）

- Kafka 默认语义 = **至少一次**：重复投递不可避免（rebalance、offset 未提交就崩溃）→ 消费端必须幂等：`processed_events(event_id PRIMARY KEY)` 去重表，处理前先插（冲突则跳过）。
- event_id = 业务级幂等源（`contract_id + stage + 输入 hash`），不是消息 UUID（重投的 UUID 会变）。

### 2.4 重试与死信（DLQ）

指数退避（重试 topic `*.retry.5s/30s` 或消费端 sleep 后 seek）+ 最大次数 → 死信 topic + 告警。**死信不是垃圾场**：每条死信必须有人工处理闭环（runbook 列入 M8）。

### 2.5 SSE（进度推送，替代轮询）

- FastAPI `StreamingResponse` + `text/event-stream`：订阅 Redis pub/sub（worker 完成一步 publish 一次），断线自动重连（SSE 原生 `Last-Event-ID`）。
- 为什么不用 WebSocket：进度推送是单向流，SSE 更简单、走 HTTP、代理友好。

---

## 3. 详细设计（伪代码级）

### 3.1 事件契约（版本化 schema，向后兼容）

```json
// contract.parse 事件 v1
{
  "event_id": "contract_id+parse+input_sha",     // 业务幂等键
  "event_version": 1,
  "contract_id": "...",
  "object_key": "contracts/.../x.pdf",
  "source_format": "pdf_scan",
  "trace_id": "...",                              // OTel 贯穿
  "occurred_at": "2026-10-25T02:00:00Z"
}
// 兼容规则：只加可选字段（minor）；改语义/删字段 = v2 新 topic，双写过渡
```

### 3.2 生产者（API 侧）

```python
async def submit_contract(file, user, idem_key):
    contract = await save_and_upload(file, user)          # <50ms：MinIO + INSERT
    await kafka.produce("contract.parse", ParseEvent(contract), key=contract.id)
    return {"task_id": trace_id, "contract_id": contract.id}, 202   # 同步段结束
```

### 3.3 消费者（worker 侧）

```python
async def consume_parse(msg):
    event = ParseEvent.model_validate_json(msg)
    if await processed_repo.exists(event.event_id):       # 幂等：处理过直接跳过
        await consumer.commit(msg); return
    try:
        result = await pipeline.parse(event)              # M1 逻辑原样复用
        await kafka.produce("contract.extract", ExtractEvent.from(result), key=event.contract_id)
        await progress.publish(event.contract_id, step="parse", pct=33)
        await processed_repo.insert(event.event_id)       # 成功才记，失败不记
        await consumer.commit(msg)                        # 处理+落库成功后才提交位点
    except RecoverableError as e:                         # 网络/超时 → 重试
        await retry_or_dlq(msg, e, backoff=exp(5s,30s,max=5))
    except PoisonError as e:                              # 数据问题（重试无意义）→ 直接 DLQ
        await dlq.send(msg, reason=e); alert.fire("dlq_growth")
```

### 3.4 容量与故障参数（单机 32G 约束下的精打细算）

```
Kafka KRaft：KAFKA_HEAP_OPTS=-Xmx1g -Xms1g；单 broker，副本因子 1（单机无意义）；
topic 保留 7 天；日志段 1G。
worker：docker compose --scale parse-worker=2 --scale review-worker=3（按需扩）。
session.timeout=45s（单机抖动容忍大些），max.poll.interval ≥ 最长单条处理时间（OCR 可能 3min）。
```

### 3.5 SSE 进度

```python
@router.get("/tasks/{contract_id}/progress")
async def progress_stream(contract_id: str):
    async def gen():
        pubsub = redis.pubsub(); await pubsub.subscribe(f"progress:{contract_id}")
        async for m in pubsub.listen():
            if m.type == "message": yield f"data: {m.data}\n\n"
            if await task_done(contract_id): yield "event: done\ndata: {}\n\n"; break
    return StreamingResponse(gen(), media_type="text/event-stream")
```

### 3.6 可观测钩子（M8 全面展开，这里先埋）

- 消费位点 lag、组内 rebalance 次数、DLQ 增速 → Prometheus exporter（`kafka-exporter`）。
- 每条事件消费耗时、重试次数 → structlog + trace。

---

## 4. 生产复杂性应对（M6 核心章）

| 真实情况 | 处置 |
|---|---|
| worker 被 kill -9 | 消息未 commit → 其他 worker 重新消费（至少一次语义）；幂等去重保证业务不重 |
| 同一事件投递 3 次 | `processed_events` 幂等表挡住（验收：3 次投递业务数据不重） |
| 批量审查 1 万条堆积 | 扩 worker（`--scale`）+ 分区数 ≥ worker 数；堆积量指标告警 |
| rebalance 风暴（worker 频繁重启） | session.timeout 调大；消费逻辑必须能在 rebalance 后幂等续跑 |
| 消息乱序（同合同两阶段倒挂） | 分区键 contract_id 保证分区内有序；消费端按 contract 状态机二次校验（乱序事件丢弃+告警） |
| 事件 schema 演进破坏旧消费者 | 只加可选字段；破坏性变更走新 topic + 双写灰度；契约文档是唯一事实源 |
| 死信堆积没人管 | DLQ 增速告警 + runbook 处理流程（M8）；演示必须包含"一条死信被处理闭环" |
| Kafka 内存拖垮单机 | KRaft 单节点 + 堆 ≤1G；实在不行回退 arq（诚实预案，ADR 记录） |

---

## 5. 测试计划

- 集成（testcontainers Kafka）：全流水线消费、重复投递幂等、poison message 进 DLQ、kill worker 后任务接管。
- 混沌演练（Docker）：`kill -9` worker 容器 → 观察接管与最终完成率。
- 压力预演：脚本灌 1 万事件 → 消化时长记录（M7 正式压测的输入）。
- 契约测试：event schema 兼容性检查（新 schema 反序列化旧数据）。

---

## 6. 验收文档（DoD）

- [ ] 上传接口 P95 < 100ms（Kafka 异步后）（证据：压测/日志）
- [ ] `kill -9` 一个 worker，任务被其他 worker 接管，最终完成率 100%（证据：混沌演练记录）
- [ ] 同一事件重复投递 3 次，业务数据不重复（证据：幂等测试）
- [ ] 消息堆积 1 万条时，扩 worker 后 5 分钟内消化完（证据：测试记录）
- [ ] 事件 schema 变更向后兼容（新增字段可选，旧消费者不受影响）（证据：契约测试）
- [ ] SSE 进度推送断线可重连（证据：E2E 或手动演示）
- [ ] 事件契约文档（schema + 版本规则）+ 运维手册（堆积处理）完成

**演示脚本**：上传 → 立即返回 task_id → 页面进度条实时推进 → 中途 kill worker → 进度继续。

**Gate 回写区**：G1 ☐ / G2 ☐ / G3 ☐（P95 数字回写）/ G4 ☐。

---

## 7. 风险与回滚

| 风险 | 缓解 / 回滚 |
|---|---|
| Kafka 资源撑爆单机 | 堆 ≤1G + topic 精简；回退 arq（ADR 记录设计不浪费） |
| 迁移期双队列不一致 | 灰度：新合同走 Kafka、存量走 arq；对比监控一周后切换 |
| 至少一次语义被误当精确一次 | 幂等表是硬要求，代码评审 checklist 项 |
| max.poll.interval < 单条处理时间 → 反复 rebalance | 参数按最慢任务（OCR 3min）设定 + 监控 rebalance 次数 |
