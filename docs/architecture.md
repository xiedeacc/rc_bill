# 架构视图与责任边界

本文件将系统上下文、运行组件、提交时序和目标部署拆开，避免一张图混杂全部抽象层次。源码已实现业务链路；大规模部署图表示容量模型，并不是已部署集群。

## 系统上下文

```mermaid
flowchart LR
    B[内部业务系统及其事务Outbox]
    I[身份平台]
    O[运维人员]
    N[可靠API通知服务]
    P[外部供应商HTTP API]
    Q[共享配额或受控出口平台]
    K[密钥注入平台]
    B -->|已验证身份及通知内容| N
    I -->|JWT公钥或JWKS| N
    O -->|授权查询及审计重放| N
    N -->|稳定业务幂等键| P
    N -->|原子许可及幂等释放| Q
    K -->|进程凭证与数据key| N
    style N fill:#dfefff,stroke:#315d8a,stroke-width:2px
```

本服务不实现业务事件生产、广告归因、CRM联系人状态、库存逻辑、身份签发或配额平台服务端。测试替身用于验证交互和故障处理，不能证明供应商或平台自身正确。

## 容器与持久责任

![容器与可靠性边界](diagrams/architecture.png)

[SVG可缩放版本](diagrams/architecture.svg)。蓝色框是应用运行角色，紫色框是持久基础设施，虚线表示恢复和写回。数据库提交、Kafka确认、offset提交和供应商响应是四种不同的事实。

| 提交点 | 谁拥有后续责任 | 进程此刻退出时 |
| --- | --- | --- |
| API同库COMMIT | notification及Outbox | 客户端同key重试取回原任务；Relay继续发布 |
| Kafka确认但Outbox未标记 | Outbox与Kafka均可能有事件 | 允许重发相同event，按任务代次去重 |
| PG claim提交但offset未提交 | 持久租约 | 消费重读只能识别已有租约，不重复领取 |
| offset提交但HTTP未开始 | 持久租约与恢复器 | UNKNOWN后同事务创建下一代Outbox |
| 供应商已执行但结果未写回 | 外部可能已生效，本地结果未知 | 保持deliveryKey重试；需供应商幂等 |

## 关键时序

```mermaid
sequenceDiagram
    autonumber
    participant C as 调用方
    participant A as API
    participant D as PostgreSQL
    participant R as Relay
    participant K as Kafka
    participant W as Worker
    participant P as 供应商
    C->>A: JWT + Idempotency-Key + 内容
    A->>D: 同分片事务 任务/幂等/密文/Outbox
    D-->>A: COMMIT
    A-->>C: 202 + 查询Location
    R->>D: 短事务领取Outbox
    R->>K: 稳定eventId + scheduleEpoch
    K-->>R: 确认
    R->>D: 按发布租约CAS标记
    K->>W: 调度事件 可重复
    W->>D: 校验目标版本及epoch 持久claim
    D-->>W: leaseToken + STARTED
    W->>K: 推进连续offset
    W->>P: 有界虚拟线程 单次HTTP
    alt 确认成功
        P-->>W: 2xx
        W->>D: CAS任务及attempt为成功
    else 可重试或结果未知
        P-->>W: 429/5xx/连接中断
        W->>D: 同事务状态更新及下一代Outbox
    end
    Note over D,W: 没有任何HTTP在数据库事务中进行
```

## 部署视图

```mermaid
flowchart TB
    G[入口网关与授权调用方]
    subgraph APP[应用层 独立扩容和滚动发布]
        API[API 副本组]
        RELAY[Relay 副本组]
        WORKER[Worker 副本组]
    end
    subgraph DATA[受控数据平台 三可用区目标]
        PG[PG分片 主库与同步副本]
        KC[3个专用KRaft Controller]
        KB[Kafka Broker 跨区RF3]
        KC --- KB
    end
    G --> API
    API --> PG
    PG --> RELAY
    RELAY --> KB
    KB --> WORKER
    WORKER --> PG
    WORKER --> E[受控出口及供应商]
    M[独立迁移和Kafka初始化Job] -.-> DATA
```

本地及CI使用1个PG服务器和同机3个Kafka broker；分片测试使用两个隔离schema及独立数据源。生产目标中的跨AZ、同步复制、64物理主分片与2048分区需要真实环境验收，不能从该图或单机测试推断已达到。

Kubernetes模板默认少量副本，Worker可以先使用all角色，也能拆为relay/dispatch。API readiness避免因一个共享分片故障把所有API副本摘除；受影响请求返回503，总体health与后台恢复日志仍报告依赖异常。后台逐分片处理，单分片失败不会直接跳过其后的健康分片；冷启动仍要求配置与路由校验成功，不声称已实现在线自动搬迁。

## 模块依赖

```mermaid
flowchart LR
    API[api 装配与HTTP] --> APP[application 用例与端口]
    W[worker 装配与调度] --> APP
    AD[adapters PG Kafka HTTP] -.实现端口.-> APP
    APP --> DOMAIN[domain 纯Java规则]
    API -.装配.-> AD
    W -.装配.-> AD
```

纯规则可以不启动Spring测试；数据库语义必须用PG，消息协议必须用Kafka。边界图不替代测试，具体证据见[验证记录](verification.md)。
