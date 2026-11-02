# Kafka 高流量投递可靠性与部署设计

版本2.0，2026-11-02。本文与[系统设计](system-design.md)共同构成当前基线，替代原先的低容量数据库队列方案。假设持续20,000条通知/秒、峰值100,000条/秒持续10分钟，默认平均重试放大1.05。参数均为压测起点，部署、业务适配、集成与故障测试仍为TODO。

## 1 保证范围

“不多发、不漏发”必须按层定义，不能只靠一组Kafka参数承诺外部HTTP恰好执行一次。

| 层次 | 实际保证 | 不能保证的部分 |
| --- | --- | --- |
| API接收 | 分片PG唯一约束和同事务写任务、幂等、Outbox，202后有持久责任 | 不接受PG提交失败后仅放内存的请求 |
| Producer到Kafka | 显式幂等生产、acks=all、RF=3/minISR=2抑制客户端协议重试重复，保留故障下副本 | 进程重启后的应用主动重发、Outbox重发仍可能产生相同业务事件的多条记录 |
| Kafka到任务状态 | eventId及scheduleEpoch配合任务CAS，重复消费不重复领取当前代次 | 不能保证消费者永远不会再次收到同一记录 |
| Kafka内部消费再生产 | 合适的事务生产者与read_committed可实现Kafka边界内EOS | 不覆盖PG提交、Redis配额、HTTP副作用 |
| Worker到供应商 | 所有重试/重放使用稳定deliveryKey，供应商原子去重时可避免窗口内重复业务效果 | 对方处理成功而响应丢失时，若供应商无幂等接口，无法同时保证绝不漏执行和绝不重复执行 |

选定保证是“已接收任务不因单节点故障静默丢失；重复消息不重复推进同代状态；不确定HTTP结果至少一次重试并记录风险”。TTL/次数耗尽保留终态与审计，不谎称已送达。若产品强制要求某类库存变更绝不重复，该destination必须要求供应商提供持久幂等或可查询的业务请求ID；不满足时不得启用该保证等级。

## 2 接收与事务 Outbox

API不直接承担Kafka发布。一次MyBatis/Spring数据库事务在**同一物理PG分片**写入以下内容：幂等记录、notification、payload、delivery_chain和首个Outbox事件。提交后返回202，Kafka暂时不可用时由Outbox保留；超过Outbox容量或最老年龄门限则停止接受新任务并返回503。不存在“先202再补写Outbox”的窗口。

`outbox_event`字段：`event_id, logical_slot, notification_id, schedule_epoch, event_type, payload, available_at, publish_state, lease_token, lease_until, published_at`。约束`UNIQUE(notification_id,schedule_epoch,event_type)`，event_id稳定不变。事件只含路由和状态索引，不含Body或凭证；预计600B，单条硬上限8KiB。正常消息不携带64KiB通知Body，避免重试在Kafka中反复复制隐私数据。

Relay按分片/固定lane分工，每100ms小批读取到期未发布行，`FOR UPDATE SKIP LOCKED`短事务取得发布租约后提交；**不持数据库锁等待Kafka**。默认每批≤500，每进程共享少量Producer并异步发送，限制未完成send数量及buffer占用。Future收到Kafka成功确认后按Outbox租约CAS标记PUBLISHED；失败保留待发布，指数退避。事务结束与markPublished之间崩溃，允许同eventId再次发布，由消费者业务幂等处理。

Outbox发布租约180s，Producer delivery.timeout=120s，结果写回另留余量；不要将HTTP的60s任务租约拿来管理发布。旧Relay迟到只能CAS失败，不得覆盖新发布状态。发送超时是未知结果，不能判为Kafka一定未写入。

初版采用明确的轮询Outbox，不同时运行Debezium和轮询Relay，避免双发布路径。后续若切CDC，必须另行验收snapshot重复、WAL保留、replication slot膨胀、failover slot、DDL兼容和Connect offset/配置/状态topic的持久性；Kafka Connect EOS也不能替代消费者业务去重。

已发布Outbox保留24h便于重放核对；未发布行不得按时间直接删除。任务全部保留期内可从PG状态生成**相同当前scheduleEpoch**的修复事件，Kafka不是唯一可恢复副本。修复重发不递增epoch，避免使已经排队的合法事件全部失效；真正的重试/延后状态转换才递增epoch。

## 3 消费与提交 offset

Topic事件字段冻结为：`schemaVersion=1, eventId, notificationId, logicalSlot, scheduleEpoch, destinationId, createdAt, expiresAt`。Kafka key使用notificationId，但Relay显式指定`partition=logicalSlot % 2048`，不使用默认字符串key哈希分区器。slot已经按完整幂等身份散列，因此分区不按tenant或destination聚集，避免大客户/热门供应商占满一个分区。初始PG映射为`physicalShard=logicalSlot % 64`，2048是64的整数倍，因此每个dispatch partition只关联一个PG主分片，便于连接池与消费亲和。分区数/slot映射是版本化拓扑，不能独立随意更改。身份、目标和版本必须与PG记录核对，不能将消息里自报tenant当授权证明。

消费采用“**先持久移交责任，再提交offset，HTTP异步执行**”模式，避免将10s外部调用阻塞在Kafka poll线程上：

1. 固定平台线程调用KafkaConsumer，顺序读取小批记录。KafkaConsumer不是线程安全对象，不交给虚拟线程并发调用。
2. 校验事件版本、大小和路由；准备本地执行名额。没有本地名额时pause相关partition，继续poll维持组成员，待有空位再resume，不创建无限虚拟线程等待信号量。
3. 在该通知所在分片执行短事务：若状态PENDING/RETRY_WAIT且scheduleEpoch匹配、到期且TTL充足，则CAS变IN_FLIGHT，写STARTED attempt与60s lease。其他结果必须明确分类：终态/旧epoch/已有有效租约是可确认的重复；PG错误或未知任务不是成功。
4. **只有步骤3事务已提交，或已确认记录被既存持久状态覆盖，才可提交该记录之后的offset**。同partition只推进连续处理完成的前缀。提交失败允许重读，不能回滚已提交PG事务，也不能擅自跳过未处理的前序记录。
5. 将租约交给有界的虚拟线程HTTP执行器。提交executor失败/进程立刻退出时，由持久租约恢复器重新生成Outbox调度，不能依赖Kafka再次消费来恢复已提交offset的工作。
6. HTTP结果通过lease_token CAS写回，任务与attempt、必要的下一次Outbox事件在同一事务提交。HTTP不在PG事务或Kafka事务内。

Spring Kafka规划使用BATCH ack模式配合record listener，每条listener只有完成持久责任移交后返回，一轮poll全部持久移交后再合并提交offset，降低每消息同步提交的往返开销；`enable.auto.commit=false`、`syncCommits=true`、`asyncAcks=false`。监听器不得“提交到内存executor就直接return”。如果以后切异步ack，必须另行实现每partition连续水位和rebalance测试，不能调用最大的offset越过空洞。

已有有效租约的重复消息可以ack，是因为租约记录和恢复器已经持久承担责任。恢复器不是可选优化；未完成/未健康时Worker不得对这种handoff模式宣称ready。租约过期后标UNKNOWN、按预算转换状态和写新Outbox；旧Worker迟到不写回。任务租约不能围栏外部HTTP，重复副作用仍依赖供应商幂等。

限流/熔断导致长期不能发送时，同事务将任务设RETRY_WAIT、更新nextAttemptAt、递增epoch并插入延迟Outbox，再ack消息；不计网络attempt。短时本地满载优先pause，不要每条消息不断生成新的延后事件。没有配额时不先创建STARTED再排队。

## 4 延迟重试与死信

Kafka没有通用原生逐条延迟队列。当前选择PG Outbox的`available_at`控制重试，到期后由Relay写回同一个dispatch topic；PG只扫描小范围到期Outbox和异常恢复索引，正常通知不再由Worker扫描数据库领取。

重试采用系统设计中的指数退避、full jitter、Retry-After、24h TTL和20次网络预算。禁止在consumer里sleep等待下一次重试，禁止无间隔“消费失败→立刻丢回原topic”的热循环。Kafka producer网络重试与业务HTTP重试是两层不同预算：前者可靠发布同事件，后者是持久任务的新调度代次；不能叠加HTTP客户端自动重试。

业务永久失败/耗尽预算：PG同事务写DEAD/EXPIRED、attempt结果及dead事件Outbox，然后发布`notification.dead.v1`供告警/审计。业务状态以PG为准，DLT只是可重建的事件流。人工重放只能走审计API，不允许运维无条件将DLT重新灌入dispatch。

反序列化失败、未知schema、非法路由属于毒消息：先持久隔离到`notification.quarantine.v1`并收到确认，再提交源offset，隔离key为`sourceTopic/partition/offset`。隔离消息自身可能重复，下游按该key去重；隔离失败就不ack。单条最大8KiB及脱敏规则避免把恶意巨型payload搬到DLT。合法event但PG缺失任务应暂停相关slot、告警和对账，不能静默丢弃。

`auto.offset.reset=none`用于运行中的消费组：offset缺失或越过retention应停下告警，禁止自动latest跳过任务。新消费组由部署流程在明确起始水位初始化，不能对新组直接套none后假定能启动。修复流程根据PG中的非终态、当前epoch及租约生成缺失调度，确认恢复后再人工设置offset；清理PG前必须校验恢复与幂等窗口。

## 5 Broker 与 Topic 参数

选型基线Kafka 4.2.2，客户端由Boot BOM解析为4.2.2；上线时固定受支持且通过安全评估的补丁版本及镜像digest，并重复混版本兼容测试。采用KRaft，不新建ZooKeeper部署。[Kafka KRaft部署说明](https://kafka.apache.org/42/operations/kraft/)

| 范围 | 参数及起始值 | 理由与注意事项 |
| --- | --- | --- |
| 控制面 | 3个专用controller，3可用区各1 | 保持多数派；生产不与broker combined部署 |
| 数据面 | 12个broker，3可用区各4 | 故障后留存吞吐和磁盘余量；数量仍需实测 |
| 节点与角色 | 全集群唯一`node.id`；`process.roles=broker`或`controller` | 节点ID、存储身份固定，禁止重建时重复使用别人的数据卷 |
| 控制面发现 | `controller.quorum.bootstrap.servers`列出3个controller的稳定内部DNS与端口 | 按4.2动态quorum流程初始化；不能把broker端口当controller端口 |
| 监听器 | 明确`listeners`、`advertised.listeners`、`controller.listener.names`、`inter.broker.listener.name`与`listener.security.protocol.map` | 客户端、节点间、controller路径分离并全部配置TLS/认证 |
| 数据目录 | `log.dirs`指向专用持久卷，controller元数据盘单独规划 | 禁止容器临时盘承担持久责任；监控文件句柄/segment数/恢复耗时 |
| 放置 | `broker.rack=az-a/az-b/az-c` | 3副本放到不同故障域，验证实际replica assignment |
| 默认副本 | `default.replication.factor=3` | 创建topic仍显式RF=3，不能只依赖默认 |
| 确认副本 | cluster-level `min.insync.replicas=2`，topic同样显式2 | 4.2默认ELR下不在单broker动态覆盖；配合acks=all，ISR不足拒绝写入 |
| Leader选举 | `unclean.leader.election.enable=false` | 不从落后且不合格副本强行选主换可用性；4.2的ELR功能必须按官方语义验收 |
| 自动建topic | `auto.create.topics.enable=false` | 避免拼写错误建成单副本/错误分区数topic |
| Consumer offsets | `offsets.topic.replication.factor=3`，显式检查内部topic minISR=2 | offset也必须耐单节点故障；监控offset提交失败 |
| Kafka事务内部topic | `transaction.state.log.replication.factor=3`，`transaction.state.log.min.isr=2` | 即使当前Outbox不用Kafka事务，也为平台内部事务用途正确配置 |
| dispatch topic | 2048 partitions，RF=3，`cleanup.policy=delete` | 并行分发；不依赖compaction做即时去重 |
| dispatch保留 | `retention.ms=604800000`，`retention.bytes=-1` | 7天窗口，24h TTL之外留恢复时间；容量门禁必须先于磁盘耗尽，不能随意缩retention |
| 日志滚动 | `segment.bytes=1073741824`，`segment.ms=21600000` | 便于清理；retention按segment删除，不是逐记录精确TTL |
| dead topic | 48 partitions，RF=3，7天保留 | 失败事件供运维消费；PG终态元数据30天仍保留 |
| quarantine topic | 48 partitions，RF=3，7天保留 | 限制大小、权限与敏感信息；高流量毒消息触发入口隔离 |
| 消息上限 | topic `max.message.bytes=1048576` | 应用正常事件≤8KiB，broker上限留批次压缩等余量 |
| 副本抓取 | `replica.fetch.max.bytes=2097152`或更高实测值 | 与消息/批次大小联动，不能只调producer上限 |

RF=3、minISR=2、acks=all允许一副本不可用时在满足ISR条件下继续写。acks=all等待当时全部ISR，并非“永远只等2个”。若少于2个合格同步副本，停止确认写入；不能为绿色监控临时降可靠性。[Kafka Broker配置语义](https://kafka.apache.org/42/configuration/broker-configs/)、[Topic配置](https://kafka.apache.org/42/configuration/topic-configs/)

复制确认不等于每条消息在所有磁盘上同步fsync，也不能覆盖所有副本/区域同时永久丢失。可靠性依赖跨故障域副本、可靠持久卷、PG/WAL备份、恢复演练和明确RPO。不要宣传任意灾难下绝对零丢失；跨区域MirrorMaker异步复制也有复制延迟，不能宣称RPO0。

4.2新集群沿用默认启用的ELR：只在ISR之外但被控制器记录为安全的eligible replica中进行合格选举，这不等于允许任意落后副本unclean选主。验收时查询ELR状态；启用ELR后minISR使用cluster/topic配置，调整minISR会清理相关ELR状态，不能无审查反复改参数。[Kafka ELR语义](https://kafka.apache.org/42/operations/eligible-leader-replicas/)

## 6 Producer 参数

| 参数 | 值 | 取舍 |
| --- | --- | --- |
| `enable.idempotence` | `true` | 显式开启，参数冲突时失败而非悄悄失效 |
| `acks` | `all` | 与topic minISR共同决定确认条件 |
| `retries` | `2147483647` | 由delivery.timeout统一限制一次send的总体时间 |
| `max.in.flight.requests.per.connection` | `5` | 4.2基线的幂等约束；无需为保序固定为1而牺牲吞吐 |
| `delivery.timeout.ms` | `120000` | 包含batch等待与请求重试；超时保留Outbox待重发 |
| `request.timeout.ms` | `30000` | 单次请求预算；delivery必须至少覆盖request+linger |
| `max.block.ms` | `5000` | 元数据/缓冲等待有界，超时不丢任务 |
| `linger.ms` | `5` | 小幅聚合批次，低延迟与吞吐折中 |
| `batch.size` | `65536` | 每partition批次上限，实际受流量影响 |
| `buffer.memory` | `67108864` | 64MiB起点，不能用无限内存吸收Kafka故障 |
| `compression.type` | `lz4` | 事件元数据压缩；容量估算保守不计压缩收益 |
| `max.request.size` | `1048576` | 与broker/topic/fetch联合校验 |
| `client.id` | 服务名加稳定实例标识 | 监控/配额归属，不用通知ID |

Producer对象线程安全且应共享，不为每个虚拟线程创建一个Producer。`send()`被调用不表示成功；必须处理异步Future失败，包括认证、超时、RecordTooLarge、消息序列异常。Outbox失败不得伪装为PUBLISHED。当前无`transactional.id`，这是有意选择：一个PG事务与一个Kafka事务顺序提交也不具备原子性，不能替代Outbox。[Kafka Producer配置](https://kafka.apache.org/42/configuration/producer-configs/)

如果未来引入纯Kafka consume-transform-produce链，可采用每个活跃实例唯一的transactional.id前缀、处理fencing、配置transaction.timeout与broker上限、同事务发送输出及offset，消费端read_committed；事务不能跨HTTP等待。不能用同一个transactional.id启动多个活跃producer。Spring的事务同步或两个transaction manager并不形成PG+Kafka全局原子提交。[Spring Kafka EOS边界](https://docs.spring.io/spring-kafka/reference/kafka/exactly-once.html)

## 7 Consumer 参数

| 参数 | 值 | 注意事项 |
| --- | --- | --- |
| `group.id` | `notification-dispatch-v1` | 同一职责共享组；禁止每个pod一个group导致广播 |
| `enable.auto.commit` | `false` | offset由持久handoff完成后推进 |
| `auto.offset.reset` | `none` | 新组预置水位；运行组缺offset停机对账 |
| `isolation.level` | `read_committed` | 不读已中止事务；对非事务重复记录不自动去重 |
| `group.protocol` | `classic` | 本基线明确选classic，不混用新consumer协议的heartbeat配置 |
| `partition.assignment.strategy` | `org.apache.kafka.clients.consumer.CooperativeStickyAssignor` | 减少大范围迁移；所有组员保持兼容 |
| `session.timeout.ms` | `45000` | 故障检测起点，确认在broker允许范围内 |
| `heartbeat.interval.ms` | `3000` | 心跳不代替max.poll间隔；不能据此放心阻塞listener |
| `max.poll.interval.ms` | `300000` | 只包含有界PG移交，不能无限调大隐藏阻塞 |
| `max.poll.records` | `100` | 单轮100次移交，DB慢则暂停而非读更多 |
| `fetch.min.bytes` | `1` | 低流量时不人为攒大包；高流量自然成批 |
| `fetch.max.wait.ms` | `100` | 服务端等待上限起点 |
| `max.partition.fetch.bytes` | `1048576` | 与消息上限匹配；首批特例说明它不是严格进程内存上限 |
| `fetch.max.bytes` | `16777216` | 16MiB起点，结合分区数、consumer数核算内存 |
| `allow.auto.create.topics` | `false` | 同时关闭客户端自动创建 |
| Spring listener concurrency | 每Worker 32个**平台**线程 | 集群总数按2048个partition分配，空闲消费者不能凭空增加分区并行度 |

默认64个Worker×32消费者=2048，平均每poll线程一个partition；Worker另设最多1024个在途HTTP任务，并不等于listener concurrency=1024。按分区lag与handoff延迟扩容；rebalance时停止新的该partition移交，提交已持久完成的连续前缀。已持有PG租约的虚拟线程可在有效期内完成；新owner重读后只能按状态去重，不得无条件再发送。[Kafka Consumer配置](https://kafka.apache.org/42/configuration/consumer-configs/)

分区数不仅取决于字节带宽：若每条同步PG handoff平均10ms，一个poll线程约100条/s，105k/s至少需要1050个活跃poll线程。2048个线程在单AZ损失22个Worker后仍有1344个，理想移交能力约134.4k/s；BATCH offset提交摊薄网络往返，真实容量仍需计入P99、配额判断和连接等待。若移交平均延迟增加到20ms，健康集群也仅约102.4k/s，应背压或优化批量数据库移交，不能盲目调大max.poll.interval。

2048分区带来更多副本、segment、文件句柄、消费者实例和重平衡成本；每broker平均约512个dispatch分区副本。必须压测元数据/启动恢复和内存，不把“分区越多越快”作为原则。Kafka错误处理器不得在固定几次失败后默认跳过记录；只有完成持久隔离或责任移交才允许恢复处理器返回成功。

## 8 PostgreSQL 是否仍然需要

本方案保留PG，是为了提供强一致入口幂等、按ID状态查询、重试计划、租约、重放审计和Outbox原子提交。Kafka offset只代表某partition中的读取位置，不代表外部HTTP业务成功；compacted topic不是实时唯一约束，Producer幂等键不是调用方的Idempotency-Key。

可以设计纯Kafka入口+Streams状态存储/changelog方案，但需要重新解决同步202语义、同key异Body冲突响应、交互式查询路由、状态恢复、去重保留期、跨分区重放等问题。它不是删除PG依赖后的等价替换。本题选择保留分片PG并公开其性能成本；Kafka解决分发和削峰，不能消除接收阶段的数据库吞吐上限。

### 8.1 路由与同分片事务

定义4096个稳定logical slot，`slot = first12bits(SHA256(lengthPrefixed(tenant,client,"submit",idempotencyKey)))`，映射到初始64个物理PG主分片，每个主分片有跨区同步备库及第三副本。按key而不是只按tenant散列，使大租户可分散。`logicalSlot`不是Kafka partition号，两者不能混为一谈。

任务ID采用内部UUIDv8布局，预留12位slot路由字段；公开API仍是opaque UUID，调用者不得解析或把它当授权。完整布局/固定测试向量在ID功能实现时冻结。所有同一任务的幂等、Body、attempt、Outbox和重放子任务均落同slot。重放先从originalId取slot，再在该slot对`replay:<originalId>`作用域去重，不能对replay key重新散列跨库。查询从ID路由，避免64库广播；不存在额外中心“任务ID到分片”热表。

slot映射配置版本化并缓存，变化通过明确迁移协议：冻结该slot新写→排空/转移Outbox与租约→拷贝所有仍有效的幂等及任务数据→一致性核对→原子切换owner epoch→解除写入。旧owner拒绝写，切换间隙可503。禁止直接把`hash % 64`改为`hash % 128`，否则同key路由改变会失去唯一约束。路由哈希算法不可随意改变；有恶意定向key流量时按调用方限额和slot热点保护。

### 8.2 热点与解决边界

| 热点类型 | 问题 | 处理与局限 |
| --- | --- | --- |
| 一个租户大量不同key | 按tenant分片会压满单库 | 按完整幂等身份散列slot，拆散大租户 |
| 同一个key疯狂重试 | 全部请求竞争一个唯一索引/行 | 每pod single-flight、短TTL已完成结果缓存、入口限流；缓存命中仍比对fingerprint与授权。不存在性的缓存不能证明已持久接收 |
| 单通知重复消息风暴 | 大量CAS/租约检查同一行 | 本地有界去重缓存和接收速率保护；PG仍作最终裁决，不能用Bloom filter阳性直接丢通知 |
| 单个provider全局配额行 | 每次HTTP都更新同一PG行，分片无效 | 移除这种设计；使用按provider分区的配额协调、批量短期额度租赁和本地信号量，不把业务幂等移到Redis |
| 热门供应商Kafka key | 全部记录进入一个partition | 使用通知的logicalSlot显式分区，provider配额独立控制；不要求供应商全局有序 |
| 时间递增索引/大JSON更新 | B-tree右侧竞争、WAL与TOAST膨胀 | Body独立不可变表，窄状态行，局部索引，小批归档；加分区不等于增加物理IO能力 |
| 状态机高更新频率 | 磁盘/WAL、autovacuum才是瓶颈 | 按实际事务压测、缩短热表保留、物理分片；MyBatis只提高SQL可控性，不凭空提升硬件容量 |
| 虚拟线程连接风暴 | 数万虚拟线程等待少量DB连接 | DB并发门禁、按分片池预算、短事务；不按线程数增加连接 |

同一个业务key需要串行裁决，不能靠给它加随机盐写进多个分片“解决热点”，这会破坏幂等。若单一合法key流量仍超过承载能力，限流/缓存/coalescing才是正确手段；Kafka不会自动解决该问题。

### 8.3 全局配额单独处理

配额协调按provider account分区，使用Redis原子脚本/外部受控出口的配额功能分配短窗口额度。高QPS目标每Worker每100ms申请一批grant，grant有稳定requestId、额度和过期时间，重复申请返回同grant；本地额度用完或过期才再申请，所有有效grant额度总和受控。并发slot单独租赁，持有期间仍受HTTP deadline约束，未用的速率额度到期作废，不在故障后随意退款。

这会把高QPS供应商每消息一次中央写，降为每Worker每窗口一次中央写；同provider仍有必须串行的预算裁决，是逻辑上无法凭空消除的热点。极高配额可将子预算分配到固定lane，子预算总和不得超过总额。低QPS目标用更小grant避免浪费；目标不健康时隔离该目标，不阻塞其他消息。

Redis故障切换可能丢失最近grant，不能声称异步复制下绝不会超发。检测到不确定epoch时暂停新grant，等待旧grant/并发租约安全失效后恢复；有“硬上限绝不超额”合同的目标由供应商或唯一受控出口执行最终限额。分布式配额/出口本身是外部依赖，本项目实现客户端适配，不实现它们的服务端。

## 9 高流量容量预算

平均20k/s、峰值100k/s、平均Body2KiB、正常尝试系数1.05。以下是未计压缩的估算，不是实测性能。

| 项目 | 估算 |
| --- | --- |
| 日通知量 | 17.28亿 |
| 峰值原始Body入口 | 195.3MiB/s；若全为64KiB则6.1GiB/s，必须另限字节 |
| 7天Body | 22.53TiB，另加活跃任务，Body单独加密存储和清理 |
| 30天元数据 | 518.4亿任务；按任务+索引1KiB约48.28TiB |
| 30天幂等记录 | 按384B/条约18.10TiB |
| 30天尝试记录 | 544.32亿条；按512B/条约25.35TiB |
| PG原始合计 | 约114.26TiB，不含Outbox/额外副本/WAL/膨胀/临时空间 |
| PG可用磁盘预算 | 按2倍余量约228.5TiB主数据容量；64分片每主约3.57TiB，两个完整副本另乘3，备份另计 |
| dispatch Kafka日原始量 | 20k×1.05×86400×600B≈0.99TiB |
| dispatch 7天RF3 | 约20.79TiB，未含dead/quarantine、segment/index和故障余量 |
| Kafka部署起点 | 12broker×4TiB可用盘≈48TiB；一可用区故障剩32TiB，恢复副本/磁盘余量必须实测 |
| 峰值Kafka入流 | 105k×600B≈60.1MiB/s逻辑写，RF3总副本写约180.2MiB/s，不含消费者读取与协议开销 |
| partition峰值 | 105k/2048≈51.3条/s/partition，正常元数据很小，瓶颈更可能是DB handoff或供应商 |

PG峰值事务下限：接收100k/s + 领取105k/s + 写回105k/s ≈310k事务/s，另有Outbox发布批次领取/标记、重复请求、重放、归档。均匀落64分片约4844事务/s/主分片；要求单分片在真实索引、同步复制、加密Body、清理并发下压测至少约6500这类事务/s并满足P99，不能用空表TPS证明。达不到就增物理分片或降低接收预算，不能假设加Kafka已解决DB瓶颈。

在途HTTP≈105k/s×0.2s=21,000；2倍余量42,000。64个Worker×1024=65,536，损失单AZ最多22个实例后仍有43,008。平均延迟恶化到2s则需210,000在途，当前预算只能约32.8k尝试/s，必须熔断、隔离和积压，不能无限创建虚拟线程。初始64个Worker每个8vCPU/8GiB，48个API每个4vCPU/4GiB，均是压测起点。

接收前DB并发预算按Little定律估算：100k/s×10ms≈1000个同时持有事务的连接；每分片约16（平均时延假设），另有Worker/Relay事务及尾延迟。每PG主初始max_connections=300，PgBouncer backend budget≤200，保留≥100给运维、迁移和故障缓冲；8个API接入路由单元和8个Worker路由单元共享每分片预算，不能48个pod各给64个分片建10连接而产生30,720连接。

连接路由单元是本服务进程的部署分组，不是新业务系统。API分8个路由组，每组拥有8个主分片，6个pod/组；接收按key计算slot，查询按ID计算slot，若请求落错组则经带身份校验的内部转发路由，不能随机改slot。每pod每分片小池起点4，API约24连接/主分片。Worker仅对已分配partition对应的PG分片懒创建池，每池4；2048个consumer至多2048条分片关联，均衡上界约128连接/主分片。rebalance后待旧租约处理结束再回收闲置池，恢复期间也受PgBouncer全局预算约束。Relay/恢复器额外预留预算；不依赖每pod连接所有64数据库。池尺寸与路由策略须在实际事务尾延迟下校准，不能只满足平均值。

2小时平均速率全目标故障会产生1.44亿任务，原始Body约274.7GiB；Outbox/重试不能放内存。Kafka最大lag年龄、Outbox最老未发布年龄和磁盘余量比一个全局`count(*)`更适合作背压依据：初始Outbox年龄>60s软拒绝新流量，>5分钟或预计12h耗尽磁盘硬保护，Kafka最老未处理>24h警报/TTL对账，达到retention前必须修复。阈值要按故障预算调整，已接受任务继续保留责任。

## 10 部署与运维 TODO

1. KRaft controller使用独立持久卷、独立资源和反亲和；统一cluster.id，一次初始化，禁止重启脚本重新format存储。broker与controller设置独立监听器，校验advertised.listeners从客户端与其他broker均可达。
2. 客户端使用`security.protocol=SASL_SSL`、经平台批准的`sasl.mechanism`（例如SCRAM-SHA-512）与`ssl.endpoint.identification.algorithm=https`，或采用双向TLS；truststore/凭证从密钥平台注入，不提交JAAS密码。API不直连broker；Relay只具备dispatch/dead写权限，Worker只读对应topic/group及隔离写权限，运维建topic独立账号。禁用不必要的Delete/Alter权限。
3. 每AZ副本数、ISR、offline partitions、unclean election、UnderMinIsrPartitionCount、磁盘、请求延迟、网络与GC进入监控。Producer error/retry、Outbox年龄、consumer lag年龄、offset commit失败、重平衡次数和UNKNOWN尝试单独告警。
4. 滚动升级一次只动一个故障域内少量broker，等待ISR恢复再继续；controller保留多数派。先验证协议兼容，再提升metadata.version；不可假定提升feature level之后还能任意降级。
5. 分区增加会改变默认key映射并破坏旧新partition之间的顺序。当前CAS/epoch不依赖Kafka顺序，但有序目标必须新topic或明确分区映射迁移，不能直接扩partition后声称顺序不变。
6. 按磁盘与网络对reassignment和副本恢复限速，持续留出单AZ失效后的副本重建余量。`retention.bytes`按partition计算，不能误当全topic总额度。扩大集群优先于临时缩短retention。
7. 毒消息、认证失败、缺失offset、PG状态缺失均要有停止与人工对账路径。关闭自动静默恢复器的“默认跳过”。恢复备份后先暂停出站，再核对幂等窗口和可能已成功的HTTP。
8. 集成/故障验收包括：发送成功但ACK丢失、发布后未markPublished就杀Relay、持久handoff后未提交offset就杀consumer、offset提交后未执行HTTP就杀Worker、rebalance中继续完成旧租约、供应商成功后写回失败、ISR从3降2降1、磁盘满、offset过期、slot迁移和重复重放。要求task/attempt/Outbox不变量与审计结果同时正确。

本阶段不交付可直接上生产的Kubernetes清单。以上部署参数是可审查设计，必须通过真实集群与失效演练；README会分别说明“已加入依赖”和“实际集成已完成”的区别。

## 11 虚拟线程与 Spring Cloud

Java虚拟线程适合本服务大量等待HTTP/数据库I/O的调用栈，Spring Cloud应用可以通过底层Spring Boot启用。它降低等待时占用的平台线程成本，不提高供应商QPS、DB连接数或CPU密集计算能力。Boot配置为：

```yaml
spring:
  threads:
    virtual:
      enabled: true
  main:
    keep-alive: true
```

MVC请求处理和Boot自动配置的任务执行器可使用虚拟线程；并非所有第三方库线程都会自动改变。Kafka poll/container固定用平台线程；HTTP执行器未来采用独立的virtual-thread-per-task executor并在提交**之前**申请有限执行名额。名额满则背压，不把几百万个虚拟线程当队列。应用保留Java21编译兼容，生产优先评估JDK25 LTS以利用JDK24起消除普通synchronized阻塞pinning的改进；JDK21路径必须单独测pinning，不用JDK27测试代替。[Boot虚拟线程配置](https://docs.spring.io/spring-boot/reference/features/spring-application.html#features.spring-application.virtual-threads)、[JDK24改进](https://docs.oracle.com/en/java/javase/24/migrate/significant-changes-jdk-24.html)

Spring Kafka当前官方文档仍对并发listener使用虚拟线程给出谨慎建议，因此显式保持平台poll线程。JDK24改进monitor pinning，不会自动修复KafkaConsumer线程安全、锁竞争或异步offset提交协议；不要把二者混为一谈。[Spring Kafka线程安全说明](https://docs.spring.io/spring-kafka/reference/kafka/thread-safety.html)

MyBatis底层仍经JDBC/Hikari访问PG。数据库事务在一个虚拟线程中开启和关闭，不能在`@Transactional`方法里启动子线程后指望自动继承同一事务/SqlSession。SqlSessionTemplate由Spring管理，不能共享裸SqlSession到多个任务。ThreadLocal/MDC/安全身份需要显式上下文传播和finally清理；每个虚拟线程缓存大对象会造成巨大内存放大。

Resilience4j采用有界SemaphoreBulkhead，避免再套固定ThreadPoolBulkhead使虚拟线程退化成等待另一层池；熔断不是重试。真实HTTP客户端必须在连接、响应和总deadline处中止I/O，Future超时不一定停止底层请求；释放并发许可前确认请求已结束或标记不确定。持久重试仍由Outbox调度，不用虚拟线程sleep数分钟/数小时充当定时器。

验收指标：固定相同出口QPS与并发上限对比平台/虚拟线程模式的吞吐、P99、CPU、堆内存、活跃连接、JFR pinning/阻塞和carrier占用；测试响应2s/10s、DB池耗尽、Kafka重平衡与优雅停机。只有达到受控并发下的改善才扩大使用，不承诺开启一个开关就获得倍数性能。
