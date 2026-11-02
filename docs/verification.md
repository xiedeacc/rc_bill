# 实际验证记录

本文件区分源码实现、执行结果和生产目标。报告中的失败保留原样，不把mock、skip或仅编译成功当作真实基础设施验证。

## 已完成的独立CI验证

已确认的完整运行基线：`d58d40abc4678d38f20a25088075ca267a7de134`， [CI run 41518168245](https://github.com/xiedeacc/rc_bill/actions/runs/41518168245)。其后修订以[最新证据](https://github.com/xiedeacc/rc_bill/blob/ci-evidence/latest.json)及对应commit为准；不要将一个旧报告自动套在新代码上。

| 检查 | 实际结果 |
| --- | --- |
| JDK21干净构建 | 全部Maven模块通过 |
| 单元/组件用例 | 145个，0失败，0错误，0跳过 |
| 真实基础设施集成用例 | 12个，0失败，0错误，0跳过 |
| HTTP与Kafka契约 | OpenAPI、JSON Schema、负例及文档链接检查通过 |
| 实际API与Worker进程 | runtime启动及业务闭环通过 |
| 功能并发烟测 | 8并发、64请求，64个任务最终SUCCEEDED |
| 容器构建 | API及Worker镜像构建成功，基础镜像固定digest |
| 容器运行 | UID10001、只读根文件系统、受限/tmp下启动检查通过 |

这次64请求烟测的接收P95为927.93ms、总完成耗时7.15秒。此前两次分别为393.21ms和673.53ms。它们均属于共享CI机器上的冷启动功能烟测，**没有证明设计中的P95≤150ms目标，更没有证明10万QPS**。不通过放宽断言或选择更漂亮的一次数字把它包装成容量验收；生产验收应固定环境、预热、重复测量并分解认证、加密、DB及配额RPC耗时。

## 测试实际覆盖的边界

- UUIDv8路由、时间局部性与边界；body UTF-8字节上限、非法Unicode、Header注入/重复/保留名称及指纹规范化。
- 幂等命中先于目标重新校验；同key异指纹冲突；Caller与分页约束。
- HTTP状态分类、UNKNOWN结果、次数/TTL耗尽、Retry-After日期/数字/溢出、抖动上下界。
- 执行名额在创建虚拟线程前取得，任务运行时不提前释放，关闭/拒绝不泄漏名额。
- 公网/私网/链路本地/IPv6/mapped地址、固定控制面origin、显式遗留origin；真实本地HTTP无自动重试、不跟随重定向、有界响应与DNS/请求取消。
- AES-GCM随机化、错误任务AAD、篡改、截断和非法key；凭证引用限制、诊断脱敏、可变buffer清理。
- MyBatis真实参数绑定和单行bytea结果物化；真实PG32并发同key只有一个任务、一个payload和一个Outbox，冲突无孤儿记录。
- 旧租约结果不能覆盖新状态；UNKNOWN恢复产生下一代Outbox；重放审计、幂等与整链活跃约束；按tenant/client隔离读取。
- 重复有界Body清理持续推进；未发布责任阻止元数据清理；目标版本不能被另一进程改写；容量保护不吞掉已有幂等回执。
- 两个隔离schema/数据源的稳定路由及URL重排拒绝；一个分片的故障不会直接跳过后续健康分片的恢复。
- 真实签名JWT API、真实PG、真实Kafka、真实HTTP fixture的完整链路；同消费组offset提交后故意丢弃执行，通过恢复器完成任务。
- 独立迁移命令重复执行、Kafka初始化及保留已有offset；元数据传播等待有界且不重试权限错误。

外部供应商与共享配额平台使用测试替身。测试验证客户端行为及协议，不证明这些外部平台自己的集群一致性。

## Red与Green证据

| 问题 | 失败证据 | 修复与验证 |
| --- | --- | --- |
| 原骨架返回202假接收 | 四个路由期望501但得到202 | 保留scaffold501；runtime另行实现真实提交 |
| 路由、输入与重试策略 | 对应stub未实现时用例失败 | 纯规则与边界用例通过 |
| UUID被当bean读取id属性 | 首次PG CI五个场景失败；本地参数绑定复现五个错误 | `79f6410`显式@Param后通过 |
| byte[]被当多行byte数组 | 第二次PG CI重放失败；MapperProxy最小测试得到空数组 | `b1199b0`单行映射后真实链路通过 |
| Topic创建ACK早于broker可见 | 新增初始化CLI的真实Kafka CI失败 | `839547c`等待可恢复元数据收敛，权限错误不重试 |
| Header凭证与幂等字段冲突 | 五种配置回归先失败 | `f435793`启动时拒绝不安全绑定 |
| 过慢容量探测授权旧快照 | 可替换时间的失败测试 | `d58d40a`失败关闭，并把阻塞探测移出ConcurrentHashMap monitor |

纯策略、输入边界、执行容量等采用先失败测试再实现；部分基础设施接线先实现、后通过真实CI补充框架语义回归。不能把整个过程宣称为完美的全程TDD。首批接线提交也有可进一步拆分之处；本次保留公开历史和失败证据，不通过重写历史掩盖过程。

## 环境与可复现性

本地：Windows、JDK27，编译目标21。独立CI：Ubuntu24.04、Temurin21、PostgreSQL17、三个Kafka4.2.2 broker。PG/Kafka在同一台CI机器上；它们不是三个真实可用区。基础镜像和Action固定摘要/commit，Maven Wrapper固定版本与校验和。

```sh
./mvnw clean verify
./mvnw -Pintegration clean verify
```

第二条必须提供TEST_PG_URL、TEST_PG_USER、TEST_PG_PASSWORD和TEST_KAFKA_BOOTSTRAP，缺失时失败。具体命令及演示见[运行文档](runtime-and-operations.md)。每轮CI的JUnit XML、构建日志、API/Worker日志和烟测结果保存在Actions artifacts及`ci-evidence/runs/<sha>/`。

## 未被本次测试证明的事项

- 没有部署真实生产集群、生产公网入口或真实供应商，也没有发布生产镜像。
- 没有验证64个物理主分片、2048分区、10万QPS、真实跨AZ/区域灾难、备份恢复RPO/RTO。
- 没有宣称JDK25性能收益、绝对HTTP恰好一次、外部配额平台正确性、在线DEK轮换或自动分片搬迁已经实现/验收。
- Kubernetes清单与手动CD需要真实环境secrets、身份、配额、出口规则和环境审批；定义工作流不等于已经部署成功。
- 完整未删改会话zip需要在最终会话结束后导出，当前不能用本文件代替。
