# 运行部署与故障处置

本文件描述实际运行入口及外部依赖契约。生产部署清单是需要环境适配的模板；没有自动创建生产数据库、身份平台、密钥平台、配额服务或公网域名。

## 1 运行模式

| 模式 | 作用 | 基础设施 |
| --- | --- | --- |
| scaffold | 保留原骨架演示，业务接口501，readiness DOWN | 无外部依赖，只允许loopback |
| runtime API | JWT校验、持久接收、状态/attempt查询、审计重放 | PostgreSQL、目标配置、加密key、JWT公钥或JWKS |
| runtime Worker all | Relay、Kafka消费、HTTP投递、恢复与清理 | PG、Kafka、目标配置、密钥注入及共享配额接口 |
| runtime Worker relay | 只发布Outbox并恢复发布租约、清理 | PG和Kafka，不装配HTTP/供应商密钥/配额客户端 |
| runtime Worker dispatch | 消费、执行、恢复HTTP租约 | PG、Kafka、目标配置、密钥和配额 |

只启用一个Spring profile：`scaffold`或`runtime`。Worker运行角色用`NOTIFICATION_ROLE=all|relay|dispatch`选择。应用进程与迁移/初始化命令分离，生产不要让所有副本同时承担DDL。

## 2 本地可复现演示

准备JDK21或受支持的更新版本、Python和Docker Compose。Windows可使用Maven Wrapper的`.cmd`版本；下列Python辅助脚本同时支持Windows/Linux。测试基础设施只绑定loopback。

```sh
./mvnw clean verify
python -m venv .venv
# Linux/macOS
.venv/bin/pip install -r requirements-dev.txt
# Windows替换为 .venv\Scripts\pip.exe
docker compose -f compose.test.yaml up -d --wait
.venv/bin/python scripts/prepare-demo.py
.venv/bin/python scripts/run-demo.py migrate
.venv/bin/python scripts/run-demo.py bootstrap
```

然后在三个终端分别运行：

```sh
.venv/bin/python scripts/mock-external-services.py
.venv/bin/python scripts/run-demo.py api
.venv/bin/python scripts/run-demo.py worker
```

`prepare-demo.py`在被Git忽略的`tmp/demo`中创建临时RSA密钥、1小时JWT、加密key和配置。再次执行只刷新JWT，保留已有密钥和环境配置，不会悄悄更换数据加密key。它不是生产身份平台。

执行功能并发烟测：

```sh
.venv/bin/python scripts/load-smoke.py --requests 64 --concurrency 8
```

脚本要求所有请求持久接收并最终SUCCEEDED，同时输出接收P95和总耗时。这只是小规模功能压力检查，不能当成十万QPS证明。脚本仅允许loopback目标，避免误向真实库存/CRM批量发通知。

手动请求示例，先读取`tmp/demo/token.txt`中的测试JWT：

```http
POST /v1/notifications
Authorization: Bearer <demo token>
Idempotency-Key: demo-registration-001
Content-Type: application/json

{"destinationId":"demo-supplier","destinationVersion":1,"contentType":"text/plain","body":"hello","ttlSeconds":60}
```

返回202与Location后，用同一JWT GET该Location查询状态。重复同key同内容返回同一ID，改Body返回409。`http://127.0.0.1:9909/stats`显示mock接收次数和不同幂等键的业务效果数量，不显示Body。

模拟“对方处理成功但连接断开”：在第一次启动API前，将demo目标URL设为`http://127.0.0.1:9909/notify?mode=commit-and-drop`。已注册的目标不能覆盖同一版本，需新增版本并在请求中指定新版本。mock会首次产生效果后不给响应，再次请求用同key返回成功。自动化故障测试还覆盖持久claim并提交offset后未执行HTTP的窗口。

停止三个应用进程后，移除本项目的可丢弃测试基础设施：

```sh
docker compose -f compose.test.yaml down -v
```

这会删除该Compose项目的测试数据。不要将测试配置指向真实业务数据库。

## 3 运行参数与秘密

| 参数 | 含义 |
| --- | --- |
| PG_URL | 逗号分隔、顺序固定的JDBC URL；生产应启用验证服务端身份的TLS |
| PG_USER / PG_PASSWORD | 本角色数据库凭证，从secret平台注入 |
| KAFKA_BOOTSTRAP_SERVERS | Kafka bootstrap列表 |
| KAFKA_PARTITIONS | 当前部署固定分区数，默认16；高流量模型2048不是本地默认 |
| NOTIFICATION_KAFKA_REQUIRED_REPLICAS | Worker启动验证最小副本数，默认3 |
| NOTIFICATION_ENCRYPTION_KEY | Base64编码的32字节AES数据key；必填，无生产默认值 |
| DESTINATIONS_FILE | 管理员审查后的目标JSON文件 |
| JWT_ISSUER / JWT_AUDIENCE | API令牌验证约束 |
| NOTIFICATION_SECURITY_PUBLIC_KEY_FILE | 固定RSA公钥文件；适合本地验证和受控部署 |
| NOTIFICATION_SECURITY_JWK_SET_URI | 未配置固定公钥时的可信JWKS地址；使用HTTPS |
| QUOTA_URL | 可信共享配额服务的origin，不是业务方传入URL |
| NOTIFICATION_QUOTA_CREDENTIAL_REF | 例如`env:QUOTA_TOKEN`；生产必须认证配额接口 |
| NOTIFICATION_HTTP_MAXIMUM | Worker在途上限，默认128；不是Kafka listener concurrency |
| NOTIFICATION_KAFKA_CONCURRENCY | 平台poll线程数，默认2 |
| NOTIFICATION_API_MAXIMUM_IN_FLIGHT | 读取请求Body前的API在途上限，默认512 |
| NOTIFICATION_ADMISSION_MAXIMUM_PENDING | 全部署未终态任务软上限，默认50000，均摊到分片 |
| NOTIFICATION_HTTP_ADDITIONAL_ORIGINS | 显式批准的遗留HTTP/非443 HTTPS origin，逗号分隔；不会放宽供应商私网/DNS策略 |
| NOTIFICATION_ALLOW_LOOPBACK | 仅本地测试例外；开启时应用必须监听loopback |
| DB_MIGRATE | 应用是否执行迁移，默认false；生产使用独立迁移命令 |

背压使用约1秒的分片容量采样，因此有采样及并发事务的超调，不是精确的全局QPS配额。原始Outbox业务到期时间`scheduled_at`与发布重试时间`available_at`分开；正常等待未来Retry-After不算发布积压，发布失败的重试也不能不断重置积压年龄。

PG连接池每进程总预算不超过约64个，每分片至多8个；必须继续核算所有副本与数据库max_connections。扫描到期条件使用STABLE的`CURRENT_TIMESTAMP`，实际租约时间从数据库获取，避免VOLATILE时间谓词妨碍索引范围扫描。[PostgreSQL函数波动性说明](https://www.postgresql.org/docs/17/xfunc-volatility.html)

数据key必须长期保管并随备份保护。不能直接改环境变量后期待旧密文仍可读；本版本不包含在线DEK轮换/重加密工具。供应商凭证通过`credentialRef`解析，`credentialHeader`和`credentialPrefix`可支持Bearer、Basic或API-key Header；实际凭证值不进入目标快照，轮换凭证不需要改写业务任务。

## 4 共享配额服务的外部契约

此平台不在本仓库实现范围内，仓库只包含客户端及明确标为mock的测试替身。

- `POST /v1/permits`：请求`providerBudgetKey`、唯一`requestId`和`leaseSeconds=30`。平台必须原子检查账户级速率和并发，并对requestId去重。
- 允许时返回200及`permitId`（UUID）、`expiresAt`（RFC3339）。客户端仅使用剩余有效期超过HTTP预算余量、且不过长的许可；格式错误、过期、429、5xx、网络异常均不得发起HTTP。
- `POST /v1/permits/{permitId}/release`：幂等释放并发名额，不能退款已经消耗的速率令牌；客户端重复close只调用一次。释放失败由租约到期兜底。
- 生产使用HTTPS和服务身份。控制面客户端固定在已配置origin，可连接该可信内网平台；供应商客户端仍拒绝私网和云metadata地址，两套策略不能混用。

当前实现每次发送获取一个原子许可，没有宣称已完成批量grant优化。100k/s模型需要把配额RPC计入同步handoff延迟和容量；可在实测瓶颈后扩展批量额度租赁，但不以一个本地Semaphore假装分布式配额。

## 5 数据迁移与Kafka初始化

独立迁移入口不需要JWT或供应商密钥：

```sh
java -Dloader.main=com.rcbill.notification.worker.MigrationApplication \
  -cp notification-worker/target/notification-worker-0.1.0-SNAPSHOT.jar \
  org.springframework.boot.loader.launch.PropertiesLauncher
```

迁移使用独立DDL角色与直接PG连接，不经transaction-pooling PgBouncer。并发索引迁移声明非事务执行，并采用PostgreSQL session advisory lock；失败时先检查invalid index和Flyway历史，再人工修复，不能自动执行破坏性回滚。[Flyway锁设置](https://documentation.red-gate.com/flyway/reference/configuration/flyway-namespace/flyway-postgresql-namespace/flyway-postgresql-transactional-lock-setting)

Kafka初始化将main class改为`BootstrapApplication`，需要建topic及初始化group的管理员权限。它创建受控topic，验证已有分区/副本配置，并只初始化没有offset的组，已有offset保持不变。运行服务账号不应有这些管理权限。

拓扑保护记录分片数量、Kafka分区数和每个数据库的物理分片位置。重排JDBC URL列表会被拒绝，不能静默改变幂等路由。若从早期原型迁移且`physical_shard_id`为空，必须按**原映射**在维护窗口补齐，不能根据一个新URL顺序猜测。在线分片搬迁和跨区域主写切换需要独立运维流程，本服务不自动迁移数据。

## 6 CI与发布

CI固定第三方Action SHA和基础镜像digest，运行合同校验、JDK21构建/测试、真实PG和三Kafka broker集成、真实API/Worker演示、64请求并发烟测、非root只读容器启动。失败和成功报告均上传，并在独立`ci-evidence`分支保留可通过SSH核对的证据；测试作业没有仓库写权限，发布证据由独立作业完成。

`notification-release`工作流只接受master的手动触发。默认不发布、不部署。发布要求该精确commit已经通过完整CI，再构建并推送GHCR镜像；部署使用镜像digest，先运行迁移Job，成功后滚动更新API/Worker并等待readiness。production environment的审批规则必须由仓库管理员配置；工作流名称本身不等于已有审批人。

生产前先配置：

1. `notifications` namespace、审查后的ConfigMap及目的地版本，不能直接把空目标模板当成已配置业务。
2. 分离的`notification-api-runtime`、`notification-worker-runtime`、`notification-migration` secrets，以及需要的TLS证书与镜像拉取凭证。
3. API/Worker使用数据操作角色，迁移使用DDL角色；Kafka运行账号与初始化管理员分离。Relay可单独部署且不注入供应商凭证。
4. 外部Ingress、服务身份、真实共享配额、按环境设置的出口白名单、监控采集和备份。仓库没有虚构一个适用于所有环境的Egress CIDR或公网域名。
5. 真实环境的SQL计划、容量、供应商限额、恢复/故障域和低速/大响应测试。Compose三broker同机不代表三AZ容灾。

`deploy/k8s`提供非root、只读根文件系统、资源预算、探针、PDB、跨zone分布和Ingress限制模板。模板不是已经部署的集群；资源与副本数是起点，需要按容量模型调整。发布失败保留旧Pod和迁移日志，按既定兼容策略回滚应用，不自动回滚数据库DDL。

## 7 故障处置

| 现象 | 先检查 | 动作 |
| --- | --- | --- |
| 202后长时间PENDING | Outbox原始到期年龄、发布失败、Kafka lag、配额服务 | 暂停新流量或修复依赖，保留持久任务，禁止换key重提 |
| IN_FLIGHT超过租约 | 恢复器heartbeat、PG可用性、旧Worker暂停 | 恢复器标UNKNOWN并产生下一代Outbox，旧token不能写回 |
| 大量401/403 | issuer/audience/密钥及scope，区分入口和供应商错误 | 修复授权；供应商永久失败任务通过审计重放 |
| Kafka offset越过保留窗口 | group历史、PG账本、topic配置 | 停下对账，不把auto.offset.reset改成latest掩盖丢失 |
| 目标版本注册冲突 | 配置版本与指纹、部署差异 | 发布新版本；不要改DB指纹绕过保护 |
| 无法解密 | key是否被换掉、备份key是否对应 | 恢复正确key；不删除任务或伪造成功 |
| 清理停滞 | 终态、幂等保留期、活跃重放链、未发布事件 | 修复阻塞责任；不得删除未发布Outbox以释放磁盘 |

依赖不可用时readiness与指标应暴露问题，liveness不因单个供应商故障重启所有实例。所有运维操作先保留证据，以任务ID与事件ID定位，不记录原始Body、JWT或供应商凭证。
