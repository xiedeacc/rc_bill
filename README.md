# 可靠 API 通知服务

接收内部业务系统的通知请求，异步投递到广告平台、CRM、库存等外部 HTTP API。不同目标通过受控的版本化配置支持各自 URL、Header 和 Body。业务方不需要等待或解析供应商响应。

**当前交付是系统设计与可运行的代码骨架，尚不是完整通知服务。** 所有业务接口返回 `501 NOT_IMPLEMENTED`，不会接收任务、写数据库或发出外部请求。生产鉴权、持久化、幂等、调度和重试均待后续 TDD 实现；不能将本阶段构建结果用于生产接收流量。

## 文档入口

- [系统设计](docs/system-design.md)：需求对应、架构与组件、事务和数据模型、幂等、租约、重试、重放、安全、容量、故障恢复、测试与发布方案。
- [Kafka高流量专项设计](docs/kafka-reliability.md)：Kafka参数/部署、Outbox、offset与幂等边界、PG分片热点、虚拟线程。
- [OpenAPI 契约](contracts/openapi.yaml)：四个目标业务接口及字段约束、权限、幂等、分页、错误语义；版本 OpenAPI 3.0.3。
- [项目规则](AGENTS.md)：TDD、单功能提交、依赖方向及外部系统边界。
- [验证记录](docs/verification.md)：本阶段真实执行的 Red / Green、构建与本地启动检查。

## 设计选择

Java 21 编译目标，Spring Boot 4.1.2 + Spring Cloud 2025.1.4，MyBatis Starter 4.1.0、Spring Kafka 4.1.2（kafka-clients 4.2.2），Maven 3.9.11 Wrapper 固定版本与 SHA-256。选型兼容性来源见系统设计参考资料。

目标架构是API、Outbox Relay、Worker三个运行角色，Relay与Worker共用worker模块并规划独立部署。分片PostgreSQL使用MyBatis维护幂等/状态/Outbox短事务，Kafka是主要投递通道，正常工作不再扫描数据库任务表领取。Spring Cloud CircuitBreaker / Resilience4j依赖已引入，具体业务适配仍待实现。

虚拟线程通过Boot配置开启，用于MVC与自动任务执行器；未来HTTP执行器在提交前限制在途数，Kafka消费循环显式保持平台线程。底层MyBatis仍使用JDBC和连接池，不跨虚拟线程传播数据库事务。

投递采用至少一次语义；Kafka幂等生产不能消除Outbox应用重发，Kafka事务也不涵盖PG或HTTP。外部系统成功后本服务崩溃或响应丢失，可能再次发送；供应商支持稳定幂等键时才能在其去重窗口内避免重复业务效果。服务不会承诺通用 HTTP 下的端到端 exactly-once。

容量设计假设为平均20,000条/秒、10分钟峰值100,000条/秒，默认24小时内最多20次尝试。设计包含计算过程、背压与扩容条件；这些是待压测的预算，不是已达到的性能指标。

## 代码结构

```text
notification-domain/       状态与查询值类型，无Spring生产依赖
notification-application/  提交/查询/重放用例及存储/投递/配置等端口
notification-adapters/     MyBatis/Kafka依赖及本地启动保护；具体外部适配器待实现
notification-api/          业务路由占位、API启动类，默认127.0.0.1:8080
notification-worker/       Worker启动类及Cloud依赖，后续装配Relay/Consumer角色，默认127.0.0.1:8081
contracts/                 OpenAPI目标契约
docs/                      系统设计与验证证据
```

依赖方向为 api/worker → adapters → application → domain。应用端口可在后续单元测试中注入fake；业务生产者、广告、CRM、库存、身份平台和密钥平台不在本项目中实现。

## 构建

准备 JDK 21（推荐）或受支持的新版本 JDK，并设置 `JAVA_HOME`。无需全局安装 Maven，首次使用 Wrapper 需访问 Maven Central。首次构建会下载依赖。

PowerShell：

```powershell
# 将JAVA_HOME设为本机JDK目录，而非bin目录
# $env:JAVA_HOME = 'C:\path\to\jdk-21'
.\mvnw.cmd clean verify
```

Linux/macOS：

```sh
./mvnw clean verify
```

编译输出统一为 Java 21。当前验证机器使用 Windows 和 JDK 27.0.1.0.3，编译参数为 `--release 21`；尚未在 JDK21/25运行时与 Linux 上验证。生产优先评估JDK25，未来 CI 使用JDK21/25矩阵，见系统设计 TODO。

## 本地启动

构建后在两个终端分别运行。若 `java` 不在 PATH，PowerShell 可使用 `& "$env:JAVA_HOME\bin\java.exe"` 代替 `java`。

```sh
java -jar notification-api/target/notification-api-0.1.0-SNAPSHOT.jar --spring.profiles.active=scaffold
java -jar notification-worker/target/notification-worker-0.1.0-SNAPSHOT.jar --spring.profiles.active=scaffold
```

必须且只能启用 `scaffold` profile。默认profile、`prod`、`scaffold,prod` 均拒绝启动；监听地址只能是字面量 `127.0.0.1` 或 `::1`，不能绑定 `0.0.0.0`。骨架不接入生产认证，故仅允许本地健康和501占位接口。scaffold配置禁用DataSource自动连接、Kafka listener启动和自动建topic，不需要本机安装PG或Kafka。引入starter不等于完成集成。

| 检查 | API | Worker | 预期 |
| --- | --- | --- | --- |
| 存活 | http://127.0.0.1:8080/actuator/health/liveness | http://127.0.0.1:8081/actuator/health/liveness | 200 UP |
| 就绪 | http://127.0.0.1:8080/actuator/health/readiness | http://127.0.0.1:8081/actuator/health/readiness | 503 DOWN，业务尚未实现 |
| 总健康 | /actuator/health | /actuator/health | 503 DOWN，预期行为 |
| 业务路由 | OpenAPI定义的四个路径 | 无业务Controller | API返回501及Problem Details |

PowerShell示例（`curl.exe` 避免旧版PowerShell的curl别名）：

```powershell
curl.exe -i http://127.0.0.1:8080/actuator/health/liveness
curl.exe -i http://127.0.0.1:8080/actuator/health/readiness
curl.exe -i -X POST http://127.0.0.1:8080/v1/notifications
```

最后一条当前返回501；DTO校验和身份校验尚未接线，不应据此判断生产契约接受空请求。后续完整请求示例及校验约束见OpenAPI。

## 已验证与未完成

2026-11-02 已验证：`clean verify` 成功，21个单元/独立MVC/自动配置测试全部通过，两个可执行jar均实际启动，4条API路由返回501；两进程liveness 200、readiness 503；不安全配置在监听前启动失败；两个自动任务执行器经真实任务确认使用虚拟线程；OpenAPI和Kafka事件Schema校验通过。

尚未实现：真实业务逻辑、数据库迁移、生产认证、目标配置适配、出站HTTP、安全出口、限流/熔断接线、持久重试、重放审计、归档、运行指标。设计中的集成测试、故障/容量测试和CI/CD均为TODO。当前启动冒烟不能替代API→MyBatis/PG→Outbox→Kafka→Worker→供应商的集成测试。

本会话按要求不编写完整开发周期计划，也不继续实现业务功能。下一阶段应在设计基础上安排TDD迭代；各行为按AGENTS规则独立提交。

## 最终作业提交清单

- [x] 保留原始题目PDF，完成设计文档和代码骨架。
- [x] README包含范围、运行方式、设计取舍和验证结果。
- [x] 建立本地Git，按规则拆分提交。
- [ ] 后续完成作业要求的真实通知服务实现及对应测试。
- [x] 已通过SSH发布到`git@github.com:xiedeacc/rc_bill.git`，后续本地变更的推送状态以交付说明为准。
- [ ] 最终导出所有参与作业的AI完整、未删改会话，打包zip并放入仓库，附来源和SHA-256。当前会话未结束，尚未生成该归档；不以摘要替代原始记录。

`invite.ics` 是原有邀请文件，保持原样且忽略，不作为作业代码提交。请勿在后续会话或代码中输入生产密钥，以便最终如实提交完整会话。
