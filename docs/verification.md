# 骨架验证记录

日期：2026-11-02。范围：构建、模块契约、骨架启动与占位接口。本记录不表示业务投递、数据库一致性或生产安全已经完成。

## 环境与构建

Windows PowerShell，JDK 27.0.1.0.3；Maven Wrapper 3.9.11，Java编译目标21。Spring Boot 4.1.2与Spring Cloud 2025.1.4共同构建并启动成功。已加入MyBatis Starter 4.1.0、Spring Kafka 4.1.2及kafka-clients 4.2.2，并通过打包启动检查。未执行JDK21/25运行时验证或跨平台验证。

最终命令：`./mvnw clean verify`（Windows实际调用`mvnw.cmd`）。父项目及五个模块均BUILD SUCCESS，API与Worker生成独立可执行jar。domain的Maven Enforcer规则拒绝Spring编译依赖。

| 测试类 | 用例数 | 结果 |
| --- | --- | --- |
| ScaffoldModeTest | 14 | 全部通过；显式profile、混合profile、缺失/外部/通配地址、IPv6回环、管理地址 |
| ScaffoldHealthIndicatorTest | 1 | 未实现业务时明确DOWN |
| NotificationControllerTest | 4 | 四个路由均501，Problem Details及instance/traceId有效，不能返回Location假接收 |
| VirtualThreadConfigurationTest（API和Worker各1） | 2 | 读取各自application.yaml，由真实Boot自动配置执行器运行任务，确认Thread.isVirtual为true |
| 总计 | 21 | 0失败，0错误，0跳过 |

测试使用真实断言暴露占位行为，不为纯状态枚举或只读record编写getter测试。MVC测试使用独立MockMvc，无数据库或外部服务；不将其计为生产集成测试。

## TDD证据

1. 启动守卫初始无保护：12个测试中11个失败，失败原因为应拒绝的启动配置未抛异常。
2. 健康指示器初始UP：健康测试失败，期望DOWN；实现scaffold未就绪指示后通过。
3. 业务路由初始占位返回202：4个测试全部失败，明确显示“expected 501 but was 202”；改成Problem Details 501后通过。
4. 补充独立管理地址边界：先运行测试确认对`0.0.0.0`未拒绝，再增加校验；最后全量19个用例通过。

5. 虚拟线程新增配置：先运行API/Worker两条执行器测试，均因实际运行在线程池平台线程而失败；开启Boot虚拟线程属性后通过。之后全量21个测试通过。

只提交Green版本。原始本地构建过程日志位于被忽略的`tmp/`，最终记录保留关键事实；这些构建日志不替代题目要求的完整AI会话zip。

## 实际进程检查

对打包后的两个jar使用临时本地端口启动，执行HTTP检查后关闭测试进程，未遗留后台服务。

| 检查 | 结果 |
| --- | --- |
| API liveness | 200，UP |
| API readiness | 503，DOWN |
| Worker liveness | 200，UP |
| Worker readiness | 503，DOWN |
| API提交/状态/尝试/重放四条路由 | 全部501，code为NOT_IMPLEMENTED，不含Location |
| API默认profile | 非零退出，在监听前拒绝 |
| API同时启用scaffold与prod | 非零退出，在监听前拒绝 |
| API绑定0.0.0.0 | 非零退出，在监听前拒绝 |

启动冒烟只是本地可运行性验证，不是设计中TODO的数据库、供应商mock、并发、故障恢复与端到端集成测试。

## 契约和交付检查

使用`openapi-spec-validator`解析并验证OpenAPI 3.0.3文件，4个operation合法且引用可解析。Kafka事件JSON Schema通过Draft2020-12验证，合法事件通过，越界slot、负epoch、未知字段和错误UUID等输入被拒绝。状态枚举、路由数量与当前骨架接口对应；生产鉴权和字段校验尚未实现，骨架只执行501占位行为。

Git按规则、设计、OpenAPI、构建、领域类型、应用端口、启动保护、API壳、Worker壳和交付说明拆分。v1已通过SSH推送GitHub；本次v2变更为本地独立提交，尚未推送。集成测试、CI/CD、容量与恢复演练以及完整会话zip均未完成，详见设计和README。

## 高流量设计修订的验证边界

参数来源核对到Kafka4.2官方文档，特别区分producer幂等、Kafka EOS和外部HTTP副作用；容量公式已独立复算。真实Kafka集群、PG分片、Outbox、Mapper SQL、offset/租约故障协议、配额和有界HTTP执行器均未实现，未作高流量压测。引入依赖与开启虚拟线程不代表已获得这些能力。scaffold禁用DataSource自动连接、Kafka listener自动启动和自动建topic，本地冒烟未连接外部基础设施。
