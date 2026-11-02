# 骨架验证记录

日期：2026-11-02。范围：构建、模块契约、骨架启动与占位接口。本记录不表示业务投递、数据库一致性或生产安全已经完成。

## 环境与构建

Windows PowerShell，JDK 27.0.1.0.3；Maven Wrapper 3.9.11，Java编译目标21。Spring Boot 4.1.2与Spring Cloud 2025.1.4共同构建并启动成功。未执行JDK21运行时验证或跨平台验证。

最终命令：`./mvnw clean verify`（Windows实际调用`mvnw.cmd`）。父项目及五个模块均BUILD SUCCESS，API与Worker生成独立可执行jar。domain的Maven Enforcer规则拒绝Spring编译依赖。

| 测试类 | 用例数 | 结果 |
| --- | --- | --- |
| ScaffoldModeTest | 14 | 全部通过；显式profile、混合profile、缺失/外部/通配地址、IPv6回环、管理地址 |
| ScaffoldHealthIndicatorTest | 1 | 未实现业务时明确DOWN |
| NotificationControllerTest | 4 | 四个路由均501，Problem Details及instance/traceId有效，不能返回Location假接收 |
| 总计 | 19 | 0失败，0错误，0跳过 |

测试使用真实断言暴露占位行为，不为纯状态枚举或只读record编写getter测试。MVC测试使用独立MockMvc，无数据库或外部服务；不将其计为生产集成测试。

## TDD证据

1. 启动守卫初始无保护：12个测试中11个失败，失败原因为应拒绝的启动配置未抛异常。
2. 健康指示器初始UP：健康测试失败，期望DOWN；实现scaffold未就绪指示后通过。
3. 业务路由初始占位返回202：4个测试全部失败，明确显示“expected 501 but was 202”；改成Problem Details 501后通过。
4. 补充独立管理地址边界：先运行测试确认对`0.0.0.0`未拒绝，再增加校验；最后全量19个用例通过。

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

使用`openapi-spec-validator`解析并验证OpenAPI 3.0.3文件，4个operation合法且引用可解析。状态枚举、路由数量与当前骨架接口对应；生产鉴权和字段校验尚未实现，骨架只执行501占位行为。

Git按规则、设计、OpenAPI、构建、领域类型、应用端口、启动保护、API壳、Worker壳和交付说明拆分。未配置或推送GitHub远端。集成测试、CI/CD、容量与恢复演练以及完整会话zip均未完成，详见设计和README。
