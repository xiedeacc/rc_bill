# 适配器边界

已引入MyBatis Starter、PostgreSQL驱动与Spring Kafka依赖。当前仅实现本地骨架的启动保护与未就绪健康指示，scaffold禁用数据库自动连接、Kafka消费启动与自动建topic；不连接数据库、Kafka、密钥平台或供应商。

后续按独立功能实现 `NotificationStore`、`DeliveryQueue`、`OutboxStore`、`DispatchPublisher`、`DestinationCatalog`、`HttpTransport`、`SecretProvider`、`DispatchAdmission`。MyBatis Mapper完成同分片短事务，Kafka发布必须等待确认后标记Outbox，消费者按事件epoch精确CAS领取并持久移交后推进offset。HTTP只发送一次请求，不能处于数据库或Kafka事务中。不得使用JdbcTemplate编写仓储；MyBatis底层仍依赖JDBC/Spring事务基础设施。

业务生产者、广告系统、CRM、库存系统、OIDC服务端、Vault服务端不属于本项目。测试通过这些端口的fake或外部服务mock模拟，不在main源码下实现周边系统。
