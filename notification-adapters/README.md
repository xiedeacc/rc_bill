# 适配器边界

当前仅实现本地骨架的启动保护与未就绪健康指示，不连接数据库、密钥平台或供应商。

后续按独立功能实现 `NotificationStore`、`DeliveryQueue`、`DestinationCatalog`、`HttpTransport`、`SecretProvider`、`DispatchAdmission`。PostgreSQL适配器拥有短事务，HTTP适配器只发送一次请求；二者不能在同一数据库事务内执行。

业务生产者、广告系统、CRM、库存系统、OIDC服务端、Vault服务端不属于本项目。测试通过这些端口的fake或外部服务mock模拟，不在main源码下实现周边系统。
