# AI协作排障案例 从Kafka消费成功到通知实际丢失

这是基于本项目协议设计的可复现故障演练，不是真实生产事故。故障注入点是“数据库已持久领取、Kafka已提交offset、HTTP尚未开始时Worker退出”。场景选择先于实现，执行证据在对应测试完成后补充，不能把预期结果写成实测结果。

## 业务现象与初始证据

业务方提交一条CRM通知得到202。Kafka lag归零，但供应商mock没有收到请求，任务持续停在IN_FLIGHT。接口接收成功、消息消费成功和供应商送达成功是三个不同事实；只观察lag会误判业务健康。

面试演示使用合成tenant、无个人信息的Body和供应商mock。记录notificationId、eventId、scheduleEpoch、attemptNo、leaseUntil、offset和供应商接收次数；不向AI发送生产JWT或凭证。

## 候选人如何组织AI工作

第一次提示应提供事实与约束，而不是“帮我修复Kafka丢消息”：

> 当前是PG事务Outbox、Kafka至少一次消息、Worker持久claim后提交offset。HTTP尚未开始时杀进程，任务停在IN_FLIGHT。请列出可区分的故障假设，指出需要检查哪些状态和提交点；不要先修改参数，也不要假设Kafka丢失消息。

要求AI建立以下假设表，并逐项证伪：

| 假设 | 判别证据 | 结论条件 |
| --- | --- | --- |
| Producer未写入Kafka | Outbox未发布、broker无记录 | 与已消费offset相矛盾时排除 |
| 消费者提前提交内存任务 | PG无STARTED或无租约 | 这是不可恢复窗口，必须改变ack顺序 |
| 已持久移交但没有恢复执行 | PG有STARTED及过期lease，无后续Outbox | 本演练的主要故障 |
| 供应商已经处理但响应丢失 | mock收到请求而DB未知 | 应保留同deliveryKey重试，不能等同未发送 |
| 旧Worker迟到覆盖新结果 | 新旧leaseToken不同，旧写回成功 | CAS缺失，需独立修复与回归 |

第二次提示将目标收窄：

> 请先写失败测试：第一次claim成功后不执行HTTP，推进可替换时钟或数据库租约状态，运行恢复器，必须产生新调度代次；旧token写回必须失败。保持HTTP在数据库事务外，禁止通过把offset提前/随意后移掩盖问题。

## 最小修复与反例

恢复器使用短事务锁定过期IN_FLIGHT，标记对应STARTED为UNKNOWN，然后依据TTL与次数进入RETRY_WAIT、DEAD或EXPIRED。允许重试时递增scheduleEpoch并同事务插入Outbox。若两个恢复器竞争，只允许一次状态转换。任务完成写回必须同时校验IN_FLIGHT和leaseToken。

应拒绝AI提出的以下“快速修复”：

- 将`enable.auto.commit`打开：增加不可控提交窗口。
- 把所有失败无条件塞回Kafka：形成重试风暴，丢失TTL和attempt预算。
- 把任务改成PENDING但不创建Outbox：Kafka主分发路径下任务仍可能永远不被唤醒。
- 使用Redis SETNX代替持久状态：缓存故障、过期和DB提交之间仍有一致性窗口。
- 每次重试生成新的供应商幂等键：数据库去重成功也可能重复扣库存。
- 只加sleep让测试通过：无法确定性证明租约恢复时序。

## 验证与面试展示

演示应包含Red日志、修复diff、Green结果以及以下反例：重复event只能领取一次；claim后退出可恢复；供应商已处理但写回失败时允许重投且deliveryKey不变；旧token不能覆盖；TTL到期不会再次发送；恢复与写回竞争只产生一个合法后继状态。

AI负责生成候选假设、辅助定位、起草测试和寻找反例；候选人负责选择一致性边界、验证源代码、检查运行证据并解释剩余风险。面试官真正应看到的是“如何验证AI输出”，而不只是“能让AI生成很多代码”。

## 执行证据

待本轮业务实现和故障测试完成后填写测试类、命令、失败/通过结果及提交号。当前没有声称该演练已在真实PG/Kafka环境执行。
