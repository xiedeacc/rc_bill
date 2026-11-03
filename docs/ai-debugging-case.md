# AI协作排障实录与故障演练

本文件区分两类证据：前半部分是本轮开发中真实发生的CI失败与修复，后半部分是主动设计并执行的可靠性故障演练。它们都不是被包装成真实事故的虚构生产故事。

## 实际案例一 MyBatis参数绑定失败

第一次真实PG CI中，本地单元/组件测试已通过，但5个数据库集成场景全部报错：

```text
There is no getter for property named 'id' in 'class java.util.UUID'
Mapper: LedgerMapper.insertChain
```

[原始失败CI](https://github.com/xiedeacc/rc_bill/actions/runs/41485481447)，源码基线`63a3172`。最初的Mapper测试只验证SQL能解析、存在参数占位符，没有执行实际参数绑定；因此绿色结果没有覆盖真正出错的边界。

排查时先把假设拆成UUID格式错误、PG类型不支持、MyBatis参数命名三类。错误发生在JDBC执行之前，栈指向`DefaultParameterHandler`与反射getter，所以无需先改数据库列或把UUID转成字符串。直接用真实`ParamNameResolver`和`DefaultParameterHandler`、仅mock JDBC PreparedStatement，即可在本地复现五个失败。

修复是给单参数Mapper方法显式加`@Param("id")`。UUID不是MyBatis默认识别的普通标量参数，裸对象可能被按bean访问`id`属性；显式命名建立参数Map后才会正确取出UUID并交给驱动。`-parameters`不能替代这一绑定契约。

验证：新增5个参数化用例，修复前5个错误，修复后全部通过；提交`79f6410`。没有改HTTP契约、数据库UUID类型或业务幂等算法。

## 实际案例二 bytea被误认为多行byte结果

第二次PG CI中，前四个场景已通过，重放仍失败：

```text
Bad value for type byte: \x...
MapperMethod.executeForMany
ByteTypeHandler.getNullableResult
```

[第二次失败CI](https://github.com/xiedeacc/rc_bill/actions/runs/41486235156)。这次不是AES解密失败：异常在读取数据库字段时发生。裸`byte[]`返回类型被Mapper方法分派解释为“多行结果组成数组”，从而用单个byte的TypeHandler读取整个bytea。

最小复现使用真实MapperProxy和配置，仅替换SqlSession的边界返回值；测试期望密文字节，却得到空数组。修复将查询映射为单行`Map<String,Object>`，再通过默认方法取出`ciphertext`的byte[]。这是明确单行形状，而不是把密文转成字符串或关闭校验。

提交`b1199b0`后，[真实PG/Kafka端到端CI通过](https://github.com/xiedeacc/rc_bill/actions/runs/41486832737)：123个单元/组件测试、6个集成测试，零失败、零跳过。后续新增测试的最新结果以验证文档和对应CI SHA为准，不能把这组历史数字冒充最新结果。

## 实际案例三 topic创建确认与broker可见性不同步

增加独立Kafka初始化命令后，CI发现创建topic的Future已经成功，但紧接着的describe仍可能得到`UnknownTopicOrPartitionException`。[该次CI记录](https://github.com/xiedeacc/rc_bill/actions/runs/41511974823)。这不是“加一个固定sleep就好”的问题：控制面确认与各broker元数据传播并非同一个完成点。

修复`839547c`增加有限的元数据就绪等待，仅重试可恢复错误，并等待leader/ISR可用；权限错误立即失败，已有topic不反复创建，已有消费offset不重置。纯逻辑测试用可替换的纳秒时间与pause函数验证收敛、权限失败及截止时间，没有在单元测试中真实sleep。

## 如何证明AI帮助了排查而不是掩盖问题

本轮保留失败CI、最小回归测试和修复提交。AI帮助归纳假设、缩小边界、寻找框架语义并生成反例；证据来自真实执行，而不是AI说“应该没问题”。候选人应主动说明：纯策略和边界组件按Red/Green推进；部分基础设施适配先实现后由真实CI暴露缺陷，不能把整个过程包装成完美的全程TDD。

可复演的提示模板：

> 以下是真实堆栈和失败测试。请先区分错误发生在参数绑定、JDBC执行还是结果物化阶段，提出能证伪的最小实验。不要先改业务数据类型，也不要用全套mock绕过框架行为。修复后说明原测试为何漏检。

## 故障演练 Kafka已提交offset但HTTP尚未执行

下面是受控故障注入，不是实际生产事故。测试在同一消费组提交offset后故意丢弃内存执行，再通过持久租约恢复推进任务。它验证责任移交协议，不伪称已经完成跨AZ杀节点演练。

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

`LedgerIT.durableHandoffCrashRecoversAndFencesLateWorker`验证PG租约、UNKNOWN记录、下一代事件和旧token围栏；`PipelineIT.deliveryRetryDuplicateAndDurableHandoffRecovery`在真实Kafka提交offset后注入执行丢失，并等待后续成功。最新同组版本使用原消费组，不靠另一个组重新读旧记录。

执行命令为`./mvnw -Pintegration clean verify`，需要真实PG/Kafka。原始报告保存在Actions artifacts及`ci-evidence/runs/<commit>/`。测试环境、最新通过commit和实际规模见[验证记录](verification.md)。

## 案例四：接收P95超过150ms，如何避免AI“调参猜根因”

这是2026-11-03在本仓库实际执行的性能调查，不是虚构的生产事故。旧记录927.93ms来自readiness后的首批64请求。详细协议、原始数据位置、前后对照及限制见[接收性能分析](acceptance-performance.md)。

1. **先纠正指标定义。** AI检查脚本后发现没有预热，只能证明冷窗口偏慢。增加冷窗口、预热、三个独立窗口和逐请求结果，保持2KiB、并发8、新连接策略及真实PG/Kafka/Worker一致。HTTP非202、任务未完成、终态失败均使实验失败，不能从延迟数据里删掉失败样本。
2. **用观测区分假设。** 增加JFR固定阶段事件，不记录业务数据。首轮冷P95 588.40ms，后续128.46/86.27/63.40ms；持续收敛说明256次预热仍不足。把正式预热扩大到4096次，后续每窗口2048次，而不是挑最小值宣称修复。
3. **缩小优化范围。** 最后一个先导窗口的Mapper累计耗时占HTTP服务端跨度约74%；正常接收9次Mapper调用，耗时分散在多个调用，PG SocketRead亦有等待。冷窗口还有认证、类加载、加密和框架初始化成本。没有证据支持“Kafka ACK慢”“单一热点行”“必须扩连接池”或“GC就是根因”。Kafka发布不在202同步路径，扩大Kafka并发不是本次直接修复。
4. **先写可失败的资源预算测试。** `AcceptanceRoundTripsTest`在原实现上实际得到9次store调用，而期望保留容量/时钟/年龄检查并合并写入的4次，运行失败。合并五表写入及时间戳回读后本地verify通过；Mapper契约检查affectData和参数绑定，PG故障用例检查Outbox失败时五表回滚。
5. **用反例约束AI的SQL。** PG数据修改CTE不能靠基础表重读兄弟CTE结果，必须使用RETURNING；唯一约束竞争输家必须回滚临时task/payload/chain/outbox，再读赢家。不能仅凭“一个SQL天然原子”就忽略业务去重冲突产生的临时记录。不能使用提前202、异步落库、关闭fsync、忽略失败或移除背压改善数字。
6. **在同机做因果对照。** 正式实验方案按旧版/新版/新版/旧版运行（用户要求停止后已取消，未完成），每轮重启API和Worker，使用新schema、topic、消费组；Worker与客户端保持相同。公开全部窗口、失败率与录制开销限制，不将不同runner的两次P95直接相减当收益。

可复用提示模板（复盘整理，非逐字会话）：

> 下面是冷窗口、预热后窗口、JFR阶段耗时及9次Mapper调用。先说明证据能排除什么、不能证明什么；给出最小可检验的改动，先写失败回归。所有五表写入仍要同分片事务，竞争输家不得留下孤儿行，202必须等commit。请设计同机交替版本对照，并保留冷启动未达标和尾延迟退化的数据。

AI在此任务中也需要审查：初版录制配置沿用JFR profile默认项，会包含环境变量；发现后在业务录制前取消该CI，明确关闭环境变量、系统属性与JVM启动参数事件，并仅发布筛选后的摘要。基准统计的TDD占位实现用于验证“用均值冒充P95”和“忽略失败状态”两个反例。面试展示应强调这些检查过程，而非宣称AI第一次输出就完全正确。
