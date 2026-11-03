# 接收延迟性能调查（2026-11-03）

## 审查结论与实验计划

原烟测在 readiness 后立即发送 64 个请求、并发 8，P95 927.93ms 包含首次请求初始化，不能据此判断稳定运行延迟，也不能把后续跨 runner 的单次改善归因于代码。

本次先固定 2KiB body、独立幂等键、并发 8、真实 PG / Kafka / Worker，保留首批 64 请求作为冷窗口；之后执行 256 请求预热及三个 512 请求窗口。每阶段等待任务全部成功，记录每次 POST 原始耗时、成功率、阶段吞吐和时间范围。使用相同客户端连接策略；结果仅代表此闭环并发负载，不代表生产容量或开放到达率 SLO。

增加无业务数据的 JFR 阶段事件（HTTP 入口、授权后处理、Mapper 调用、事务整体及事务函数），结合 JVM 执行采样与等待事件判断时间分布。阶段是嵌套的，不能相加；Mapper 耗时包含 JDBC 等待和 Java 映射，不等于数据库执行时间。未开启录制时不产出记录。仅测试环境录制，禁止收集令牌、body、SQL 参数或业务标识。

**状态：已按用户2026-11-03指示停止性能测试，不再启动新的压测。** 本地只做过单元/组件验证；真实PG/Kafka和性能负载运行在GitHub托管的隔离CI runner，并非本机部署服务。当前同机ABBA对照已取消，以下保留停止前的实际数据。

优化必须由测量支撑，不放松 durable commit、幂等唯一约束、背压或租约安全。先补失败回归测试，再实现；提交与重测结果在完成后补充。生产 150ms 目标目前仍未验收。

## 首轮实测与优化决策

基线 `db04ef6`，Actions `41611863607`：并发8，冷窗口P95 588.40ms；预热168.25ms；三个后续窗口128.46 / 86.27 / 63.40ms，1856个请求全部接收并成功投递。窗口仍持续变快，不能把第一个预热后窗口称作充分稳定的长期运行。正式协议将预热扩展到4096请求，每个后续窗口2048请求，保留所有窗口而不挑最快值。

最后一个窗口中，512个请求的 `http.accept` 累计13586.942ms，所有Mapper事件累计约10054ms（约74%）；事务累计9743.704ms。各计时存在嵌套，不能将事务与Mapper耗时相加。每次新接收有9次Mapper调用（幂等预读、数据库时钟、Outbox年龄检查、五表写入、回读created_at），另有低频背压计数。数据库驱动SocketRead等待有实测记录，首次窗口另有类加载、密码运算及框架初始化采样；冷窗口不归因于一条慢SQL，亦没有证据认定连接池或GC是唯一根因。

选择的改动：将五表写入与created_at回读合并为一个有参数绑定的MyBatis数据修改CTE，继续由原Spring事务控制提交；时钟、Outbox年龄和容量检查不变。重复键仍由唯一约束裁决，CTE返回未插入幂等记录时抛出内部Duplicate，整个事务回滚，再读取赢家。不能把临时任务或payload留在库内。MyBatis必须使用`affectData=true`使返回结果的写语句参与事务语义。优化不改变HTTP契约，也不提前返回202。


## 已取得的数据

环境：Ubuntu 24.04 runner、4个逻辑CPU、Temurin 21、PostgreSQL 17、三个Kafka 4.2.2 broker。API、Worker、数据库、broker和Python驱动同机；先导JFR采样中整机CPU多次达到97%—100%。客户端使用urllib，每次新连接、并发8、2KiB文本body、独立幂等键、TTL60秒；闭环客户端会等待前一个请求返回才发送下一个，不是固定到达率压测。所有分位数使用nearest-rank，失败样本不得从分布删除。录制开启的成本也包含在结果内。

| 版本/协议 | 窗口 | 请求数 | P50 ms | P95 ms | P99 ms | 接收成功/秒 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| 优化前 db04ef6 / 协议1 | 冷窗口 | 64 | 118.72 | 588.40 | 697.29 | 42.42 |
| 同上 | 预热 | 256 | 58.07 | 168.25 | 228.51 | 118.46 |
| 同上 | 预热后1 | 512 | 44.73 | 128.46 | 162.29 | 143.38 |
| 同上 | 预热后2 | 512 | 37.14 | 86.27 | 151.09 | 188.55 |
| 同上 | 预热后3 | 512 | 30.20 | 63.40 | 89.12 | 238.83 |
| 优化后 0e83d02 / 协议2 | 冷窗口 | 64 | 76.226 | 547.281 | 562.733 | 59.974 |
| 同上 | 预热 | 4096 | 47.099 | 141.523 | 248.899 | 132.476 |
| 同上 | 预热后1 | 2048 | 27.383 | 64.045 | 91.700 | 250.874 |
| 同上 | 预热后2 | 2048 | 25.349 | 60.766 | 107.374 | 268.771 |
| 同上 | 预热后3 | 2048 | 23.494 | 49.623 | 67.973 | 306.865 |

优化前1856个、优化后10304个请求均为202且最终SUCCEEDED，窗口记录的errors、unfinished、terminal_errors均为0。优化后冷P95仍超过150ms；预热后窗口低于150ms，但仍可见收敛趋势，不能称为长期稳态SLO验收。两版预热量、样本数、runner不相同，**禁止用此表计算代码优化百分比**。同机ABBA尚未完成，因此目前确定的优化是正常路径Mapper调用9→4，端到端因果收益仍待后续授权的固定环境实验。

原始证据：

- 优化前：[CI 41611863607](https://github.com/xiedeacc/rc_bill/actions/runs/41611863607)、[逐请求结果](https://github.com/xiedeacc/rc_bill/blob/ci-evidence/runs/db04ef68fd9b1b4e854a928b957fe99fee50a82a/ci-demo-benchmark.json)、[JFR分阶段摘要](https://github.com/xiedeacc/rc_bill/blob/ci-evidence/runs/db04ef68fd9b1b4e854a928b957fe99fee50a82a/ci-demo-profile.json)。
- 优化后：[CI步骤8日志](https://github.com/xiedeacc/rc_bill/actions/runs/41613081735/job/125839631206#step:8:31)逐窗口JSON；[测试结果](https://github.com/xiedeacc/rc_bill/blob/ci-evidence/runs/0e83d025d87784ea476954a61e7894039a8cecd8/summary.json)记录runtime成功、performance取消。第二次运行在比较准备期间被取消，初始结果已移到临时目录，原收集器未归档该目录，因此优化后的逐请求JSON/JFR摘要未保存在证据分支；上表以实际Actions日志为来源，不伪造丢失样本。收集器已改为保留该临时目录，但没有为补齐证据重新运行测试。

## 瓶颈定位的证据与边界

先导最后窗口512个请求：Mapper累计10053.879ms，HTTP入口计时累计13586.942ms，占74.0%；事务累计9743.704ms，占71.7%。这是同一窗口累计耗时比，**不是分位数相减**。事务内工作累计8434.086ms；数据库往返及Java映射构成主要可优化路径。JFR同时观察到PG驱动SocketRead等待。证据能支持“减少持久层交互次数”，不能区分每次等待中网络、PG执行及操作系统调度的精确占比，也不证明某个索引失效。

冷窗口HTTP平均128.93ms，客户端平均181.25ms；入口之外还包含连接、容器调度和客户端开销。授权前开销、首次类加载、RSA/AES及框架初始化在冷窗口明显，不能只优化SQL就宣称消除了冷启动。JFR中后台线程池正常等待不能当作请求阻塞；稀疏CPU采样不能当作精确火焰图占比。没有足够证据归因于热点key、池耗尽、GC或虚拟线程pinning；此次随机key负载也未专门验证热点。

## TDD与提交证据

- `3a7bda6`：计时回归先期望2个事件但实际0个；实现后成功/异常均记录固定阶段，不含业务参数。
- `7c21eeb`：统计TDD占位实现以均值替代分位数、忽略失败，两个用例先失败；实现nearest-rank和全量结果统计后通过。
- `0e83d02`：`AcceptanceRoundTripsTest`旧实现实际调用boundedPending、now、oldestUnpublished、insertTask、insertPayload、insertDedup、insertChain、outbox、task；新实现保留前三项并改为acceptBundle，通过调用预算回归。额外测试检查返回写语句的affectData、绑定参数以及真实PG故障回滚。
- 停止前，本地verify通过；CI的148个单元/组件用例及13个真实基础设施用例均0失败、0错误、0跳过。Outbox故障回滚、同key32并发、时间戳与幂等回执均在真实PG验证。整个CI最终取消，不称作完整流水线成功；本轮容器运行步骤未执行。

## 保留的复现方案（未继续执行）

普通CI恢复64请求功能烟测。长压测和ABBA只能通过workflow_dispatch显式勾选`acceptance_performance`启动，默认false；本次最终提交使用`[skip ci]`，不会因push重新开启测试。开发机不承担性能验收。

未来经授权在专用测试资源运行时，执行scripts/compare-acceptance.sh，固定旧版db04ef6与候选API；顺序旧/新/新/旧，每轮新JVM、schema、topic、消费组，Worker和客户端一致。保留冷窗口；延长预热到吞吐、P95、CPU与编译活动收敛，必要时延长测量时间而非挑选最快窗口。另做录制关闭的复测、固定到达率及阶梯并发、同key热点、跨AZ RTT、池等待、PG pg_stat_statements/EXPLAIN、长期积压与故障恢复验收。这些均是后续计划，未执行，10万QPS仍为设计假设。

实现依据：[MyBatis返回数据的写语句affectData](https://mybatis.org/mybatis-3/sqlmap-xml.html)、[PostgreSQL数据修改CTE快照与RETURNING](https://www.postgresql.org/docs/17/queries-with.html)、[JDK21 JFR事件](https://docs.oracle.com/en/java/javase/21/docs/api/jdk.jfr/jdk/jfr/Event.html)。
