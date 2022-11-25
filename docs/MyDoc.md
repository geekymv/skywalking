CoreModule 的实现 CoreModuleProvider

#### OAL
OALLexer.g4 词法规则，词法规则名称以大写字母开头
OALParser.g4 语法规则，词法规则名称以小写字母开头

#### JVMModuleProvider
start 方法中调用 OALEngineLoaderService 的 load 方法， 加载 JVM OAL 定义

一个 source 可以有多个 metrics
同一个 source（ServiceInstanceJVMGC）的 metrics 都会对应一个 Dispatcher 处理类（ServiceInstanceJVMGCDispatcher）
也就是说一个source下的所有 metrics 都交给一个 dispatcher 分发
Dispatcher 处理类 实现了 SourceDispatcher（dispatch方法） 接口

instance_jvm_old_gc_time = from(ServiceInstanceJVMGC.time).filter(phase == GCPhase.OLD).sum();
metricsName -> InstanceJvmOldGcTime
metricsClass -> InstanceJvmOldGcTimeMetrics
sourceName -> ServiceInstanceJVMGC


#### JVM Metrics 处理过程

JVMMetricReportServiceHandler -> JVMSourceDispatcher -> SourceReceiver（SourceReceiverImpl）-> SourceDispatcher

- JVMMetricReportServiceHandler，gRPC 接受 agent 发送过来的JVM数据；
- JVMSourceDispatcher，对JVM数据分类，CPU、Memory 等，比如将 Memory 数据封装成 ServiceInstanceJVMMemory（Source），发送给 SourceReceiver；
- SourceReceiver 根据 Source 中的 scope 找到对应的 SourceDispatcher（比如 ServiceInstanceJVMMemory 对应的 SourceDispatcher 是由 OAL 生成的 ServiceInstanceJVMMemoryDispatcher）
  将 ServiceInstanceJVMMemory 转换成 InstanceJvmMemoryHeapMetrics（OAL生成）等，调用 MetricsStreamProcessor#in 方法。

#### work flow
OALEngineLoaderService -调用 OALRuntime#notifyAllListeners 方法  -> StreamAnnotationListener#notify 为每个 Stream数据 创建 worker.

MetricsStreamProcessor 负责将 Metrics 的所有 worker 串起来

MetricsAggregateWorker（L1聚合） -> MetricsRemoteWorker（将 metrics 发送给目标 OAP 节点）

（集群模式下另一个 OAP 节点）RemoteServiceHandler -> RemoteHandleWorker -> MetricsPersistentWorker -> DataCarrier 内存队列
- RemoteServiceHandler 接收其他节点发送过来的数据，
  
PersistentConsumer 消费 DataCarrier -> 将数据写入 ReadWriteSafeCache

PersistenceTimer 数据持久化定时器，每25秒执行一次，将缓存中的数据取出来持久化

#### 告警
MetricsPersistentWorker#nextWorker 触发告警 AlarmNotifyWorker -> AlarmEntrance -> MetricsNotify(NotifyHandler)#notify 方法，将 Metrics 放入对应规则的滑动窗口

