CoreModule 的实现 CoreModuleProvider

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

PersistenceTimer 数据持久化定时器

#### 告警
MetricsPersistentWorker#nextWorker 触发告警 AlarmNotifyWorker -> AlarmEntrance -> MetricsNotify(NotifyHandler)#notify 方法，将 Metrics 放入对应规则的滑动窗口

