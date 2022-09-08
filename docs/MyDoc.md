CoreModule 的实现 CoreModuleProvider

OALEngineLoaderService -调用 OALRuntime#notifyAllListeners 方法  -> StreamAnnotationListener#notify 为每个 Stream数据 创建 worker.

MetricsStreamProcessor 负责将 Metrics 的所有 worker 串起来
MetricsAggregateWorker（L1聚合） -> MetricsRemoteWorker（将 metrics 发送给目标 OAP 节点）
（另一个 OAP 节点）RemoteHandleWorker -> MetricsPersistentWorker
