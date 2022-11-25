/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.oap.server.core.analysis.worker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.UnexpectedException;
import org.apache.skywalking.oap.server.core.analysis.data.MergableBufferedData;
import org.apache.skywalking.oap.server.core.analysis.data.ReadWriteSafeCache;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.exporter.ExportEvent;
import org.apache.skywalking.oap.server.core.storage.IMetricsDAO;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.client.request.PrepareRequest;
import org.apache.skywalking.oap.server.library.datacarrier.DataCarrier;
import org.apache.skywalking.oap.server.library.datacarrier.consumer.BulkConsumePool;
import org.apache.skywalking.oap.server.library.datacarrier.consumer.ConsumerPoolFactory;
import org.apache.skywalking.oap.server.library.datacarrier.consumer.IConsumer;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.apache.skywalking.oap.server.telemetry.TelemetryModule;
import org.apache.skywalking.oap.server.telemetry.api.CounterMetrics;
import org.apache.skywalking.oap.server.telemetry.api.MetricsCreator;
import org.apache.skywalking.oap.server.telemetry.api.MetricsTag;

/**
 * MetricsPersistentWorker is an extension of {@link PersistenceWorker} and focuses on the Metrics data persistent.
 * Metrics 数据的存储
 */
@Slf4j
public class MetricsPersistentWorker extends PersistenceWorker<Metrics> {
    /**
     * The counter of MetricsPersistentWorker instance, to calculate session timeout offset.
     */
    private static long SESSION_TIMEOUT_OFFSITE_COUNTER = 0;

    private final Model model;
    private final Map<Metrics, Metrics> context;
    private final IMetricsDAO metricsDAO;
    private final Optional<AbstractWorker<Metrics>> nextAlarmWorker;
    private final Optional<AbstractWorker<ExportEvent>> nextExportWorker;
    private final DataCarrier<Metrics> dataCarrier;
    private final Optional<MetricsTransWorker> transWorker;
    private final boolean enableDatabaseSession;
    private final boolean supportUpdate;
    private long sessionTimeout;
    private CounterMetrics aggregationCounter;
    private CounterMetrics skippedMetricsCounter;
    /**
     * The counter for the round of persistent.
     */
    private int persistentCounter;
    /**
     * The mod value to control persistent. The MetricsPersistentWorker is driven by the {@link
     * org.apache.skywalking.oap.server.core.storage.PersistenceTimer}. The down sampling level workers only execute in
     * every {@link #persistentMod} periods. And minute level workers execute every time.
     */
    private int persistentMod;
    /**
     * @since 8.7.0 TTL settings from {@link org.apache.skywalking.oap.server.core.CoreModuleConfig#getMetricsDataTTL()}
     */
    private int metricsDataTTL;
    /**
     * @since 8.9.0 The persistence of minute dimensionality metrics will be skipped if the value of the metric is the
     * default.
     */
    private boolean skipDefaultValueMetric;

    MetricsPersistentWorker(ModuleDefineHolder moduleDefineHolder, Model model, IMetricsDAO metricsDAO,
                            AbstractWorker<Metrics> nextAlarmWorker, AbstractWorker<ExportEvent> nextExportWorker,
                            MetricsTransWorker transWorker, boolean enableDatabaseSession, boolean supportUpdate,
                            long storageSessionTimeout, int metricsDataTTL) {
        // 指定 ReadWriteSafeCache
        super(moduleDefineHolder, new ReadWriteSafeCache<>(new MergableBufferedData(), new MergableBufferedData()));
        this.model = model;
        this.context = new HashMap<>(100);
        this.enableDatabaseSession = enableDatabaseSession;
        this.metricsDAO = metricsDAO;
        this.nextAlarmWorker = Optional.ofNullable(nextAlarmWorker);
        this.nextExportWorker = Optional.ofNullable(nextExportWorker);
        this.transWorker = Optional.ofNullable(transWorker); // 分钟维度转换成小时、天的维度
        this.supportUpdate = supportUpdate;
        this.sessionTimeout = storageSessionTimeout;
        this.persistentCounter = 0;
        this.persistentMod = 1;
        this.metricsDataTTL = metricsDataTTL;
        this.skipDefaultValueMetric = true;

        String name = "METRICS_L2_AGGREGATION";
        int size = BulkConsumePool.Creator.recommendMaxSize() / 8;
        if (size == 0) {
            size = 1;
        }
        BulkConsumePool.Creator creator = new BulkConsumePool.Creator(name, size, 20);
        try {
            ConsumerPoolFactory.INSTANCE.createIfAbsent(name, creator);
        } catch (Exception e) {
            throw new UnexpectedException(e.getMessage(), e);
        }

        this.dataCarrier = new DataCarrier<>("MetricsPersistentWorker." + model.getName(), name, 1, 2000);
        this.dataCarrier.consume(ConsumerPoolFactory.INSTANCE.get(name), new PersistentConsumer());

        MetricsCreator metricsCreator = moduleDefineHolder.find(TelemetryModule.NAME)
                                                          .provider()
                                                          .getService(MetricsCreator.class);
        aggregationCounter = metricsCreator.createCounter(
            "metrics_aggregation", "The number of rows in aggregation",
            new MetricsTag.Keys("metricName", "level", "dimensionality"),
            new MetricsTag.Values(model.getName(), "2", model.getDownsampling().getName())
        );
        skippedMetricsCounter = metricsCreator.createCounter(
            "metrics_persistence_skipped", "The number of metrics skipped in persistence due to be in default value",
            new MetricsTag.Keys("metricName", "dimensionality"),
            new MetricsTag.Values(model.getName(), model.getDownsampling().getName())
        );
        SESSION_TIMEOUT_OFFSITE_COUNTER++;
    }

    /**
     * Create the leaf and down-sampling MetricsPersistentWorker, no next step.
     * 创建 MetricsPersistentWorker，没有 next worker
     */
    MetricsPersistentWorker(ModuleDefineHolder moduleDefineHolder,
                            Model model,
                            IMetricsDAO metricsDAO,
                            boolean enableDatabaseSession,
                            boolean supportUpdate,
                            long storageSessionTimeout,
                            int metricsDataTTL) {
        this(moduleDefineHolder, model, metricsDAO,
             null, null, null,
             enableDatabaseSession, supportUpdate, storageSessionTimeout, metricsDataTTL
        );

        // Skipping default value mechanism only works for minute dimensionality.
        // Metrics in hour and day dimensionalities would not apply this as duration is too long,
        // applying this could make precision of hour/day metrics to be lost easily.
        // 分钟维度的 worker 忽略默认值的 metrics
        this.skipDefaultValueMetric = false;

        // For a down-sampling metrics, we prolong the session timeout for 4 times, nearly 5 minutes.
        // And add offset according to worker creation sequence, to avoid context clear overlap,
        // eventually optimize load of IDs reading.
        this.sessionTimeout = this.sessionTimeout * 4 + SESSION_TIMEOUT_OFFSITE_COUNTER * 200;
        // The down sampling level worker executes every 4 periods.
        this.persistentMod = 4;
    }

    /**
     * Accept all metrics data and push them into the queue for serial processing
     */
    @Override
    public void in(Metrics metrics) {
        aggregationCounter.inc();
        dataCarrier.produce(metrics);
    }

    @Override
    public List<PrepareRequest> buildBatchRequests() {
        if (persistentCounter++ % persistentMod != 0) {
            return Collections.emptyList();
        }
        // 从缓存中读取所有数据
        final List<Metrics> lastCollection = getCache().read();

        long start = System.currentTimeMillis();
        if (lastCollection.size() == 0) {
            return Collections.emptyList();
        }

        /*
         * Hard coded the max size. This only affect the multiIDRead if the data doesn't hit the cache.
         */
        int maxBatchGetSize = 2000;
        final int batchSize = Math.min(maxBatchGetSize, lastCollection.size());
        List<Metrics> metricsList = new ArrayList<>();
        List<PrepareRequest> prepareRequests = new ArrayList<>(lastCollection.size());
        for (Metrics data : lastCollection) {
            // hourPersistenceWorker、dayPersistenceWorker
            // 将 metrics 转换成 小时、天维度的数据
            transWorker.ifPresent(metricsTransWorker -> metricsTransWorker.in(data));

            metricsList.add(data);

            if (metricsList.size() == batchSize) {
                // 将 Metrics 数据写入存储 request，metricsList 会清空
                flushDataToStorage(metricsList, prepareRequests);
            }
        }

        // 不足一个 batchSize 的 metrics
        if (metricsList.size() > 0) {
            flushDataToStorage(metricsList, prepareRequests);
        }

        if (prepareRequests.size() > 0) {
            log.debug(
                "prepare batch requests for model {}, took time: {}, size: {}", model.getName(),
                System.currentTimeMillis() - start, prepareRequests.size()
            );
        }
        return prepareRequests;
    }

    private void flushDataToStorage(List<Metrics> metricsList,
                                    List<PrepareRequest> prepareRequests) {
        try {
            // 从DB中加载 metrics 放入缓存
            loadFromStorage(metricsList);

            long timestamp = System.currentTimeMillis();
            for (Metrics metrics : metricsList) {
                Metrics cachedMetrics = context.get(metrics);
                // 缓存中有（上面刚刚从DB加载到缓存），说明DB中存在metrics,需要更新
                if (cachedMetrics != null) {
                    /*
                     * If the metrics is not supportUpdate, defined through MetricsExtension#supportUpdate,
                     * then no merge and further process happens.
                     */
                    if (!supportUpdate) {
                        continue;
                    }
                    /*
                     * Merge metrics into cachedMetrics, change only happens inside cachedMetrics.
                     */
                    // 和缓存中的 metrics 合并
                    final boolean isAbandoned = !cachedMetrics.combine(metrics);
                    if (isAbandoned) {
                        continue;
                    }
                    // 计算 metrics value
                    cachedMetrics.calculate();
                    if (skipDefaultValueMetric && cachedMetrics.haveDefault() && cachedMetrics.isDefaultValue()) {
                        // Skip metrics in default value
                        skippedMetricsCounter.inc();
                    } else {
                        // 准备批量更新的 request
                        prepareRequests.add(metricsDAO.prepareBatchUpdate(model, cachedMetrics));
                    }
                    nextWorker(cachedMetrics);
                    cachedMetrics.setLastUpdateTimestamp(timestamp);
                } else {
                    // 计算 metrics value
                    metrics.calculate();
                    if (skipDefaultValueMetric && metrics.haveDefault() && metrics.isDefaultValue()) {
                        // Skip metrics in default value
                        skippedMetricsCounter.inc();
                    } else {
                        // 将 metrics 批量存储
                        prepareRequests.add(metricsDAO.prepareBatchInsert(model, metrics));
                    }
                    // 告警和导出 worker
                    nextWorker(metrics);
                    metrics.setLastUpdateTimestamp(timestamp);
                }

                /*
                 * The `metrics` should be not changed in all above process. Exporter is an async process.
                 */
                nextExportWorker.ifPresent(exportEvenWorker -> exportEvenWorker.in(
                    new ExportEvent(metrics, ExportEvent.EventType.INCREMENT)));
            }
        } catch (Throwable t) {
            log.error(t.getMessage(), t);
        } finally {
            // 将 list 清空
            metricsList.clear();
        }
    }

    private void nextWorker(Metrics metrics) {
        nextAlarmWorker.ifPresent(nextAlarmWorker -> nextAlarmWorker.in(metrics));
        nextExportWorker.ifPresent(
            nextExportWorker -> nextExportWorker.in(new ExportEvent(metrics, ExportEvent.EventType.TOTAL)));
    }

    /**
     * Load data from the storage, if {@link #enableDatabaseSession} == true, only load data when the id doesn't exist.
     */
    private void loadFromStorage(List<Metrics> metrics) {
        final long currentTimeMillis = System.currentTimeMillis();
        try {
            // 不在缓存中的 metrics
            List<Metrics> notInCacheMetrics =
                metrics.stream()
                       .filter(m -> {
                           // 判断 context 中是否有 metrics
                           final Metrics cachedValue = context.get(m);
                           // Not cached or session disabled, the metric could be tagged `not in cache`.
                           if (cachedValue == null || !enableDatabaseSession) {
                               return true;
                           }
                           // The metric is in the cache, but still we have to check
                           // whether the cache is expired due to TTL.
                           // This is a cache-DB inconsistent case: 缓存和DB一致性
                           // Metrics keep coming due to traffic, but the entity in the
                           // database has been removed due to TTL.
                           if (!model.isTimeRelativeID() && supportUpdate) {
                               // Mostly all updatable metadata level metrics are required to do this check.

                               if (metricsDAO.isExpiredCache(model, cachedValue, currentTimeMillis, metricsDataTTL)) {
                                   // The expired metrics should be removed from the context and tagged `not in cache` directly.
                                   context.remove(m);
                                   return true;
                               }
                           }

                           return false;
                       })
                       .collect(Collectors.toList());
            if (notInCacheMetrics.isEmpty()) {
                return;
            }
            // 从DB中获取这些不在缓存中的metrics
            final List<Metrics> dbMetrics = metricsDAO.multiGet(model, notInCacheMetrics);
            if (!enableDatabaseSession) {
                // Clear the cache only after results from DB are returned successfully.
                context.clear();
            }
            // 将这些 metrics 放入缓存中
            dbMetrics.forEach(m -> context.put(m, m));
        } catch (final Exception e) {
            log.error("Failed to load metrics for merging", e);
        }
    }

    @Override
    public void endOfRound() {
        if (enableDatabaseSession) {
            Iterator<Metrics> iterator = context.values().iterator();
            long timestamp = System.currentTimeMillis();
            while (iterator.hasNext()) {
                Metrics metrics = iterator.next();

                if (metrics.isExpired(timestamp, sessionTimeout)) {
                    iterator.remove();
                }
            }
        }
    }

    /**
     * Metrics queue processor, merge the received metrics if existing one with same ID(s) and time bucket.
     *
     * ID is declared through {@link Object#hashCode()} and {@link Object#equals(Object)} as usual.
     */
    private class PersistentConsumer implements IConsumer<Metrics> {
        @Override
        public void init(final Properties properties) {

        }

        @Override
        public void consume(List<Metrics> data) {
            // 将数据写入缓存，metrics L2 Aggregation
            MetricsPersistentWorker.this.onWork(data);
        }

        @Override
        public void onError(List<Metrics> data, Throwable t) {
            log.error(t.getMessage(), t);
        }

        @Override
        public void onExit() {
        }
    }
}
