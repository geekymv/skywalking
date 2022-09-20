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

package org.apache.skywalking.oap.server.storage.plugin.elasticsearch.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.oap.server.core.analysis.DownSampling;
import org.apache.skywalking.oap.server.core.storage.IHistoryDeleteDAO;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.library.client.elasticsearch.ElasticSearchClient;
import org.apache.skywalking.oap.server.storage.plugin.elasticsearch.IndicesMetadataCache;
import org.joda.time.DateTime;

@Slf4j
public class HistoryDeleteEsDAO extends EsDAO implements IHistoryDeleteDAO {

    public HistoryDeleteEsDAO(ElasticSearchClient client) {
        super(client);
    }

    @Override
    public void deleteHistory(Model model, String timeBucketColumnName, int ttl) {
        ElasticSearchClient client = getClient();

        if (!model.isRecord()) {
            if (!DownSampling.Minute.equals(model.getDownsampling())) {
                /*
                 * In ElasticSearch storage, the TTL triggers the index deletion directly.
                 * As all metrics data in different down sampling rule of one day are in the same index, the deletion operation
                 * is only required to run once.
                 * 不同采样规则下的同一天的所有维度的数据在同一个 index 里，删除操作只需要执行一次
                 * 比如 sw_metrics-count-20220920
                 * 202209201517 分钟
                 * 2022092015 小时
                 * 20220920 天
                 */
                return;
            }
        }
        long deadline = Long.parseLong(new DateTime().plusDays(-ttl).toString("yyyyMMdd"));
        String tableName = IndexController.INSTANCE.getTableName(model);
        Collection<String> indices = client.retrievalIndexByAliases(tableName);

        if (log.isDebugEnabled()) {
            log.debug("Deadline = {}, indices = {}, ttl = {}", deadline, indices, ttl);
        }

        List<String> prepareDeleteIndexes = new ArrayList<>();
        List<String> leftIndices = new ArrayList<>();
        for (String index : indices) {
            long timeSeries = TimeSeriesUtils.isolateTimeFromIndexName(index);
            if (deadline >= timeSeries) {
                prepareDeleteIndexes.add(index);
            } else {
                leftIndices.add(index);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Indices to be deleted: {}", prepareDeleteIndexes);
        }
        for (String prepareDeleteIndex : prepareDeleteIndexes) {
            client.deleteByIndexName(prepareDeleteIndex);
        }
        String latestIndex = TimeSeriesUtils.latestWriteIndexName(model);
        String formattedLatestIndex = client.formatIndexName(latestIndex);
        if (!leftIndices.contains(formattedLatestIndex)) {
            client.createIndex(latestIndex);
        }
    }

    @Override
    public void inspect(List<Model> models, String timeBucketColumnName) {
        List<String> indices = new ArrayList<>();
        models.forEach(model -> {
            if (!model.isTimeSeries()) {
                return;
            }

            ElasticSearchClient client = getClient();

            if (!model.isRecord()) {
                if (!DownSampling.Minute.equals(model.getDownsampling())) {
                    /*
                     * As all metrics data in different down sampling rule of one day are in the same index, the inspection
                     * operation is only required to run once.
                     */
                    return;
                }
            }
            String tableName = IndexController.INSTANCE.getTableName(model);
            Collection<String> indexes = client.retrievalIndexByAliases(tableName);

            indices.addAll(indexes);
        });
        IndicesMetadataCache.INSTANCE.update(indices);
    }
}
