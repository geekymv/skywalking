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

package org.apache.skywalking.oap.server.recevier.configuration.discovery;

import com.google.common.hash.Hashing;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import org.apache.skywalking.oap.server.configuration.api.ConfigChangeWatcher;
import org.apache.skywalking.oap.server.library.module.ModuleProvider;

/**
 * AgentConfigurationsWatcher used to handle dynamic configuration changes.
 */
public class AgentConfigurationsWatcher extends ConfigChangeWatcher {
    private volatile String settingsString;
    private volatile AgentConfigurationsTable agentConfigurationsTable;
    private final AgentConfigurations emptyAgentConfigurations;

    public AgentConfigurationsWatcher(ModuleProvider provider) {
        super(ConfigurationDiscoveryModule.NAME, provider, "agentConfigurations");
        this.settingsString = null;
        this.agentConfigurationsTable = new AgentConfigurationsTable();
        // noinspection UnstableApiUsage
        // 空的动态配置信息
        this.emptyAgentConfigurations = new AgentConfigurations(
            null, new HashMap<>(),
            Hashing.sha512().hashString("EMPTY", StandardCharsets.UTF_8).toString()
        );
    }

    /**
     * 配置变更通知
     * @param value of new.
     */
    @Override
    public void notify(ConfigChangeEvent value) {
        if (value.getEventType().equals(EventType.DELETE)) {
            settingsString = null;
            this.agentConfigurationsTable = new AgentConfigurationsTable();
        } else {
            // agetn config 配置 key: configuration-discovery.default.agentConfigurations
            // agent config 配置内容:
            /*
            configurations:
              //service name
              serviceA:
                // Configurations of service A
                // Key and Value are determined by the agent side.
                // Check the agent setup doc for all available configurations.
                key1: value1
                key2: value2
                ...
              serviceB:
                ...
             */

            settingsString = value.getNewValue();
            AgentConfigurationsReader agentConfigurationsReader =
                new AgentConfigurationsReader(new StringReader(value.getNewValue()));
            // 读取配置内容
            this.agentConfigurationsTable = agentConfigurationsReader.readAgentConfigurationsTable();
        }
    }

    @Override
    public String value() {
        return settingsString;
    }

    /**
     * Get service dynamic configuration information, if there is no dynamic configuration information, return to empty
     * dynamic configuration to prevent the server from deleted the dynamic configuration, but it does not take effect
     * on the agent side.
     *
     * @param service Service name to be queried
     * @return Service dynamic configuration information
     */
    public AgentConfigurations getAgentConfigurations(String service) {
        // 根据 agent 服务名称从本地缓存中获取配置
        AgentConfigurations agentConfigurations = agentConfigurationsTable.getAgentConfigurationsCache().get(service);
        if (null == agentConfigurations) {
            return emptyAgentConfigurations;
        } else {
            return agentConfigurations;
        }
    }
}
