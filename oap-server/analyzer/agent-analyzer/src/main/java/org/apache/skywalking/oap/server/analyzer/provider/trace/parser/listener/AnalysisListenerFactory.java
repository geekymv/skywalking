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

package org.apache.skywalking.oap.server.analyzer.provider.trace.parser.listener;

import org.apache.skywalking.oap.server.library.module.ModuleManager;
import org.apache.skywalking.oap.server.analyzer.provider.AnalyzerModuleConfig;

/**
 * AnalysisListenerFactory implementation creates the listener instances when required. Every AnalysisListener could
 * have its own creation factory.
 * 每个 AnalysisListener 有自己的创建工厂，主要是从 ModuleManager 获取一些依赖的服务
 */
public interface AnalysisListenerFactory {
    AnalysisListener create(ModuleManager moduleManager, AnalyzerModuleConfig config);
}
