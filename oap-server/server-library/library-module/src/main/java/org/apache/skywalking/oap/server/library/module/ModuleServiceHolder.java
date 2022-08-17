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

package org.apache.skywalking.oap.server.library.module;

/**
 * 模块需要的Service
 * 每个 ModuleProvider 提供一个容器（Map<Class<? extends Service>, Service> services），用于存储接口和对应的实现类
 */
public interface ModuleServiceHolder {
    /**
     * 注册服务实现
     * @param serviceType
     * @param service
     * @throws ServiceNotProvidedException
     */
    void registerServiceImplementation(Class<? extends Service> serviceType,
        Service service) throws ServiceNotProvidedException;

    /**
     * 查找服务实现
     * @param serviceType
     * @param <T>
     * @return
     * @throws ServiceNotProvidedException
     */
    <T extends Service> T getService(Class<T> serviceType) throws ServiceNotProvidedException;
}
