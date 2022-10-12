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

package org.apache.skywalking.oap.server.core.analysis;

import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class IDManagerTest {
    @Test
    public void testServiceID() {
        IDManager.ServiceID.ServiceIDDefinition define = new IDManager.ServiceID.ServiceIDDefinition(
            "Service",
            true
        );
        final IDManager.ServiceID.ServiceIDDefinition relationDefine = IDManager.ServiceID.analysisId(
            IDManager.ServiceID.buildId(
                "Service",
                true
            ));
        Assert.assertEquals(define, relationDefine);
    }

    @Test
    public void testServiceRelationID() {
        IDManager.ServiceID.ServiceRelationDefine define = new IDManager.ServiceID.ServiceRelationDefine(
            IDManager.ServiceID.buildId("ServiceSource", true),
            IDManager.ServiceID.buildId("ServiceDest", true)
        );

        final String relationId = IDManager.ServiceID.buildRelationId(define);
        final IDManager.ServiceID.ServiceRelationDefine serviceRelationDefine = IDManager.ServiceID.analysisRelationId(
            relationId);
        Assert.assertEquals(define, serviceRelationDefine);
    }

    @Test
    public void testServiceIdAndServiceInstanceId() {
        // service id
        String service = "spring-app";
        final String serviceId = IDManager.ServiceID.buildId(service, true);
        System.out.println(serviceId);

        // service instance id
        String instanceName = UUID.randomUUID().toString().replace("-", "") + "@192.168.10.1";
        final String serviceInstanceId = IDManager.ServiceInstanceID.buildId(serviceId, instanceName);
        System.out.println(serviceInstanceId);
    }

}
