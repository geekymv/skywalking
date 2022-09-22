package org.apache.skywalking.oap.server.core.analysis;

import org.apache.skywalking.oap.server.core.analysis.metrics.CountMetrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.Metrics;
import org.apache.skywalking.oap.server.core.analysis.metrics.annotation.MetricsFunction;
import org.apache.skywalking.oap.server.core.remote.grpc.proto.RemoteData;
import org.junit.Assert;
import org.junit.Test;

public class FunctionCategoryTest {

    @Test
    public void testUniqueFunctionName() {
        final String a = FunctionCategory.uniqueFunctionName(CustomAMetrics.class);
        Assert.assertEquals("metrics-custom", a);

        final String b = FunctionCategory.uniqueFunctionName(CustomBMetrics.class);
        Assert.assertEquals("metrics-count", b);
    }

    @MetricsFunction(functionName = "custom")
    class CustomAMetrics extends CountMetrics {

        @Override
        public Metrics toHour() {
            return null;
        }

        @Override
        public Metrics toDay() {
            return null;
        }

        @Override
        protected String id0() {
            return null;
        }

        @Override
        public void deserialize(final RemoteData remoteData) {

        }

        @Override
        public RemoteData.Builder serialize() {
            return null;
        }

        @Override
        public int remoteHashCode() {
            return 0;
        }
    }


    class CustomBMetrics extends CountMetrics {

        @Override
        public Metrics toHour() {
            return null;
        }

        @Override
        public Metrics toDay() {
            return null;
        }

        @Override
        protected String id0() {
            return null;
        }

        @Override
        public void deserialize(final RemoteData remoteData) {

        }

        @Override
        public RemoteData.Builder serialize() {
            return null;
        }

        @Override
        public int remoteHashCode() {
            return 0;
        }
    }
}


