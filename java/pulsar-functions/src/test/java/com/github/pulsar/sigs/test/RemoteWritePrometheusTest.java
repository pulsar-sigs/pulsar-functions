package com.github.pulsar.sigs.test;

import prometheus.Remote;
import prometheus.Types;

import java.util.ArrayList;
import java.util.List;

public class RemoteWritePrometheusTest {
    public static Remote.WriteRequest newWriteRequest(){
        List<Types.Label> labels2 = new ArrayList<>();
        Types.Label name = Types.Label.newBuilder()
                .setName("__name__")
                .setValue("hello")
                .build();
        labels2.add(name);

        Remote.WriteRequest build = Remote.WriteRequest.newBuilder()
                .addMetadata(Types.MetricMetadata.newBuilder()
                        .setHelp("help")
                        .setType(Types.MetricMetadata.MetricType.COUNTER)
                        .setMetricFamilyName("familyName")
                        .build())
                .addTimeseries(Types.TimeSeries.newBuilder()
                        .addAllLabels(labels2)
                        .addSamples(Types.Sample.newBuilder()
                                .setTimestamp(System.currentTimeMillis())
                                .setValue(1)
                                .build())
                        .build())
                .build();
        return build;
    }
}
