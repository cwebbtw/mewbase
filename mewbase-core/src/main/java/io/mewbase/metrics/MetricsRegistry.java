package io.mewbase.metrics;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.micrometer.core.instrument.Meter;

import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


import java.util.List;


import static io.micrometer.core.instrument.Metrics.*;

public interface MetricsRegistry {

    static void ensureRegistry() {

        if ( globalRegistry.getRegistries().isEmpty() ) addRegistry(new SimpleMeterRegistry());

        new ClassLoaderMetrics().bindTo(globalRegistry);
        new JvmMemoryMetrics().bindTo(globalRegistry);
        new JvmGcMetrics().bindTo(globalRegistry);
        new ProcessorMetrics().bindTo(globalRegistry);
        new JvmThreadMetrics().bindTo(globalRegistry);

    }


    static BsonObject allMetricsAsDocument() {

        List<Meter> meters = globalRegistry.getMeters();

        BsonArray metersArray = new BsonArray();

        meters.forEach( meter -> {

            BsonArray tags = new BsonArray();
            meter.getId().getTags().forEach( t -> {
                BsonObject tag = new BsonObject();
                tag.put(t.getKey(), t.getValue());
                tags.add(tag);
            } );

            BsonArray measures = new BsonArray();
            meter.measure().forEach( m -> {
                BsonObject measure = new BsonObject();
                measure.put(m.getStatistic().name(), m.getValue() );
                measures.add(measure);
            });

            // now put the name, measures and tags together
            BsonObject meterObj =  new BsonObject();
            meterObj.put("name",meter.getId().getName());
            meterObj.put("tags",tags);
            meterObj.put("measures",measures);

            metersArray.add(meterObj);
        } );

        return new BsonObject().put("meters",metersArray);
    }
}
