package io.mewbase.metrics;

import io.mewbase.bson.BsonArray;
import io.mewbase.bson.BsonObject;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;


import java.util.List;


import static io.micrometer.core.instrument.Metrics.*;

public interface MetricsRegistry {

    static String discoverAllMetrics() {

        addRegistry(new SimpleMeterRegistry());

        MeterRegistry registry = globalRegistry;

        new ClassLoaderMetrics().bindTo(registry);
        new JvmMemoryMetrics().bindTo(registry);
        new JvmGcMetrics().bindTo(registry);
        new ProcessorMetrics().bindTo(registry);
        new JvmThreadMetrics().bindTo(registry);

        List<Meter> meters = registry.getMeters();

        BsonObject doc = new BsonObject();

        meters.forEach( meter -> {
            final Meter.Id mId = meter.getId();
            // mId.getConventionName(NamingConvention.dot);
            System.out.println(mId.toString());
            System.out.println(mId.getConventionName(NamingConvention.identity));
            Iterable<Measurement> vals = meter.measure();
            vals.forEach( v -> System.out.println(v) );

            BsonArray meterArray = doc.getBsonArray(mId.getName(), new BsonArray());

            BsonArray tags = new BsonArray();
            mId.getTags().forEach( t -> {
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

            // now put the measures and
            //




        } );


        // System.out.println(meters);
        return meters.toString();
    }
}
