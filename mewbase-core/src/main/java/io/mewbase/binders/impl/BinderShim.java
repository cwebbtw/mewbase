package io.mewbase.binders.impl;

import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Shims are used for two main purposes ...
 *
 * 1) Instrument the Calls to teh underlying class
 * 2) Implement security on a call by call basis possibly wrt Counters and other instrumentation
 */

public class BinderShim implements Binder {

    private final String METRICS_NAME = "mewbase.binder";

    private final Counter putCounter;
    private final Counter getCounter;
    private final Counter delCounter;
    private final AtomicLong docsCount =  new AtomicLong(0);

    private final Binder impl;


    public BinderShim(Binder impl) {

        this.impl = impl;

        final String name = impl.getName();

        List<Tag> tag = Arrays.asList(Tag.of("name", name));
        putCounter = Metrics.counter( METRICS_NAME + ".put", tag);
        getCounter = Metrics.counter( METRICS_NAME + ".get", tag);
        delCounter = Metrics.counter( METRICS_NAME + ".delete", tag);
        // gauges need to wrapped and registered
        Metrics.gauge(METRICS_NAME + ".documents", tag, docsCount);
        docsCount.set(countDocuments());
    }

    @Override
    public String getName() {
        return impl.getName();
    }

    @Override
    public CompletableFuture<BsonObject> get(String name) {
        CompletableFuture<BsonObject> ob = impl.get(name);
        getCounter.increment();
        return ob;
    }

    @Override
    public CompletableFuture<Boolean> put(String id, BsonObject doc) {
        CompletableFuture<Boolean> op = impl.put(id, doc);
        putCounter.increment();
        // put may change the total number of docs
        return op.thenApplyAsync( b -> {
            docsCount.set(countDocuments());
            return b; } );
    }

    @Override
    public CompletableFuture<Boolean> delete(String name) {
        CompletableFuture<Boolean> cb = impl.delete(name);
        delCounter.increment();
        // delete may change the total number of docs
        return cb.thenApplyAsync( b -> {
            docsCount.set(countDocuments());
            return b; } );
    }

    @Override
    public Long countDocuments() {
        return impl.countDocuments();
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments() {
        return impl.getDocuments();
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments(Predicate<KeyVal<String, BsonObject>> filter) {
        return impl.getDocuments(filter);
    }

    @Override
    public Boolean setStreaming(EventSink sink, String channel) {
        return impl.setStreaming (sink, channel);
    }

    @Override
    public Boolean isStreaming() {
        return impl.isStreaming();
    }

}
