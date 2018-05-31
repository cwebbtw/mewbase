package io.mewbase.binders.impl;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;


/**
 * Shims are used for two main purposes ...
 *
 * 1) Instrument the Calls to teh underlying class
 * 2) Implement security on a call by call bassis possibly wrt Counters and other instrumentation
 */

public class BinderStoreShim implements BinderStore {

    private final String METRICS_NAME = "mewbase.binderstore";

    private final Counter openCounter;
    private final Counter getCounter;
    private final Counter delCounter;
    private final AtomicLong binderCount = new AtomicLong(0);

    private final BinderStore impl;

    public BinderStoreShim(BinderStore impl) {
        openCounter = Metrics.counter(METRICS_NAME + ".open");
        getCounter = Metrics.counter( METRICS_NAME + ".get");
        delCounter = Metrics.counter( METRICS_NAME + ".delete");
        // gauges need to wrapped and registered
        Metrics.gauge(METRICS_NAME + ".binders", binderCount);
        this.impl = impl;
    }

    @Override
    public Binder open(String name) {
        Binder b = impl.open(name);
        openCounter.increment();
        impl.binders(); // force a count on the current binders
        return b;
    }

    @Override
    public Optional<Binder> get(String name) {
        Optional<Binder> ob = impl.get(name);
        getCounter.increment();
        return ob;
    }

    @Override
    public Stream<Binder> binders() {
        binderCount.set(0L);
        Stream<Binder> sb = impl.binders().map( b -> {
            binderCount.getAndIncrement();
            return b;
        });
        return sb;
    }

    @Override
    public Stream<String> binderNames() {
        return impl.binderNames();
    }

    @Override
    public Boolean delete(String name) {
        Boolean b = impl.delete(name);
        delCounter.increment();
        return b;
    }

    @Override
    public void close() throws Exception {
        impl.close();
    }
}
