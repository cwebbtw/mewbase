package io.mewbase.eventsource.impl;

import io.mewbase.eventsource.EventHandler;

import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;


import java.time.Instant;
import java.util.concurrent.CompletableFuture;


/**
 * Shims are used for two main purposes ...
 *
 * 1) Instrument the Calls to the underlying class
 * 2) Implement security on a call by call basis possibly wrt Counters and other instrumentation
 */

public class SourceShim implements EventSource {

    private static final String METRICS_NAME = "mewbase.event.source.subscribe";

    private final Counter subsCounter;

    private final EventSource impl;


    public SourceShim(EventSource impl) {
        this.impl = impl;
        subsCounter = Metrics.counter( METRICS_NAME );
    }


    @Override
    public CompletableFuture<Subscription> subscribe(String channelName, EventHandler eventHandler) {
        subsCounter.increment();
        return impl.subscribe(channelName, new EventHandlerShim(channelName, eventHandler));
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        subsCounter.increment();
        return impl.subscribeFromMostRecent(channelName, new EventHandlerShim(channelName, eventHandler));
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        subsCounter.increment();
        return impl.subscribeFromEventNumber(channelName, startInclusive, new EventHandlerShim(channelName, eventHandler));
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        subsCounter.increment();
        return impl.subscribeFromInstant(channelName, startInstant, new EventHandlerShim(channelName, eventHandler));
    }

    @Override
    public CompletableFuture<Subscription> subscribeAll(String channelName, EventHandler eventHandler) {
        subsCounter.increment();
        return impl.subscribeAll(channelName, new EventHandlerShim(channelName, eventHandler));
    }

    @Override
    public void close() {
        impl.close();
    }

}
