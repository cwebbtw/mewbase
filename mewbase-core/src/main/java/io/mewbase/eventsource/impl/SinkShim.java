package io.mewbase.eventsource.impl;


import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;



/**
 * Shims are used for two main purposes ...
 *
 * 1) Instrument the Calls to the underlying class
 * 2) Implement security on a call by call basis possibly wrt Counters and other instrumentation
 */

public class SinkShim implements EventSink {

    private static final String METRICS_NAME = "mewbase.event.sink.publish";
    private static final String SYNC = ".sync";
    private static final String ASYNC = ".async";

    private final Map<String, Counter> syncCounters = new HashMap<>();
    private final Map<String, Counter> asyncCounters = new HashMap<>();

    private final EventSink impl;

    public SinkShim(EventSink impl) {
        this.impl = impl;
    }


    private Counter createCounter(String type, String channel) {
        List<Tag> tag = Arrays.asList(Tag.of("channel", channel));
        return Metrics.counter( METRICS_NAME + type, tag);
    }

    @Override
    public Long publishSync(String channelName, BsonObject event) {
        Long evtNum = impl.publishSync(channelName,event);
        Counter ctr = syncCounters.computeIfAbsent( channelName, s -> createCounter(SYNC,channelName));
        ctr.increment();
        return evtNum;
    }

    @Override
    public CompletableFuture<Long> publishAsync(String channelName, BsonObject event) {
        CompletableFuture<Long> evtFut = impl.publishAsync(channelName,event);
        Counter ctr = asyncCounters.computeIfAbsent(channelName, s -> createCounter(ASYNC,channelName));
        ctr.increment();
        return evtFut;
    }

    @Override
    public void close() {
        impl.close();
    }

}
