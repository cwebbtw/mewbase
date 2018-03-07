package io.mewbase.eventsource.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.MultiEventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MultiEventSinkImpl implements MultiEventSink {

    private final static Logger logger = LoggerFactory.getLogger(MultiEventSinkImpl.class);

    final Set<EventSink> sinks;
    final ExecutorService exec = Executors.newWorkStealingPool();

    public MultiEventSinkImpl(Set<EventSink> sinks) {
        this.sinks = sinks;
    }

    @Override
    public Stream<Long> publishSync(String channelName, BsonObject event) {
        return sinks.parallelStream().map( sink -> {
                    try {
                        return sink.publishSync(channelName, event);
                    } catch (Exception exp) {
                        logger.error("Sink " + sink + "failed to publish event" + event);
                    }
                    return -1L;
                })
                .collect(Collectors.toList())
                .stream();
    }

    @Override
    public CompletableFuture<Stream<Long>> publishAsync(String channelName, BsonObject event) {
        CompletableFuture<Stream<Long>> fut = CompletableFuture.supplyAsync( () ->
                sinks
                    .stream()
                    .parallel()
                    .map(sink -> sink.publishAsync(channelName,event) )
                    .map( futOfLong -> futOfLong.join() )
                    .collect(Collectors.toList())
                    .stream() );
        return fut;
    }

    @Override
    public void close() { sinks.forEach( sink -> sink.close() ); }

}
