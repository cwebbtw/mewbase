package io.mewbase.eventsource.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.MultiEventSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class MultiEventSinkImpl implements MultiEventSink {

    private final static Logger logger = LoggerFactory.getLogger(MultiEventSinkImpl.class);

    final Set<EventSink> sinks;
    final ExecutorService exec = Executors.newWorkStealingPool();

    MultiEventSinkImpl(Set<EventSink> sinks) {
        this.sinks = sinks;
    }

    @Override
    public Stream<Long> publishSync(String channelName, BsonObject event) {
        return sinks.parallelStream().map ( sink -> {
            try {
                return sink.publishSync(channelName, event);
            } catch (Exception exp ) {
                logger.error("Sink " + sink + "failed to publish event" + event);
            }
         ).collect(Collectors.toList());
    }

    @Override
    public Future<Stream<Long>> publishAsync(String channelName, BsonObject event) {
        Future<Stream<Long>> fut = exec.submit( () -> {
             Stream<Long> nums = sinks
                    .stream()
                    .parallel()
                    .map(sink -> sink.publishAsync(channelName,event) )
                    .map( futOfLong -> futOfLong.join() )
                    .collect(Collectors.toList())
                    .stream();
             return nums;
        });
        return fut;
    }

    @Override
    public void close() { sinks.forEach( sink -> sink.close() );

}
