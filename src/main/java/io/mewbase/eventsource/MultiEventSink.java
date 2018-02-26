package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.impl.MultiEventSinkImpl;

import java.util.Arrays;
import java.util.HashSet;

import java.util.concurrent.Future;
import java.util.stream.Stream;


public interface MultiEventSink  {


    static MultiEventSink instance(EventSink... sinks) {
        if (sinks.length < 2) {
            throw new IllegalStateException("MultiEvent sink must have at least two embedded sinks.");
        }
        return new MultiEventSinkImpl( new HashSet<>(Arrays.asList(sinks)) );
    }

    Stream<Long> publishSync(String channelName, BsonObject event);

    Future<Stream<Long>> publishAsync(String channelName, BsonObject event);

    void close();
}
