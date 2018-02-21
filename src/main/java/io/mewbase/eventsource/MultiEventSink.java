package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.impl.MultiEventSinkImpl;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public interface MultiEventSink  {


    static MultiEventSink instance(Set<EventSink> sinks) {
        if (sinks.size() < 2) {
            throw new IllegalStateException("MultiEvent sink must have at least two embedded sinks.");
        }
        new MultiEventSinkImpl( sinks );
    }

    Stream<Long> publishSync(String channelName, BsonObject event);

    Future<Stream<Long>> publishAsync(String channelName, BsonObject event);

    void close();
}
