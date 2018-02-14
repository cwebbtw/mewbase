package io.mewbase.eventsource;


import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.EventSink;

import java.util.stream.LongStream;


public class EventSinkUtils {

    final EventSink sink;

    public EventSinkUtils(final EventSink eventSink)  {
       sink = eventSink;
    }

    public void sendNumberedEvents(final String channelName, final Long startInclusive,final Long endInclusive) throws Exception {
        LongStream.rangeClosed(startInclusive,endInclusive).forEach( l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            try {
                sink.publishSync(channelName,bsonEvent);
            } catch(Exception exp) {
                // wrap and rethrow
                throw new RuntimeException(exp);
            }
        } );
    }



}
