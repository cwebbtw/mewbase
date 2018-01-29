package io.mewbase.binders.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import java.util.Optional;
import java.util.function.Consumer;


public abstract class StreamableBinder {

    public Optional<Consumer<BsonObject>> streamFunc = Optional.empty();

    public Boolean isStreaming() { return streamFunc.isPresent(); }

    public Boolean setStreaming(final EventSink sink, final String channel) {
        Consumer<BsonObject> func = (bsonObject) -> sink.publishAsync(channel,bsonObject);
        streamFunc = Optional.of(func);
        return true;
    }


}
