package io.mewbase.binders.impl;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import java.util.Optional;
import java.util.function.BiConsumer;



public abstract class StreamableBinder {

    public final static String BINDER_NAME_KEY = "BinderName";
    public final static String DOCUMENT_ID_KEY = "DocumentID";
    public final static String DOCUMENT_CONTENT_KEY = "Document";

    public Optional<BiConsumer<String, BsonObject>> streamFunc = Optional.empty();

    public Boolean isStreaming() { return streamFunc.isPresent(); }

    public Boolean setStreaming(final EventSink sink, final String channel) {
        BiConsumer<String, BsonObject> func = (id, doc) -> {
            final BsonObject docAsEvent  =  new BsonObject().
                    put(BINDER_NAME_KEY,getName()).
                    put(DOCUMENT_ID_KEY,id ).
                    put(DOCUMENT_CONTENT_KEY,doc);
            sink.publishAsync(channel,docAsEvent);  // Best effort only
        };
        streamFunc = Optional.of(func);
        return true;
    }

    public abstract String getName();
}
