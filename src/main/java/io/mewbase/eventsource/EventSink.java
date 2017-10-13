package io.mewbase.eventsource;


import io.mewbase.bson.BsonObject;

import java.util.concurrent.CompletableFuture;

public interface EventSink {

    /**
     * Publish an Event in the form of a byte array to a named channel.
     * Any new Events that arrive at the source will be sent to the event handler.
     *
     * This function is intended to block until the associated EventSink implemetnation
     * acknowledges that the event as been received. For async Events please see the
     * @AsyncEventSink
     *
     * @param channelName
     * @param event as a BsonObject.
     */
    void publish(String channelName, BsonObject event);


    /**
     * Publish an Event in the form of a byte array to a named channel returning a
     * CompletableFuture Future that at some point in the future will complete returning
     * the event that was successfully sent or will terminate Exceptionally with the
     * associated Exception.
     *
     * For blocking version see @EventSink
     *
     * @param channelName String of the channel name.
     * @param event as a BsonObject.
     */
    CompletableFuture<BsonObject> publishAsync(String channelName, BsonObject event);


    /**
     * Close down this EventSink and all its associated resources
     */
    void close();

}
