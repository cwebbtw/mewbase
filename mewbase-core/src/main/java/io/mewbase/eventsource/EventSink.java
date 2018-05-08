package io.mewbase.eventsource;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.impl.file.FileEventSink;
import io.mewbase.util.CanFactoryFrom;

import java.util.concurrent.CompletableFuture;



public interface EventSink extends AutoCloseable {

    String factoryConfigPath = "mewbase.event.sink.factory";
    Long SYNC_WRITE_FAILED = -1l;
    Long SADLY_NO_CONCEPT_OF_A_MESSAGE_NUMBER = Long.MIN_VALUE;

    /**
     * Create an instance using the current config.
     * @return an Instance of an EventSink
     */
    static EventSink instance() {
        return EventSink.instance(ConfigFactory.load());
    }

    /**
     * Create an instance using the current config.
     * If the config fails it will create a NatsEventSink
     * @return an Instance of an EventSink
     */
    static EventSink instance(Config cfg) {
        return CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg, () -> new FileEventSink(cfg) );
    }


    /**
     * Publish an Event in the form of a byte array to a named channel.
     * Any new Events that arrive at the source will be sent to the event handler.
     *
     * This function is intended to block until the associated EventSink implementation
     * acknowledges that the event as been received. For async Events please see the
     * @EventSink:publishAsync
     *
     * @param channelName
     * @param event as a BsonObject.
     * @return the Event Number.
     */
    Long publishSync(String channelName, BsonObject event);

    /**
     * Publish an Event in the form of a byte array to a named channel returning a
     * CompletableFuture that at some point in the future will complete returning
     * the event that was successfully sent or will terminate Exceptionally with the
     * associated Exception.
     *
     * For blocking version see @EventSink:publishSync
     *
     * @param channelName String of the channel name.
     * @param event as a BsonObject.
     */
    CompletableFuture<Long> publishAsync(String channelName, BsonObject event);


    /**
     * Close down this EventSink and all its associated resources
     */
    void close();

}
