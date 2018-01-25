package io.mewbase.eventsource;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.impl.file.FileEventSource;
import io.mewbase.util.CanFactoryFrom;

import java.time.Instant;


public interface EventSource {

    String factoryConfigPath = "mewbase.event.source.factory";

    static EventSource instance() {
        return EventSource.instance(ConfigFactory.load());
    }

    static EventSource instance(Config cfg) {
        return CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg, () -> new FileEventSource(cfg) );
    }

    /**
     * Subscribe to a named channel with the given event handler.
     * Any new Events that arrive at the source will be sent to the event handler.
     * @param channelName
     * @param eventHandler
     * @return
     */
    Subscription subscribe(String channelName, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler resulting the most recent event ( == highest event
     * number) being available first and then any new events that may arrive.
     *
     * @param channelName
     * @param eventHandler
     * @return
     */
    Subscription subscribeFromMostRecent(String channelName, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler.
     * Replay the events from the given number and then any new events that may arrive.
     * @param channelName
     * @param eventHandler
     * @return
     */
    Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler
     * Replay the events froma given time (Instant) and then any new events that may arrive.
     * @param channelName
     * @param startInstant
     * @param eventHandler
     * @return
     */
    Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler resulting in all the events
     * since the start of the channel being returned and then any new events that may arrive.
     *
     * NOTE : This may result in a very large data set being returned on the stream, use with
     * caution.
     *
     * @param channelName
     * @param eventHandler
     * @return
     */
    Subscription subscribeAll(String channelName, EventHandler eventHandler);


    void close();

}
