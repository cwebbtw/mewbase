package io.mewbase.eventsource;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.eventsource.impl.SourceShim;
import io.mewbase.eventsource.impl.file.FileEventSource;
import io.mewbase.util.CanFactoryFrom;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;



public interface EventSource extends AutoCloseable {

    String factoryConfigPath = "mewbase.event.source.factory";

    /**
     * Create an instance of an EventSource given the current configuration in the environment.
     *
     * @return Instance of an EventSource, implemented as configured in the configuration.
     */
    static EventSource instance() {
        return EventSource.instance(ConfigFactory.load());
    }

    /**
     * Create an instance of an EventSource given the configuration supplied as a parameter.
     * Default is to use the FileEventSource if nothing is supplied in the config.
     *
     * @return Instance of an EventSource, implemented as configured in the configuration.
     */
    static EventSource instance(Config cfg) {
        EventSource impl = CanFactoryFrom.instance(cfg.getString(factoryConfigPath), cfg, () -> new FileEventSource(cfg) );
        return new SourceShim(impl);
    }

    /**
     * Subscribe to a named channel with the given event handler.
     * Any new Events that arrive at the source will be sent to the event handler.
     * @param channelName
     * @param eventHandler
     * @return CompletableFuture<Subscription>
     */
    CompletableFuture<Subscription> subscribe(String channelName, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler resulting the most recent event ( == highest event
     * number) being available first and then any new events that may arrive.
     *
     * @param channelName
     * @param eventHandler
     * @return CompletableFuture<Subscription>
     */
    CompletableFuture<Subscription> subscribeFromMostRecent(String channelName, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler.
     * Replay the events from the given number and then any new events that may arrive.
     * @param channelName
     * @param eventHandler
     * @return CompletableFuture<Subscription>
     */
    CompletableFuture<Subscription> subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler
     * Replay the events froma given time (Instant) and then any new events that may arrive.
     * @param channelName
     * @param startInstant
     * @param eventHandler
     * @return CompletableFuture<Subscription>
     */
    CompletableFuture<Subscription> subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler);

    /**
     * Subscribe to a named channel with the given event handler resulting in all the events
     * since the start of the channel being returned and then any new events that may arrive.
     *
     * NOTE : This may result in a very large data set being returned on the stream, use with
     * caution.
     *
     * @param channelName
     * @param eventHandler
     * @return CompletableFuture<Subscription>
     */
    CompletableFuture<Subscription> subscribeAll(String channelName, EventHandler eventHandler);

    /**
     * Close the connection to this EventSource forcing closer of any currently running subscriptions
     *
     * Generally intended to be used at program termination for example in shutdown hooks.
     */
    void close();

}
