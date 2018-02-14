package io.mewbase.eventsource.impl;


import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.impl.kafka.KafkaEventSubscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.function.Function;

/**
 * EventDispatcher factors out the common element of receiving various types of EventSource specific event types and
 * 1) Transforming them into standard "Event" types and
 * 2) Calling the EventHandler given
 */
public class EventDispatcher<T> {

    private final static Logger logger = LoggerFactory.getLogger(EventDispatcher.class);

    private final Function<T, Event> evtTransformer;
    private final EventHandler evtHandler;

    private final LinkedBlockingQueue<Event> boundedBuffer = new LinkedBlockingQueue<>(16);

    private final Future dispFut;

    private Boolean closing = false;

    public EventDispatcher(Function<T, Event> evtTransformer, EventHandler evtHandler) {
        this.evtTransformer = evtTransformer;
        this.evtHandler = evtHandler;

        dispFut = Executors.newSingleThreadExecutor().submit( () -> {
            while (!closing ) {
                try {
                    evtHandler.onEvent(boundedBuffer.take());
                } catch (InterruptedException exp) {
                    closing = true;
                }
            }
            logger.info("Event dispatcher stopped.");
        });
    }

    /**
     * Dispatch will queue a number of events and then block the calling thread while the
     * backlog is cleared
     */
    public void dispatch(T specificRecord) throws InterruptedException {
        boundedBuffer.put(evtTransformer.apply(specificRecord));
    }

    public void stop() {
        dispFut.cancel(true);
    }

}
