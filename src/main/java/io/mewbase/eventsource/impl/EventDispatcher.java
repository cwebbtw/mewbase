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

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSubscription.class);

    private final Function<T, Event> evtTransformer;
    private final EventHandler evtHandler;

    private final LinkedBlockingQueue<Event> boundedBuffer = new LinkedBlockingQueue<>(16);
    private Boolean scheduleStop = false;
    private CountDownLatch stopped = new CountDownLatch(1);


    public EventDispatcher(Function<T, Event> evtTransformer, EventHandler evtHandler) {
        this.evtTransformer = evtTransformer;
        this.evtHandler = evtHandler;

        Executors.newSingleThreadExecutor().submit( () -> {
            while (!scheduleStop || !boundedBuffer.isEmpty()) {
                try {
                    evtHandler.onEvent(boundedBuffer.take());
                } catch (InterruptedException exp) {
                    logger.info("Event dispatcher interrupted");
                    scheduleStop = true;
                } catch (Exception exp) {
                    logger.error("Error event dispatching event", exp);
                }
            }
            stopped.countDown();
        });
    }


    /**
     * Dispatch will queue a number of events and then block the calling thread while the
     * backlog is cleared
     */
    public void dispatch(T specificRecord) throws InterruptedException {
        if (!scheduleStop) {
            boundedBuffer.put(evtTransformer.apply(specificRecord));
        }
    }

    public void stop() {
        scheduleStop = true;
        try {
            stopped.await(5, TimeUnit.SECONDS);
        } catch (Exception exp) {
            logger.error("Error draining event queue whilst stopping");
        }
    }

}
