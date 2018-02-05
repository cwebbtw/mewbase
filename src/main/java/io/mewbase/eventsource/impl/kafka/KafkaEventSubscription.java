package io.mewbase.eventsource.impl.kafka;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Iterator;
import java.util.concurrent.*;


public class KafkaEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSubscription.class);

    // max queue size
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(100);

    private final ExecutorService reader = Executors.newSingleThreadExecutor();
    private Boolean readerRunning = true;
    private final ExecutorService  dispatcher = Executors.newSingleThreadExecutor();
    private Boolean dispatcherRunning = true;

    private final KafkaConsumer<String, byte[]> kafkaConsumer;

    public KafkaEventSubscription(final KafkaConsumer<String, byte[]> kafkaConsumer, EventHandler handler) {

        this.kafkaConsumer = kafkaConsumer;

       reader.submit(() -> {

            while (readerRunning) {
                try {
                    ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(10);
                    Iterator<ConsumerRecord<String, byte[]>> itr = records.iterator();
                    while (itr.hasNext()) {
                        Event evt = new KafkaEvent(itr.next());
                        eventQueue.put(evt);
                    }
                } catch (InterruptedException exp ) {
                    logger.info("Event reader thread closing");
                    readerRunning = false;
                } catch (Exception exp ) {
                    logger.error("Error in event reader", exp);
                }
            }
            kafkaConsumer.close();
        });

        // process the events
        dispatcher.submit(() -> {
            while (dispatcherRunning || !eventQueue.isEmpty()) {
                try {
                    handler.onEvent(eventQueue.take());
                } catch (InterruptedException exp) {
                    logger.info("Event reader thread closing");
                    dispatcherRunning = false;
                } catch (Exception exp) {
                    logger.error("Error in event handler", exp);
                }
            }
        });
        logger.info("Set up KafkaEventSubscription");
    }


    @Override
    public void close() {
        readerRunning = false;
        dispatcherRunning = false;
        reader.shutdown();
        dispatcher.shutdown();
        try {
            reader.awaitTermination(500, TimeUnit.MILLISECONDS);
            dispatcher.awaitTermination(500, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            reader.shutdownNow();
            dispatcher.shutdownNow();
        }
    }

}