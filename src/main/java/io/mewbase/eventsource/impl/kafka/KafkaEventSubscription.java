package io.mewbase.eventsource.impl.kafka;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.file.FileEventSource;
import io.mewbase.eventsource.impl.file.FileEventUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static com.sun.org.apache.xml.internal.security.keys.keyresolver.KeyResolver.iterator;


public class KafkaEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    // max queue size
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(100);

    private final ExecutorService reader = Executors.newSingleThreadExecutor();
    private final ExecutorService  dispatcher = Executors.newSingleThreadExecutor();


    public KafkaEventSubscription(final KafkaConsumer<String, byte[]> kafkaConsumer, EventHandler handler) {

        reader.execute(() -> {

            while (!Thread.interrupted()) {
                try {
                    ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(10);
                    Iterator<ConsumerRecord<String, byte[]>> itr = records.iterator();
                    while (itr.hasNext()) {
                        Event evt = new KafkaEvent(itr.next());
                        eventQueue.put(evt);
                    }
                } catch (InterruptedException exp ) {
                    logger.info("Event reader thread closing");
                } catch (Exception exp ) {
                    logger.error("Error in event reader", exp);
                }
            }
        });

        // process the events
        dispatcher.execute(() -> {
            while (!Thread.interrupted() || !eventQueue.isEmpty()) {
                try {
                    handler.onEvent(eventQueue.take());
                } catch (InterruptedException exp) {
                    logger.info("Event reader thread closing");
                } catch (Exception exp) {
                    logger.error("Error in event handler", exp);
                }
            }
        });
        logger.info("Set up KafkaEventSubscription");
    }

    @Override
    public void unsubscribe() {
        // Todo close kafka consumer
        reader.shutdown();
        dispatcher.shutdown();
    }

    @Override
    public void close() {
        unsubscribe();
    }

}