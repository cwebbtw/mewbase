package io.mewbase.eventsource.impl.kafka;

import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.EventDispatcher;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;


public class KafkaEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSubscription.class);

    private final KafkaConsumer<String, byte[]> kafkaConsumer;
    private final EventDispatcher<ConsumerRecord<String, byte[]>> dispatcher;
    private final Future reader;


    public KafkaEventSubscription(final KafkaConsumer<String, byte[]> kafkaConsumer, EventHandler handler) {

        this.kafkaConsumer = kafkaConsumer;

        dispatcher = new EventDispatcher<ConsumerRecord<String, byte[]>>(
                (consumerRecord) -> new  KafkaEvent(consumerRecord),
                handler);

        reader = Executors.newSingleThreadExecutor().submit( () -> {
            while (!Thread.interrupted()) {
                try {
                    ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(10);
                     for (ConsumerRecord<String, byte[]> record : records ) {
                        dispatcher.dispatch(record);
                    }
                } catch (InterruptedException exp ) {
                    logger.info("Closing down Kafka Event Subscription");
                }
            }
            kafkaConsumer.close();
        });
        logger.info("Set up Kafka Event Subscription");
    }

    @Override
    public void close() {
        // cancel and interrupt the reader
        reader.cancel(true);
        // drain and stop the dispatcher
        dispatcher.stop();
    }

}