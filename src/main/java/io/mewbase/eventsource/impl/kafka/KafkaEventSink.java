package io.mewbase.eventsource.impl.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class KafkaEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSink.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaEventSink() {
        this( ConfigFactory.load() );
    }

    public KafkaEventSink(Config cfg) {
        final String bootstrapServers = cfg.getString("mewbase.event.sink.kafka.bootstrap");
        final Properties kafkaClientProps = new Properties();
        kafkaClientProps.put("bootstrap.servers", bootstrapServers);
        kafkaClientProps.put("acks", "all");
        kafkaClientProps.put("retries", 0);
        kafkaClientProps.put("batch.size", 16);
        kafkaClientProps.put("linger.ms", 0);   // will still batch for very near (in time records)
        kafkaClientProps.put("buffer.memory", 64 * 1024);
        kafkaProducer = new KafkaProducer<>(kafkaClientProps,
                new org.apache.kafka.common.serialization.StringSerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        logger.info("Set up KafkaEventSink " + kafkaProducer);
    }


    @Override
    public Long publishSync(final String channelName, final BsonObject event) {

            CompletableFuture<Long> fut = publishAsync(channelName,event);
            kafkaProducer.flush();
            try {
                return fut.get(5, TimeUnit.SECONDS);
            } catch(Exception exp ) {
                logger.error("Synchronous publish failed to write event " + event, exp);
                return -1l;
            }
    }

    @Override
    public CompletableFuture<Long> publishAsync(final String channelName, final BsonObject event) {
        final CompletableFuture<Long> fut = new CompletableFuture();
        kafkaProducer.send(producerRecord(channelName, event),
                (RecordMetadata recordMetadata, Exception exp) -> {
                    if (recordMetadata != null) fut.complete(recordMetadata.offset());
                    if (exp != null) fut.completeExceptionally(exp);
                    // strange possible case if both are null
                    if (!fut.isDone()) fut.completeExceptionally(new IllegalStateException());
                    }
                );
        return fut;
    }


    /**
     * The key will essentially delimit partitioning for our purposes the consumer needs to see all events
     * in order with the correct event number so all event on a topic need to go in the same partition.
     * Assumption if we set the key to the same name as the parition then we can ensure it hashes to the same
     * parition is the broker.
     * BTW we can still replicate the broker for FT purposes.
     * @param channelName
     * @param event
     * @return
     */
    private ProducerRecord<String, byte[]> producerRecord(final String channelName, final BsonObject event) {
        final byte [] eventBytes = event.encode().getBytes();
        return new ProducerRecord<>(channelName, channelName, eventBytes);
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

}


