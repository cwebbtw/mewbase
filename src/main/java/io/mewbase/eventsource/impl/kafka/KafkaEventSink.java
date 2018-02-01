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


public class KafkaEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSink.class);

    private final KafkaProducer<String, byte[]> kafkaProducer;

    public KafkaEventSink() {
        this( ConfigFactory.load() );
    }

    public KafkaEventSink(Config cfg) {
        final String bootstrapServer = cfg.getString("mewbase.event.sink.kafka.bootstrap");
        final Properties kafkaClientProps = new Properties();
        kafkaClientProps.put("bootstrap.servers", "localhost:9092");
        kafkaClientProps.put("acks", "all");
        kafkaClientProps.put("retries", 0);
        kafkaClientProps.put("batch.size", 64);
        kafkaClientProps.put("linger.ms", 0);   // will still batch for very near (in time records)
        kafkaClientProps.put("buffer.memory", 64 * 1024);
        kafkaProducer = new KafkaProducer<>(kafkaClientProps,
                new org.apache.kafka.common.serialization.StringSerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        logger.info("Set up KafkaEventSink " + kafkaProducer);
    }


    @Override
    public void publishSync(final String channelName, final BsonObject event) {
        try {
            kafkaProducer.send(producerRecord(channelName, event));
            kafkaProducer.flush();
        } catch (Exception exp) {
            logger.error("Error attempting publishSync synchronous event to Kafka", exp);
        }
    }

    @Override
    public CompletableFuture<BsonObject> publishAsync(final String channelName, final BsonObject event) {
        final CompletableFuture<BsonObject> fut = new CompletableFuture();
        kafkaProducer.send(producerRecord(channelName, event),
                (RecordMetadata recordMetadata, Exception exp) -> {
                    if (recordMetadata != null) fut.complete(event);
                    if (exp != null) fut.completeExceptionally(exp);
                    // if we got here we are in deep stuff
                    logger.error("Error attempting publishSync asynchronous event to Kafka", new IllegalStateException());
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


