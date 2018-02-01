package io.mewbase.eventsource.impl.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.impl.Utils;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;
import java.util.concurrent.CompletableFuture;


public class KafkaEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSink.class);

    private final KafkaProducer<Long, Byte[]> kafkaProducer;


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
        kafkaClientProps.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        kafkaClientProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProducer = new KafkaProducer<>(kafkaClientProps);
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


    private ProducerRecord<Long, Byte[]> producerRecord(final String channelName, final BsonObject event) {
        final byte [] eventBytes = event.encode().getBytes();
        return new ProducerRecord(channelName, Utils.checksum(eventBytes),eventBytes);
    }

    @Override
    public void close() {
        kafkaProducer.close();
    }

}


