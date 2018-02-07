package io.mewbase.eventsource.impl.kafka;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;


public class KafkaEventSource implements EventSource {

    private final static Logger logger = LoggerFactory.getLogger(KafkaEventSource.class);

    private final static int partitionZeroOnly = 0;

    private final Properties kafkaConsumerProps;

    private final List<Subscription> subs = new LinkedList<>();

    public KafkaEventSource() {
        this( ConfigFactory.load() );
    }

    public KafkaEventSource(Config cfg) {
        final String bootstrapServers = cfg.getString("mewbase.event.source.kafka.bootstrap");
        kafkaConsumerProps = new Properties();
        kafkaConsumerProps.put("bootstrap.servers", bootstrapServers);
        kafkaConsumerProps.put("enable.auto.commit", "false");
        logger.info("Set up KafkaEventSource");
    }


    @Override
    public Subscription subscribe(String channelName, EventHandler eventHandler) {
        TopicPartition partition0 = new TopicPartition(channelName, partitionZeroOnly);
        KafkaConsumer<String, byte[]> kafkaConsumer = createAndAssignConsumer(partition0);
        kafkaConsumer.seekToEnd(Arrays.asList(partition0));
        return createAndRegisterSubscription(kafkaConsumer,eventHandler);
    }

    @Override
    public Subscription subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        TopicPartition partition0 = new TopicPartition(channelName, partitionZeroOnly);
        KafkaConsumer<String, byte[]> kafkaConsumer = createAndAssignConsumer(partition0);
        kafkaConsumer.seekToEnd(Arrays.asList(partition0));
        final long offset = kafkaConsumer.position(partition0);
        kafkaConsumer.seek(partition0 , offset-1);
        return createAndRegisterSubscription(kafkaConsumer,eventHandler);
    }

    @Override
    public Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        TopicPartition partition0 = new TopicPartition(channelName, partitionZeroOnly);
        KafkaConsumer<String, byte[]> kafkaConsumer = createAndAssignConsumer(partition0);
        kafkaConsumer.seek(partition0 , startInclusive);      // to include this jump back one
        return createAndRegisterSubscription(kafkaConsumer,eventHandler);
    }

    @Override
    public Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        TopicPartition partition0 = new TopicPartition(channelName, partitionZeroOnly);
        KafkaConsumer<String, byte[]> kafkaConsumer = createAndAssignConsumer(partition0);
        java.util.Map<TopicPartition,java.lang.Long> timeForPartition0 = new HashMap(1);
        timeForPartition0.put(partition0,startInstant.toEpochMilli());
        OffsetAndTimestamp offsetAndTimestamp = kafkaConsumer.offsetsForTimes(timeForPartition0).get(partition0);
        kafkaConsumer.seek(partition0 , offsetAndTimestamp.offset());
        return createAndRegisterSubscription(kafkaConsumer,eventHandler);
    }

    @Override
    public Subscription subscribeAll(String channelName, EventHandler eventHandler) {
        TopicPartition partition0 = new TopicPartition(channelName, partitionZeroOnly);
        KafkaConsumer<String, byte[]> kafkaConsumer = createAndAssignConsumer(partition0);
        kafkaConsumer.seekToBeginning(Arrays.asList(partition0));
        return createAndRegisterSubscription(kafkaConsumer,eventHandler);
    }



    private KafkaConsumer<String, byte[]> createAndAssignConsumer(TopicPartition partition) {
        kafkaConsumerProps.put("group.id", UUID.randomUUID().toString());
        KafkaConsumer<String, byte[]> kafkaConsumer =
                        new KafkaConsumer<String, byte[]>(kafkaConsumerProps,
                                new org.apache.kafka.common.serialization.StringDeserializer(),
                                new org.apache.kafka.common.serialization.ByteArrayDeserializer());
        kafkaConsumer.assign(Arrays.asList(partition));
        return kafkaConsumer;
    }


    private Subscription createAndRegisterSubscription(final KafkaConsumer<String, byte[]> kafkaConsumer,
                                                       final EventHandler eventHandler) {
        Subscription sub = new KafkaEventSubscription(kafkaConsumer,eventHandler);
        subs.add(sub);
        return sub;
    }


    @Override
    public void close() {  subs.forEach( sub -> sub.close() );  }

}


