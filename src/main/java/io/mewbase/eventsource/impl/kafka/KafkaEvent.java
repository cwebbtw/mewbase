package io.mewbase.eventsource.impl.kafka;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.netty.buffer.ByteBuf;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Instant;

public class KafkaEvent implements Event {

    final long eventNumber;
    final long epochMillis;
    final long crc32;
    final byte[] eventBuf;

    BsonObject event = null;

    public KafkaEvent(ConsumerRecord<String, byte[]> rec) {

        eventNumber = rec.offset();
        epochMillis = rec.timestamp();
        crc32 = 0l;     // Todo compute crc val on ingest
        eventBuf = rec.value();

    }

    @Override
    public BsonObject getBson() {
        if (event == null) event = new BsonObject(eventBuf);
        return event;
    }

    @Override
    public Instant getInstant() {
        return Instant.ofEpochMilli(epochMillis);
    }

    @Override
    public Long getEventNumber() {
        return eventNumber;
    }

    @Override
    public Long getCrc32() {
        return crc32;
    }

    @Override
    public String toString() {
        return "TimeStamp : " + this.getInstant() +
                " EventNumber : " + this.getEventNumber() +
                " CRC32 : " + this.getCrc32() +
                " PayLoad : " + this.getBson();
    }

}

