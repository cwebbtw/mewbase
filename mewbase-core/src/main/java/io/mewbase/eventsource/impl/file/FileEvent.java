package io.mewbase.eventsource.impl.file;

import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.netty.buffer.ByteBuf;

import java.time.Instant;


public class FileEvent implements Event {

    final long eventNumber;
    final long epochMillis;
    final long crc32;
    final ByteBuf eventBuf;

    BsonObject event = null;

    public FileEvent(long eventNumber, long epochMillis, long crc32, ByteBuf eventBuf) {
        this.eventNumber = eventNumber;
        this.epochMillis = epochMillis;
        this.crc32 = crc32;
        this.eventBuf = eventBuf;
    }

    @Override
    public BsonObject getBson() {
        if (event == null) event = BsonCodec.bsonBytesToBsonObject(eventBuf.array());
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
