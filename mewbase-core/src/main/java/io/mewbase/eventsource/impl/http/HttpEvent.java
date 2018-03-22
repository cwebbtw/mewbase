package io.mewbase.eventsource.impl.http;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;

import java.time.Instant;


class HttpEvent implements Event {

    final long eventNumber;
    final long epochMillis;
    final long crc32;
    final BsonObject event;

    public HttpEvent(byte[] httpEventArray) {
       BsonObject boj = new BsonObject(httpEventArray);
       this.eventNumber = boj.getLong("EventNumber");
       this.epochMillis = boj.getLong("EpochMillis");
       this.crc32 = boj.getLong("Crc32");
       this.event = boj.getBsonObject( "Event" );
    }

    @Override
    public BsonObject getBson() { return event; }

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
