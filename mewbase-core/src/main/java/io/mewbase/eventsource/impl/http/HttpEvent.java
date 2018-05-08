package io.mewbase.eventsource.impl.http;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;

import java.time.Instant;


public class HttpEvent implements Event {

    final static String NUMBER_KEY = "EventNumber";
    final static String START_TIME_KEY = "EpochMillis";
    final static String CRC32_KEY = "Crc32";
    final static String EVENT_KEY = "Event";

    final long eventNumber;
    final long epochMillis;
    final long crc32;
    final BsonObject event;


    public HttpEvent(byte[] httpEventArray) {
        BsonObject boj = new BsonObject(httpEventArray);
        this.eventNumber = boj.getLong(NUMBER_KEY);
        this.epochMillis = boj.getLong(START_TIME_KEY);
        this.crc32 = boj.getLong(CRC32_KEY);
        this.event = boj.getBsonObject(EVENT_KEY);
    }


    public HttpEvent(Event that) {
        this.eventNumber = that.getEventNumber();
        this.epochMillis = that.getInstant().toEpochMilli();
        this.crc32 = that.getCrc32();
        this.event = that.getBson();
    }


    @Override
    public BsonObject getBson() {
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

    public BsonObject toBson() {
       return new BsonObject()
                .put(NUMBER_KEY, getEventNumber())
                .put(START_TIME_KEY, getInstant().toEpochMilli())
                .put(CRC32_KEY, getCrc32())
                .put(EVENT_KEY, getBson());
    }

}