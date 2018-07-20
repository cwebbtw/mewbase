package io.mewbase.eventsource;

import io.mewbase.bson.BsonObject;

import java.time.Instant;


public interface Event {

    BsonObject getBson();

    Instant getInstant();

    Long getEventNumber();

    Long getCrc32();

    default  String asString() {
        return "TimeStamp : " + this.getInstant() +
                " EventNumber : " + this.getEventNumber() +
                " PayLoad : " + this.getBson();
    }

}
