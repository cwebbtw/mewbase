package io.mewbase.eventsource.impl.nats;

import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.nats.stan.Message;
import io.vertx.core.buffer.Buffer;

import java.time.Instant;


class NatsEvent implements Event {

    final Message msg;

    NatsEvent(Message msg) {
        this.msg = msg;
    }

    @Override
    public BsonObject getBson() {
            return BsonCodec.bsonBytesToBsonObject(msg.getData());
    }

    @Override
    public Instant getInstant()  { return msg.getInstant(); }

    @Override
    public Long getEventNumber() { return msg.getSequence() - 1L;}  // Nats events start at 1 and we are 0 based.

    @Override
    public Long getCrc32() { return (long)msg.getCrc32(); }


    @Override
    public String toString() {
        return "TimeStamp : " + this.getInstant() +
                "EventNumber : " + this.getEventNumber() +
                "PayLoad : " + this.getBson();
    }

}
