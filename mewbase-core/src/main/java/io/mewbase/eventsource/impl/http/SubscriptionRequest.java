package io.mewbase.eventsource.impl.http;

import io.mewbase.bson.BsonObject;


import java.time.Instant;


public class SubscriptionRequest {

    private static final String CHANNEL_TAG = "channel";
    private static final String SUBS_TYPE_TAG = "type";
    private static final String START_EVENT_TAG = "startEvent";
    private static final String START_INSTANT_TAG = "startInstant";


    public enum SubscriptionType {
        FromNow,
        FromMostRecent,
        FromEventNumber,
        FromInstant,
        FromStart
    }

    public final String channel;
    public final SubscriptionType type;
    public final Long startInclusive;
    public final Instant startInstant;


    public SubscriptionRequest(String channel, SubscriptionType type, Long startInclusive , Instant startInstant ) {
        this.channel = channel;
        this.type = type;
        this.startInclusive = startInclusive;
        this.startInstant = startInstant;
    }

    public SubscriptionRequest(BsonObject bson) {
        this.channel = bson.getString(CHANNEL_TAG);
        this.type = SubscriptionType.valueOf(bson.getString(SUBS_TYPE_TAG));
        this.startInclusive = bson.getLong(START_EVENT_TAG);
        this.startInstant = bson.getInstant(START_INSTANT_TAG);
    }


    public BsonObject toBson() {
        BsonObject bson = new BsonObject()
                .put(CHANNEL_TAG, channel)
                .put(SUBS_TYPE_TAG, type.name())
                .put(START_EVENT_TAG, startInclusive)
                .put(START_INSTANT_TAG, startInstant);
        return bson;
    }

    @Override
    public String toString() {
        return  " Channel :" + channel +
                " Type :" + type.name();
    }
}


