package io.mewbase.eventsource.impl.http;

import java.time.Instant;

public class SubscriptionRequest {

    public enum SubscriptionType {
        FromNow,
        FromMostRecent,
        FromEventNumber,
        FromInstant,
        FromStart
    }

    final String channel;
    final SubscriptionType type;
    final Long startInclusive;
    final Instant startInstant;


    public SubscriptionRequest(String channel, SubscriptionType type, Long startInclusive , Instant startInstant ) {
        this.channel = channel;
        this.type = type;
        this.startInclusive = startInclusive;
        this.startInstant = startInstant;
    }



}


