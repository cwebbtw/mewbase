package io.mewbase.eventsource.impl.nats;

import io.nats.stan.Subscription;


public class NatsSubscription implements io.mewbase.eventsource.Subscription {

    final Subscription subs;

    NatsSubscription(Subscription subs) {
        this.subs = subs;
    }

    @Override
    public void close() {
        subs.close();
    }


}
