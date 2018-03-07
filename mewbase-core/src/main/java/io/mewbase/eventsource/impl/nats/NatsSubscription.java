package io.mewbase.eventsource.impl.nats;


import io.mewbase.eventsource.Subscription;

public class NatsSubscription implements Subscription {

    final io.nats.stan.Subscription subs;

    NatsSubscription(io.nats.stan.Subscription subs) {
        this.subs = subs;
    }

    @Override
    public void close() {
        subs.close();
    }

}
