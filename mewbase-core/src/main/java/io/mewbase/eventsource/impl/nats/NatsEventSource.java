package io.mewbase.eventsource.impl.nats;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;

import io.nats.stan.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;


/**
 * An Event Source implemented by the Nats Streaming Server.
 */

public class NatsEventSource implements EventSource {

    private final static Logger logger = LoggerFactory.getLogger(NatsEventSource.class);

    private final Connection nats;

    public NatsEventSource() {
        this(ConfigFactory.load() );
    }

    public NatsEventSource(Config cfg) {

        final String userName = cfg.getString("mewbase.event.source.nats.username");
        final String clusterName = cfg.getString("mewbase.event.source.nats.clustername");
        final String url = cfg.getString("mewbase.event.source.nats.url");

        final ConnectionFactory cf = new ConnectionFactory(clusterName,userName);
        cf.setNatsUrl(url);

        try {
            String clientUUID = UUID.randomUUID().toString();
            cf.setClientId(clientUUID);
            nats = cf.createConnection();
            logger.info("Created Nats EventSource connection with client UUID " + clientUUID);
        } catch (Exception exp) {
            logger.error("Error connecting to Nats Streaming Server", exp);
            throw new RuntimeException(exp);
        }
    }


    @Override
    public CompletableFuture<Subscription> subscribe(String channelName, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().build();
        return subscribeWithOptions( channelName, eventHandler, opts);
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startWithLastReceived().build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        // Nats events start at 1 and we are 0 based.
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startAtSequence(startInclusive+1L).build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }

    @Override
    public CompletableFuture<Subscription> subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().startAtTime(startInstant).build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }

    @Override
    public CompletableFuture<Subscription> subscribeAll(String channelName, EventHandler eventHandler) {
        SubscriptionOptions opts = new SubscriptionOptions.Builder().deliverAllAvailable().build();
        return subscribeWithOptions( channelName, eventHandler, opts );
    }



    private CompletableFuture<Subscription> subscribeWithOptions(String channelName, EventHandler eventHandler, SubscriptionOptions opts) {

        MessageHandler handler = message -> eventHandler.onEvent(new NatsEvent(message));

        CompletableFuture<Subscription> fut = new CompletableFuture<>();
        try {
            fut.complete( new NatsSubscription( nats.subscribe(channelName, handler, opts) ) );
        } catch (Exception exp) {
            logger.error("Error attempting to subscribe to Nats Streaming Server", exp);
            fut.completeExceptionally(exp);
        }
        return fut;
    }




    @Override
    public void close() {
        try {
            nats.close();
        } catch (Exception exp) {
            logger.error("Error attempting close Nats Streaming Server Event source", exp);
        }
    }

}
