package io.mewbase.eventsource.impl.http;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import java.util.concurrent.CompletionException;



public class HttpEventSource implements EventSource
{
    private final static Logger logger = LoggerFactory.getLogger(HttpEventSource.class);

    final static String HOSTNAME_CONFIG_PATH = "mewbase.event.source.http.hostname";
    final static String PORT_CONFIG_PATH = "mewbase.event.source.http.port";

    private final String hostname;
    private final int port;

    public HttpEventSource() {
        this( ConfigFactory.load() );
    }


    public HttpEventSource(Config cfg) {
        hostname = cfg.getString(HOSTNAME_CONFIG_PATH);
        port = cfg.getInt(PORT_CONFIG_PATH);
        logger.info("Created HTTP Event Sink for "+hostname+":"+port);
    }


    @Override
    public Subscription subscribe(final String channelName, final EventHandler eventHandler) {
            try {
                final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromNow;
                final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,Instant.EPOCH);
                return new HttpEventSubscription(hostname,  port,  subsRq, eventHandler );
            } catch (Exception exp) {
                throw new CompletionException(exp);
            }
        }


    @Override
    public Subscription subscribeFromMostRecent(final String channelName, final EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromMostRecent;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,Instant.EPOCH);
            return new HttpEventSubscription(hostname,  port,  subsRq, eventHandler );
        } catch (Exception exp) {
            throw new CompletionException(exp);
        }
    }

    @Override
    public Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromEventNumber;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName,subsType,startInclusive,Instant.EPOCH);
            return new HttpEventSubscription(hostname,  port,  subsRq, eventHandler );
        } catch (Exception exp) {
            throw new CompletionException(exp);
        }
    }

    @Override
    public Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromInstant;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,startInstant);
            return new HttpEventSubscription(hostname,  port,  subsRq, eventHandler );
        } catch (Exception exp) {
            throw new CompletionException(exp);
        }
    }

    @Override
    public Subscription subscribeAll(String channelName, EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromStart;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,Instant.EPOCH);
            return new HttpEventSubscription(hostname,  port,  subsRq, eventHandler );
        } catch (Exception exp) {
            throw new CompletionException(exp);
        }
    }

    @Override
    public void close() {
       // Todo - Shut down all streams
    }




}
