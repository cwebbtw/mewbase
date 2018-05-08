package io.mewbase.eventsource.impl.http;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;

import io.mewbase.util.CanFailFutures;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import java.util.concurrent.CompletableFuture;




public class HttpEventSource implements EventSource, CanFailFutures
{
    private final static Logger logger = LoggerFactory.getLogger(HttpEventSource.class);

    final static String HOSTNAME_CONFIG_PATH = "mewbase.event.source.http.hostname";
    final static String PORT_CONFIG_PATH = "mewbase.event.source.http.port";
    final static String URL_PREFIX_PATH = "mewbase.event.source.http.url.prefix";

    final public static String SUBSCRIBE_ROUTE = "/subscribe";

    private final HttpClientOptions options;
    private final Vertx vertx = Vertx.vertx();
    private final String subsURI;

    
    public HttpEventSource() {
        this( ConfigFactory.load() );
    }


    public HttpEventSource(Config cfg) {
        String hostname = cfg.getString(HOSTNAME_CONFIG_PATH);
        int port = cfg.getInt(PORT_CONFIG_PATH);
        subsURI = cfg.getString(URL_PREFIX_PATH) + SUBSCRIBE_ROUTE;

        options = new HttpClientOptions()
                .setDefaultHost(hostname)
                .setDefaultPort(port);
        // TODO lookup and set various security / protocol options here
        // TODO - replace with Java 10 SE Library for HttpClient when able
        logger.info("Created HTTP Event Source for "+hostname+":"+port+" on "+ subsURI);
    }


    @Override
    public CompletableFuture<Subscription> subscribe(final String channelName, final EventHandler eventHandler) {
            try {
                final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromNow;
                final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,Instant.EPOCH);
                return new HttpEventSubscription(vertx.createHttpClient(options), subsRq, eventHandler ).future;
            } catch (Exception exp) {   // Java 8 doesnt have CompletableFuture.failedFuture​(Throwable ex)
                return CanFailFutures.failedFuture(exp);
            }
        }


    @Override
    public CompletableFuture<Subscription>  subscribeFromMostRecent(final String channelName, final EventHandler eventHandler) {

        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromMostRecent;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,Instant.EPOCH);
            return new HttpEventSubscription(vertx.createHttpClient(options), subsRq, eventHandler).future;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription>  subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromEventNumber;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName,subsType,startInclusive,Instant.EPOCH);
            return new HttpEventSubscription(vertx.createHttpClient(options), subsRq, eventHandler ).future;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription>  subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromInstant;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,startInstant);
            return new HttpEventSubscription(vertx.createHttpClient(options), subsRq, eventHandler ).future;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription>  subscribeAll(String channelName, EventHandler eventHandler) {
        try {
            final SubscriptionRequest.SubscriptionType subsType = SubscriptionRequest.SubscriptionType.FromStart;
            final SubscriptionRequest subsRq = new SubscriptionRequest(channelName, subsType,0L,Instant.EPOCH);
            return new HttpEventSubscription(vertx.createHttpClient(options), subsRq, eventHandler ).future;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }


    @Override
    public void close() {
       // Todo - Shut down all streams
    }


}
