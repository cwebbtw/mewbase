package io.mewbase.eventsource.impl.http;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


public class HttpEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(HttpEventSink.class);

    final static String HOSTNAME_CONFIG_PATH = "mewbase.event.sink.http.hostname";
    final static String PORT_CONFIG_PATH = "mewbase.event.sink.http.port";
    final static String URL_PREFIX_PATH = "mewbase.event.sink.http.url.prefix";

    final public static String PUBLISH_ROUTE = "/publish";

    // Define the tags for the body payload
    public final static String EVENT_TAG = "event";
    public final static String CHANNEL_TAG = "channel";

    private final long syncWriteTimeOut = 10;

    private final HttpClient client;
    private final AtomicBoolean clientIsClosed = new AtomicBoolean(false);

    public final String publishURI;


    public HttpEventSink() {
        this( ConfigFactory.load() );
    }


    public HttpEventSink(Config cfg) {

        final String hostname = cfg.getString(HOSTNAME_CONFIG_PATH);
        final int port = cfg.getInt(PORT_CONFIG_PATH);

        final HttpClientOptions options = new HttpClientOptions()
                    .setDefaultHost(hostname)
                    .setDefaultPort(port);
                    // TODO lookup and set various security / protocol options here
                    // TODO - replace with Java 10 SE Library for HttpClient when able
        client = Vertx.vertx().createHttpClient(options);

        publishURI = cfg.getString(URL_PREFIX_PATH) + PUBLISH_ROUTE;

        logger.info("Created HTTP Event Sink for "+hostname+":"+port+" on "+publishURI);
    }


    @Override
    public Long publishSync(String channelName, BsonObject event) {
        CompletableFuture<Long> fut = publishAsync(channelName,event);
        try {
            return fut.get(syncWriteTimeOut,TimeUnit.SECONDS);
        } catch (Exception exp) {
            logger.error("Error attempting publishSync event to HttpEventSink", exp);
            return SYNC_WRITE_FAILED;
        }
    }


    @Override
    public CompletableFuture<Long> publishAsync(final String channelName, final BsonObject event) {

        BsonObject body = new BsonObject()
                .put(CHANNEL_TAG, channelName)
                .put(EVENT_TAG, event);

        CompletableFuture<Long> fut = new CompletableFuture<>();
        client.post(publishURI, response ->
                response.bodyHandler(totalBuffer -> {
                    try {
                        final String eventNumberAsString = totalBuffer.toString();
                        final Long eventNumber = Long.valueOf(eventNumberAsString);
                        fut.complete(eventNumber);
                    } catch (Exception exp) {
                        fut.completeExceptionally(exp);
                    }
                })
        ).end(body.encode());
        return fut;
    }


    @Override
    public void close() {
        if (!clientIsClosed.get()) {
            client.close();
            clientIsClosed.set(true);
        }
        logger.info("HttpEventSink closed");
    }


}
