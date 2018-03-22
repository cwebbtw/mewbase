package io.mewbase.eventsource.impl.http;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;

import io.mewbase.eventsource.impl.file.FileEventSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.TreeMap;
import java.util.concurrent.CompletionException;

import static io.mewbase.eventsource.impl.file.FileEventUtils.*;
import static java.lang.Long.max;


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

}


    @Override
    public Subscription subscribe(String channelName, EventHandler eventHandler) {
            try {
                return new HttpEventSubscription(String host, int port, String channel, )
            } catch (Exception exp) {
                throw new CompletionException(exp);
            }
        }


    @Override
    public Subscription subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        // TODO

    }

    @Override
    public Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        // TODO
    }

    @Override
    public Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        // TODO
    }

    @Override
    public Subscription subscribeAll(String channelName, EventHandler eventHandler) {
        // TODO
    }

    @Override
    public void close() {
       // Todo - Shut down stream
    }

}
