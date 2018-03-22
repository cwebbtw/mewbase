package io.mewbase.eventsource.impl.http;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;


public class HttpEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(HttpEventSink.class);


    final static String HOSTNAME_CONFIG_PATH = "mewbase.event.sink.http.hostname";
    final static String PORT_CONFIG_PATH = "mewbase.event.sink.http.port";


    private final String hostname;
    private final int port;


    private final long syncWriteTimeOut = 5;


    public HttpEventSink() {
        this( ConfigFactory.load() );
    }


    public HttpEventSink(Config cfg) {
        hostname = cfg.getString(HOSTNAME_CONFIG_PATH);
        port = cfg.getInt(PORT_CONFIG_PATH);
        logger.info("Created HTTP Event Sink for "+hostname+":"+port);
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
        // TODO call into the Rest Client Library here
        return CompletableFuture.completedFuture(SADLY_NO_CONCEPT_OF_A_MESSAGE_NUMBER)
    }

    @Override
    public void close() {
       // connectionless so nothing to close.
    }


}
