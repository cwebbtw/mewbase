package io.mewbase.eventsource.impl.http;


import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.EventDispatcher;
import io.mewbase.eventsource.impl.file.FileEvent;
import io.mewbase.eventsource.impl.file.FileEventSource;
import io.mewbase.eventsource.impl.file.FileEventUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


public class HttpEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    private final EventDispatcher<HttpEvent> dispatcher;

    private final Path

    private Boolean closing = false;

    public HttpEventSubscription(final String host,
                                 final int port,
                                 final String channel,
                                 final EventHandler eventHandler) {



        // an HttpEvent is an Event hence i -> i is identity.
        this.dispatcher = new EventDispatcher<>( i -> i, eventHandler );

        Executors.newSingleThreadExecutor().submit( () -> {

            while (!closing) {
                try {
                    // dispatcher.dispatch(evt);
                } catch (InterruptedException exp ) {
                    closing = true;
                } catch (Exception exp ) {
                    logger.error("Error in event reader",exp);
                }
            }

        });
    }


    @Override
    public void close()  {
  //
        // drain and stop the dispatcher.
        dispatcher.stop();
    }


}