package io.mewbase.eventsource.impl.http;

import io.mewbase.eventsource.EventHandler;

import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.EventDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;


public class HttpEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(HttpEventSubscription.class);

    private final EventDispatcher<HttpEvent> dispatcher;

    private Boolean closing = false;

    public HttpEventSubscription(final String host,
                                 final int port,
                                 final SubscriptionRequest subsRequest,
                                 final EventHandler eventHandler) {



        // an HttpEvent is an Event hence i -> i is identity.
        this.dispatcher = new EventDispatcher<>( i -> i, eventHandler );

        Executors.newSingleThreadExecutor().submit( () -> {

            while (!closing) {
                try {
                    // dispatcher.dispatch(evt);
//                } catch (InterruptedException exp ) {
//                    closing = true;
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