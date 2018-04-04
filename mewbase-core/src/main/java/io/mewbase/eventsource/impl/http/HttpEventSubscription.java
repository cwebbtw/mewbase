package io.mewbase.eventsource.impl.http;

import io.mewbase.eventsource.EventHandler;

import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.EventDispatcher;

import io.vertx.core.http.HttpClient;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class HttpEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(HttpEventSubscription.class);

    private final EventDispatcher<HttpEvent> dispatcher;

    // We use an HTTP Client per subscription such that the client can be closed
    // and the server can then use the disconnect shutdown logic
    private final HttpClient client;


    public HttpEventSubscription(final HttpClient httpClient,
                                 final SubscriptionRequest subsRequest,
                                 final EventHandler eventHandler) {

        this.client = httpClient;

        // an HttpEvent is an Event hence i -> i is identity.
        this.dispatcher = new EventDispatcher<>(i -> i, eventHandler);

        // TODO Check how vert.x handles request chunks
        client.post(HttpEventSource.subscribeRoute, response  ->
                response.bodyHandler(totalBuffer -> {
                    try {
                        dispatcher.dispatch(new HttpEvent(totalBuffer.getBytes()));
                    } catch (Exception exp) {
                        logger.error("Event subscription failed", exp);
                    }
                } )
        ).end(subsRequest.toBson().encode());
    }


    @Override
    public void close()  {
        client.close();
        // drain and stop the dispatcher.
        dispatcher.stop();
    }

}