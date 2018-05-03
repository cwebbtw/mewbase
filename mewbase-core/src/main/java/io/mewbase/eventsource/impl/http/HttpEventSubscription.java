package io.mewbase.eventsource.impl.http;


import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;

import io.vertx.core.http.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;


public class HttpEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(HttpEventSubscription.class);

    // We use an HTTP Client per subscription such that the client can be closed
    // and the server can then use the disconnect shutdown logic
    private final HttpClient client;


    public final CompletableFuture<Subscription> future = new CompletableFuture<>();


    public HttpEventSubscription(final HttpClient httpClient,
                                 final SubscriptionRequest subsRequest,
                                 final EventHandler eventHandler)  {

        this.client = httpClient;


        client.post(HttpEventSource.SUBSCRIBE_ROUTE, response  -> {
            response.bodyHandler(totalBuffer -> {
                try {
                    // use the subscription UUID to set up a web socket for this
                    String subscriptionURI = totalBuffer.toString();
                    client.websocket("/"+subscriptionURI, new Handler<WebSocket>() {
                        @Override
                        public void handle(WebSocket websocket) {
                            websocket.frameHandler(frame -> {
                                HttpEvent evt = new HttpEvent(frame.binaryData().getBytes());
                                eventHandler.onEvent(evt);
                            });
                            // web socket is open and handler is bound so complete the future
                            future.complete(HttpEventSubscription.this);
                        }
                    });
                 } catch (Exception exp) {
                    logger.error("Websocket subsacription failed", exp);
                    future.completeExceptionally(exp);
                    }
                });
            }).end(subsRequest.toBson().encode());
    }


    @Override
    public void close()  {
        client.close();
        logger.info("Event subscription closed");
    }

}