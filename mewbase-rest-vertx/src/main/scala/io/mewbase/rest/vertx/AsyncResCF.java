package io.mewbase.rest.vertx;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;

import java.util.concurrent.CompletableFuture;

class AsyncResCF<T> extends CompletableFuture<T> implements Handler<AsyncResult<T>> {

    @Override
    public void handle(AsyncResult<T> ar) {
        if (ar.succeeded()) {
            complete(ar.result());
        } else {
            completeExceptionally(ar.cause());
        }
    }

}
