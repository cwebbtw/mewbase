package io.mewbase.util;


import java.util.concurrent.CompletableFuture;


// Java 8 doesnt have CompletableFuture.failedFuture​(Throwable ex) so this gives something like
// via interface inheritance and type inference.
public interface CanFailFutures {

    static  <T> CompletableFuture<T> failedFuture(Exception exp) {
        final CompletableFuture<T> fut = new CompletableFuture<>();
        fut.completeExceptionally(exp);
        return fut;
    }

}
