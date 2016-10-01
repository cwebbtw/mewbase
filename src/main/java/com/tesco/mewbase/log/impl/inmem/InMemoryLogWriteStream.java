package com.tesco.mewbase.log.impl.inmem;

import com.tesco.mewbase.bson.BsonObject;
import com.tesco.mewbase.log.LogWriteStream;
import io.vertx.core.Handler;

import java.util.Queue;

/**
 * Created by tim on 27/09/16.
 */
public class InMemoryLogWriteStream implements LogWriteStream {

    private final Queue<BsonObject> queue;

    public InMemoryLogWriteStream(Queue<BsonObject> queue) {
        this.queue = queue;
    }

    @Override
    public void exceptionHandler(Handler<Throwable> handler) {

    }

    @Override
    public void write(BsonObject bsonObject) {
        queue.add(bsonObject);
    }

    @Override
    public void close() {

    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public void drainHandler(Runnable handler) {

    }
}