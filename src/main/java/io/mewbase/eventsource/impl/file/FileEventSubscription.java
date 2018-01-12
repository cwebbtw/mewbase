package io.mewbase.eventsource.impl.file;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;

import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class FileEventSubscription extends Subscription {

    // max queue size
    private final Queue<FileEvent> eventQueue = new LinkedBlockingQueue<>(100);

    //
    Executor reader = Executors.newSingleThreadExecutor();
    Executor exec = Executors.newSingleThreadExecutor();

    public FileEventSubscription(Path channelPath, final long firstEventNumber ,EventHandler eventHandler ) {

        // load the events from file
        D r = reader.execute( () -> {
            long targetEvent = firstEventNumber;
            while (Thread.interrupted()) {
                eventQueue.add(load(targetEvent));
                targetEvent++;
            }
        });

        // process the events
        exec.execute(  () -> {
            while (Thread.interrupted()) {
                // Todo Exception Handling
                eventHandler.onEvent(eventQueue.poll());
            }
        });

    }


    @Override
    public void unsubscribe() {

    }

    @Override
    public void close() {

    }


    private Event load(long eventNumber) {

    }