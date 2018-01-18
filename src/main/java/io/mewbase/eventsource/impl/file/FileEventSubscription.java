package io.mewbase.eventsource.impl.file;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.Queue;
import java.util.concurrent.*;

public class FileEventSubscription extends Subscription {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    // max queue size
    private final Queue<FileEvent> eventQueue = new LinkedBlockingQueue<>(100);

    //
    private Executor reader = Executors.newSingleThreadExecutor();
    private Executor dispatcher = Executors.newSingleThreadExecutor();

    private final Path channelPath;

    public FileEventSubscription(Path channelPath, final long firstEventNumber ,EventHandler eventHandler ) {

        this.channelPath = channelPath;

        // load the events from file
        reader.execute( () -> {
            long targetEvent = firstEventNumber;
            while (Thread.interrupted()) {
                eventQueue.add(load(targetEvent));
                targetEvent++;
            }
        });

        // process the events
        dispatcher.execute(  () -> {
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

    // This will block only the reading thread if it needs tp
    private Event load(long eventNumber) {
        Path eventFilePath = channelPath.resolve(FileUtils.pathFromEventNumber(eventNumber));
        BsonObject event = null;
        if (eventFilePath.toFile().exists()) {
            try {
                byte[] buffer = Files.readAllBytes(eventFilePath);
                event = new BsonObject(buffer);
            } catch (Exception exp) {
                logger.error("Error reading event " + eventNumber);
                throw new CompletionException(exp);
            }
        } else {
            // set up a watcher
            try (WatchService watcher = channelPath.getFileSystem().newWatchService() ) {
                watcher.take();
            }

        }


    }

    }