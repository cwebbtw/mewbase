package io.mewbase.eventsource.impl.file;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Queue;
import java.util.concurrent.*;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;


public class FileEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    // max queue size
    private final Queue<Event> eventQueue = new LinkedBlockingQueue<>(100);

    private Executor reader = Executors.newSingleThreadExecutor();
    private Executor dispatcher = Executors.newSingleThreadExecutor();

    private final Path channelPath;

    public FileEventSubscription(final Path channelPath, final long firstEventNumber, final EventHandler eventHandler) {

        this.channelPath = channelPath;

        // load the events from file
        reader.execute(() -> {
            long targetEvent = firstEventNumber;
            while (Thread.interrupted()) {
                eventQueue.add(load(targetEvent));
                targetEvent++;
            }
        });

        // process the events
        dispatcher.execute(() -> {
            while (Thread.interrupted()) {
                try {
                    eventHandler.onEvent(eventQueue.poll());
                } catch (Exception exp) {
                    logger.error("Error handling event ", exp);
                }
            }
        });

    }

    @Override
    public void unsubscribe() {

    }

    @Override
    public void close() {

    }

    // This will block only the reading thread if it needs to
    private Event load(final long eventNumber) {
        Path eventFilePath = channelPath.resolve(FileUtils.pathFromEventNumber(eventNumber));
        if (eventFilePath.toFile().exists()) {
            try {
                return new FileEvent(eventFilePath.toFile());
            } catch (Exception exp) {
                logger.error("Error reading event " + eventNumber);
                throw new CompletionException(exp);
            }
        } else {
            // set up a watcher
            try (WatchService watcher = channelPath.getFileSystem().newWatchService()) {
                channelPath.register(watcher, ENTRY_CREATE);
                watcher.take();   // will block until something is created in the directory
                load(eventNumber);
            } catch (Exception exp) {
                logger.error("Error watching for new event " + eventNumber, exp);
                throw new CompletionException(exp);
            }

        }
        return null; // ttbomk unreachable
    }
}