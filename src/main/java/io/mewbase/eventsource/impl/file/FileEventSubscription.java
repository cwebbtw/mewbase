package io.mewbase.eventsource.impl.file;

import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.File;
import java.nio.file.*;
import java.util.concurrent.*;


public class FileEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    // max queue size
    private final BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>(100);

    private final ExecutorService reader = Executors.newSingleThreadExecutor();
    private final ExecutorService  dispatcher = Executors.newSingleThreadExecutor();

    private final Path channelPath;

    public FileEventSubscription(final Path channelPath, final long firstEventNumber, final EventHandler eventHandler) {

        this.channelPath = channelPath;

        reader.execute(() -> {
            long targetEvent = firstEventNumber;
            while (!Thread.interrupted()) {
                try {
                    Event evt = waitForEvent(targetEvent);
                    eventQueue.put(evt);
                    targetEvent++;
                } catch (InterruptedException exp ) {
                    logger.info("Event reader thread closing");
                } catch (Exception exp ) {
                    logger.error("Error in event reader",exp);
                }
            }
        });

        // process the events
        dispatcher.execute(() -> {
            while (!Thread.interrupted() || !eventQueue.isEmpty()) {
                try {
                    eventHandler.onEvent(eventQueue.take());
                } catch (InterruptedException exp) {
                    logger.info("Event reader thread closing");
                } catch (Exception exp) {
                    logger.error("Error in event handler", exp);
                }
            }
        });
    }


    @Override
    public void close() {
        reader.shutdown();
        dispatcher.shutdown();
    }

    // This will block only the reading thread
    // originally did this with a java.nio.WatchService but it was more complex and
    // did not allow fine grain control of the watchWindow.
    private final int WATCH_WINDOW_MILLIS = 3;
    private Event waitForEvent(final long eventNumber) throws Exception {
        Path eventFilePath = channelPath.resolve(FileEventUtils.pathFromEventNumber(eventNumber));
        File eventFile = eventFilePath.toFile();
        while (! (eventFile.exists() && eventFile.length() > 0) ) {
            Thread.sleep( WATCH_WINDOW_MILLIS);
        }
        return FileEventUtils.fileToEvent( eventFilePath.toFile() );
    }

}