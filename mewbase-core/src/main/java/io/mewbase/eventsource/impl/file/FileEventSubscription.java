package io.mewbase.eventsource.impl.file;


import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.Subscription;
import io.mewbase.eventsource.impl.EventDispatcher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.*;
import java.util.concurrent.*;


public class FileEventSubscription implements Subscription {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSubscription.class);

    private final Future reader;

    private final EventDispatcher<FileEvent> dispatcher;

    private final Path channelPath;

    private Boolean closing = false;

    public FileEventSubscription(final Path channelPath, final long firstEventNumber, final EventHandler eventHandler) {

        this.channelPath = channelPath;

        // a FileEvent is an Event hence i -> i is identity.
        this.dispatcher = new EventDispatcher<>( i -> i, eventHandler );

        reader = Executors.newSingleThreadExecutor().submit( () -> {
            long targetEvent = firstEventNumber;
            while (!closing) {
                try {
                    FileEvent evt = waitForEvent(targetEvent);
                    dispatcher.dispatch(evt);
                    targetEvent++;
                } catch (InterruptedException exp ) {
                    closing = true;
                } catch (Exception exp ) {
                    logger.error("Error in event reader",exp);
                }
            }
            logger.info("Subscription closed for channel "+ channelPath.getFileName());
        });
    }


    @Override
    public void close()  {
        reader.cancel(true);
        // drain and stop the dispatcher.
        dispatcher.stop();
    }

    // This will sleep only the reading thread
    // originally did this with a java.nio.WatchService but it was more complex and
    // did not allow fine grain control of the watchWindow.
    private final int WATCH_WINDOW_MILLIS = 3;
    private FileEvent waitForEvent(final long eventNumber) throws Exception {
        Path eventFilePath = channelPath.resolve(FileEventUtils.pathFromEventNumber(eventNumber));
        File eventFile = eventFilePath.toFile();
        logger.debug("Waiting for event " + eventNumber);
        while (! (eventFile.exists() && eventFile.length() > 0) ) {
            Thread.sleep( WATCH_WINDOW_MILLIS);
        }
        logger.debug("Got Event " + eventNumber);
        return FileEventUtils.fileToEvent( eventFilePath.toFile() );
    }

}