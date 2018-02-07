package io.mewbase.eventsource.impl.file;

import io.mewbase.bson.BsonObject;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;


import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static java.nio.file.StandardOpenOption.*;


public class FileEventChannel  {

    private final static Logger logger = LoggerFactory.getLogger(FileEventChannel.class);

    private final Path channelPath;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong nextEventNumber;


    public FileEventChannel(final Path channelPath) {

        this.channelPath = channelPath;

        try {
            // create the directory if it doesnt exist
            Files.createDirectories(channelPath);
            nextEventNumber = new AtomicLong(FileEventUtils.nextEventNumberFromPath(channelPath));
            logger.info("Created File Event Channel at path " + channelPath);
        } catch (Exception exp) {
            logger.error("Error creating File Event Channel", exp);
            throw new RuntimeException(exp);
        }
    }


    public long publish(final BsonObject event) throws IOException {
        try {
            // stop racing across threads and use the file create lock across processes
            lock.lock();
            final long assignedEventNumber = nextEventNumber.getAndIncrement();
            Path fullPath = channelPath.resolve(FileEventUtils.pathFromEventNumber(assignedEventNumber));
            Files.write(fullPath, FileEventUtils.eventToByteArray(event),
                    CREATE_NEW, // throws exception if another process has just created this file.
                    WRITE,      // going to write into the file
                    SYNC        // write contents and meta data on sync
                    );
            lock.unlock(); // we have the file set aside so write and close.

            return assignedEventNumber;
        } catch (FileAlreadyExistsException exp) {
            // just recurse and increment file number again
            return publish(event);
        } catch (Exception exp) {
            lock.unlock();
            logger.error("Error attempting publishSync event to File Event Channel", exp);
            throw exp;
        }
    }

}
