package io.mewbase.eventsource.impl.file;



import io.mewbase.bson.BsonObject;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;

import java.util.Comparator;
import java.util.Optional;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;


public class FileEventChannel  {

    private final static Logger logger = LoggerFactory.getLogger(FileEventChannel.class);

    private final Path channelPath;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicLong eventNumber;


    public FileEventChannel(final Path channelPath) {

        this.channelPath = channelPath;

        try {
            // create the directory if it doenst exist
            Files.createDirectories(channelPath);

            Optional<Path> mostRecentPath = Files.list(channelPath)
                    .filter(f -> !Files.isDirectory(f))
                    .max(Comparator.comparingLong(f -> f.toFile().lastModified()));

            if (mostRecentPath.isPresent()) {
                eventNumber = new AtomicLong(FileEvent.eventNumberFromPath(mostRecentPath.get()));
            } else {
                eventNumber = new AtomicLong();
            }
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
            Path fullPath = channelPath.resolve(FileEvent.pathFromEventNumber(eventNumber.incrementAndGet()));
            Files.createFile(fullPath); // throws exception if another process has just created this file.
            final long assignedEventNumber = eventNumber.get();
            lock.unlock();
            // we have the file set aside so write and close.
            Files.write(fullPath, event.encode().getBytes());
            return assignedEventNumber;
        } catch (FileAlreadyExistsException exp) {
            // just recurse and increment file number again
            return publish(event);
        } catch (Exception exp) {
            lock.unlock();
            logger.error("Error attempting publish event to File Event Channel", exp);
            throw exp;
        }

    }


}
