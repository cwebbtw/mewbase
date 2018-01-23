package io.mewbase.eventsource.impl.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;


public interface FileEventUtils {

    Logger logger = LoggerFactory.getLogger(FileEventUtils.class);

    static Path pathFromEventNumber(final long eventNumber) {
        return Paths.get(String.format("%016d", eventNumber));
    }

    static long eventNumberFromPath(final Path path) {
        return Long.parseLong(path.getFileName().toString());
    }

    /**
     * Look at all the files in the channel and return the event number of the next one
     * to be created.
     * Events are numbered starting from 1
     * @param channelPath
     * @return theNextValidEventNumber
     */
    static long nextEventNumberFromPath(final Path channelPath) throws IOException {
        return Files.list(channelPath)
                    .filter(f -> Files.isRegularFile(f))
                    .mapToLong(f -> Long.parseLong(f.toFile().getName()))
                    .max()
                    .orElse(0) + 1l;
    }


    /**
     * Given that a instant is in the past find the first event that is subsequent to that instant,
     * this may be the first event in the stream.
     * If the event in the future return the nextValid event number as above.
     * @param channelPath
     * @param startInstant
     * @return the first eventNumber subsequent to the point in time.
     * @throws IOException
     */
    static long eventNumberAfterInstant(final Path channelPath,final Instant startInstant) throws IOException {
        return Files.list(channelPath)
                .filter(f ->  Files.isRegularFile(f) && f.toFile().lastModified() > startInstant.toEpochMilli())
                .mapToLong(f -> Long.parseLong(f.toFile().getName()))
                .min()
                .orElse(nextEventNumberFromPath(channelPath));
    }


    /**
     * Ensure that a directory exists for a given channel name
     * and return a valid path for the channel
     */
    static Path ensureChannelExists(final Path baseDir, final String channelName) {
        final Path channelPath = baseDir.resolve(channelName);
        try {
            Files.createDirectories(channelPath);
        } catch (Exception exp) {
            logger.error("Error creating channel for subscription on channel "+channelName  ,exp);
        }
        return channelPath;
    }

}
