package io.mewbase.eventsource.impl.file;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import java.util.TreeMap;
import java.util.concurrent.CompletionException;

import static io.mewbase.eventsource.impl.file.FileEventUtils.ensureChannelExists;
import static io.mewbase.eventsource.impl.file.FileEventUtils.eventNumberAfterInstant;
import static io.mewbase.eventsource.impl.file.FileEventUtils.nextEventNumberFromPath;
import static java.lang.Long.max;


public class FileEventSource implements EventSource
{

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    private final TreeMap<String,FileEventChannel> channels = new TreeMap<>();

    private final Path baseDir;


    public FileEventSource() {
        this( ConfigFactory.load() );
    }

    public FileEventSource(Config cfg) {
        baseDir = Paths.get(cfg.getString("mewbase.event.source.file.basedir"));
        logger.info("Created FileEventSource connection with base directory " + baseDir);
    }


    @Override
    public Subscription subscribe(String channelName, EventHandler eventHandler) {
        Path channelPath = ensureChannelExists(baseDir,channelName);
            try {
                long next = nextEventNumberFromPath(channelPath);
                return new FileEventSubscription(channelPath, next, eventHandler);
            } catch (Exception exp) {
                throw new CompletionException(exp);
            }
        }


    @Override
    public Subscription subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        Path channelPath = ensureChannelExists(baseDir,channelName);
        try {
            long next = nextEventNumberFromPath(channelPath);
            return new FileEventSubscription(channelPath, max(0L, next - 1), eventHandler);
        } catch (Exception exp) {
            throw new CompletionException(exp);
        }
    }

    @Override
    public Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        final long startEvent = max(startInclusive,1l);
        Path channelPath = ensureChannelExists(baseDir,channelName);
        return new FileEventSubscription(channelPath, startEvent, eventHandler);
    }

    @Override
    public Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        Path channelPath = ensureChannelExists(baseDir,channelName);
        try {
            long eventNumber = eventNumberAfterInstant(channelPath, startInstant);
            return new FileEventSubscription(channelPath,eventNumber, eventHandler);
        } catch (Exception exp) {
            throw new CompletionException(exp);
        }
    }

    @Override
    public Subscription subscribeAll(String channelName, EventHandler eventHandler) {
        Path channelPath = ensureChannelExists(baseDir,channelName);
        return new FileEventSubscription(channelPath,1L, eventHandler);
    }

    @Override
    public void close() {
       // Todo - Shut down stream
    }

}
