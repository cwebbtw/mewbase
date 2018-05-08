package io.mewbase.eventsource.impl.file;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import io.mewbase.util.CanFailFutures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;

import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static io.mewbase.eventsource.impl.file.FileEventUtils.ensureChannelExists;
import static io.mewbase.eventsource.impl.file.FileEventUtils.eventNumberAfterInstant;
import static io.mewbase.eventsource.impl.file.FileEventUtils.nextEventNumberFromPath;
import static java.lang.Long.max;


public class FileEventSource implements EventSource, CanFailFutures
{

    private final static Logger logger = LoggerFactory.getLogger(FileEventSource.class);

    private final Path baseDir;


    public FileEventSource() {
        this( ConfigFactory.load() );
    }

    public FileEventSource(Config cfg) {
        baseDir = Paths.get(cfg.getString("mewbase.event.source.file.basedir"));
        logger.info("Created File Event Source connection with base directory " + baseDir);
    }


    @Override
    public CompletableFuture<Subscription> subscribe(String channelName, EventHandler eventHandler) {
        Path channelPath = ensureChannelExists(baseDir,channelName);
            try {
                long next = nextEventNumberFromPath(channelPath);
                return new FileEventSubscription(channelPath, next, eventHandler).initialisingFuture;
            } catch (Exception exp) {
                return CanFailFutures.failedFuture(exp);
            }
        }


    @Override
    public CompletableFuture<Subscription>  subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        Path channelPath = ensureChannelExists(baseDir,channelName);
        try {
            long next = nextEventNumberFromPath(channelPath);
            long currentEventNumber = max(0L, next - 1);
            return new FileEventSubscription(channelPath, currentEventNumber, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription>  subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        try {
            final long startEvent = max(startInclusive,0L);
            Path channelPath = ensureChannelExists(baseDir,channelName);
            return new FileEventSubscription(channelPath, startEvent, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription>  subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        try {
            Path channelPath = ensureChannelExists(baseDir,channelName);
            long eventNumber = eventNumberAfterInstant(channelPath, startInstant);
            return new FileEventSubscription(channelPath,eventNumber, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public CompletableFuture<Subscription> subscribeAll(String channelName, EventHandler eventHandler) {
        try {
            Path channelPath = ensureChannelExists(baseDir,channelName);
            return new FileEventSubscription(channelPath,0L, eventHandler).initialisingFuture;
        } catch (Exception exp) {
            return CanFailFutures.failedFuture(exp);
        }
    }

    @Override
    public void close() {
       // Todo - Shut down stream
    }

}
