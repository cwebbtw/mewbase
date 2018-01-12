package io.mewbase.eventsource.impl.file;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.eventsource.EventHandler;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;


import java.util.TreeMap;



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
        logger.info("Created File Event Sink connection with base directory " + baseDir);
    }


    @Override
    public Subscription subscribe(String channelName, EventHandler eventHandler) {
        // TODO - Find most recent event and add one
        return new FileEventSubscription(baseDir.resolve(channelName),0L, eventHandler);
    }

    @Override
    public Subscription subscribeFromMostRecent(String channelName, EventHandler eventHandler) {
        // TODO - Find most recent event.
        return new FileEventSubscription(baseDir.resolve(channelName),0L, eventHandler);
    }

    @Override
    public Subscription subscribeFromEventNumber(String channelName, Long startInclusive, EventHandler eventHandler) {
        return new FileEventSubscription(baseDir.resolve(channelName),startInclusive, eventHandler);
    }

    @Override
    public Subscription subscribeFromInstant(String channelName, Instant startInstant, EventHandler eventHandler) {
        // TODO get event number nearest to instant
        return new FileEventSubscription(baseDir.resolve(channelName),0L, eventHandler);
    }

    @Override
    public Subscription subscribeAll(String channelName, EventHandler eventHandler) {
        return new FileEventSubscription(baseDir.resolve(channelName),0L, eventHandler);
    }

    @Override
    public void close() {
       // Todo - Shut down stream
    }

}
