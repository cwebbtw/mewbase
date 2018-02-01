package io.mewbase.eventsource.impl.file;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;


public class FileEventSink implements EventSink {

    private final static Logger logger = LoggerFactory.getLogger(FileEventSink.class);

    private final TreeMap<String,FileEventChannel> channels = new TreeMap<>();

    private final Path baseDir;

    public FileEventSink() {
        this( ConfigFactory.load() );
    }

    public FileEventSink(Config cfg) {
        baseDir = Paths.get(cfg.getString("mewbase.event.sink.file.basedir"));
        logger.info("Created File Event Sink connection with base directory " + baseDir);
    }


    @Override
    public void publishSync(String channelName, BsonObject event) {
        FileEventChannel channel = channels.computeIfAbsent(channelName,
                    key -> new FileEventChannel(baseDir.resolve(key)));
        try {
            channel.publish(event);
        } catch (Exception exp) {
            logger.error("Error attempting publishSync event to FileEventSink", exp);
        }
    }

    @Override
    public CompletableFuture<BsonObject> publishAsync(final String channelName, final BsonObject event) {
        // Todo current impl needs work to write async
        publishSync(channelName,event);
        CompletableFuture<BsonObject> fut = CompletableFuture.supplyAsync( () -> event);
        return fut;
    }

    @Override
    public void close() {
       // nothing to see here
    }

}
