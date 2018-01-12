package io.mewbase.eventsource.impl.file;

import com.typesafe.config.Config;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.Set;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;


/**
 * Created by Nige on 10/1/2018.
 */
@RunWith(VertxUnitRunner.class)
public class FileEventSinkTest extends MewbaseTestBase {


    @Test
    public void testSinglePublishWritesEvent() throws Exception {

        final Config cfg = createConfig();
        final EventSink es = new FileEventSink(cfg);
        final String eventPath = cfg.getString("mewbase.event.sink.file.basedir" );

        final String channelName = "channel";
        final BsonObject evt = new BsonObject().put("key","value");
        es.publish(channelName,evt);

        // check that the file has been written
        final long eventNumber = 0;
        final Path path = Paths.get(eventPath,channelName, FileUtils.pathFromEventNumber(eventNumber).toString());
        final File eventFile  = path.toFile();
        assertTrue(eventFile.exists());
        assertFalse(eventFile.isDirectory());

    }

    @Test
    public void testMultiPublishWritesEvent() throws Exception {

        final Config cfg = createConfig();
        final EventSink es = new FileEventSink(cfg);
        final String eventPath = cfg.getString("mewbase.event.sink.file.basedir" );

        final String channelName = "channel";
        final BsonObject evt = new BsonObject().put("key","value");
        IntStream.range(0, 100).forEach( i -> es.publish(channelName,evt.put("evt",""+i)) );

        // check that each file has been written
        Set<Path> files =  Files.list(Paths.get(eventPath,channelName)).collect(Collectors.toSet());
        IntStream.range(0, 100).forEach( i -> {
            final String eventFileName = FileUtils.pathFromEventNumber(i).toString();
            final Path path = Paths.get(eventPath, channelName, eventFileName);
            assertTrue(files.contains(path));
        });
    }



}
