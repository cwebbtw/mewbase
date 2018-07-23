package io.mewbase.eventsource.impl.hbase;

import io.mewbase.MewbaseTestBase;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;



public class HBaseEventSourceTest extends MewbaseTestBase {

    @Test  // Requires HBase to be running see mewbase wiki
    public void testCreateHBaseEventSource() throws IOException {

        EventSource hbSource = new HBaseEventSource();
        hbSource.close();
    }


    @Test  // Requires HBase to be running see mewbase wiki
    public void testSingleEvent() throws IOException {

        EventSink hbSink = new HBaseEventSink();

        final String channelName = UUID.randomUUID().toString();
        final String dataValue = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put("data", dataValue);
        hbSink.publishSync(channelName, bsonEvent);

        // TODO now try to get it back

        hbSink.close();
    }


}
