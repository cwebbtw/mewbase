package io.mewbase.eventsource.impl.hbase;

import io.mewbase.MewbaseTestBase;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.EventSink;
import org.junit.Test;

import java.io.IOException;
import java.util.UUID;


public class HBaseEventSinkTest extends MewbaseTestBase {

    @Test  // Requires HBase to be running see mewbase wiki
    public void testCreateHBaseEventSink() throws IOException {

        EventSink hbSink = new HBaseEventSink();
        hbSink.close();

    }

    @Test  // Requires HBase to be running see mewbase wiki
    public void testSingleEvent() throws IOException {

        EventSink hbSink = new HBaseEventSink();

        //final String channelName = UUID.randomUUID().toString();
        final String channelName = "Dennis";
        final String dataValue = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put("data", dataValue);
        hbSink.publishSync(channelName, bsonEvent);
        hbSink.close();
    }

}
