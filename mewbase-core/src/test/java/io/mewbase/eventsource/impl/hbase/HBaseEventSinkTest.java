package io.mewbase.eventsource.impl.hbase;

import io.mewbase.MewbaseTestBase;
import io.mewbase.eventsource.EventSink;
import org.junit.Test;

import java.io.IOException;


public class HBaseEventSinkTest extends MewbaseTestBase {

    @Test  // Requires HBase to be running see mewbase wiki
    public void testCreateHBaseEventSink() throws IOException {

        EventSink hbSink = new HBaseEventSink();

    }


}
