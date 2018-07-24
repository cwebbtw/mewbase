package io.mewbase.eventsource.impl.hbase;


import io.mewbase.MewbaseTestBase;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.Event;
import io.mewbase.eventsource.EventSink;
import io.mewbase.eventsource.EventSource;
import io.mewbase.eventsource.Subscription;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


public class HBaseEventSourceTest extends MewbaseTestBase {

    @Test  // Requires HBase to be running see mewbase wiki
    public void testCreateHBaseEventSource() throws IOException {

        EventSource hbSource = new HBaseEventSource();
        hbSource.close();

    }


    @Test // Requires HBase to be running see mewbase wiki
    public void testSingleEvent() throws Exception {

        final EventSink sink = new HBaseEventSink();
        final EventSource source = new HBaseEventSource();

        final String testChannelName = "TestSingleEventChannel"+UUID.randomUUID();
        final String inputUUID = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        final long START_EVT_NUMBER = 0;

        final CountDownLatch latch = new CountDownLatch(1);

        final CompletableFuture<Event> evtRes = new CompletableFuture<>();

        final CompletableFuture<Subscription> subFut = source.subscribe(testChannelName,
                event -> {
                    evtRes.complete(event);
                    latch.countDown();
                }
        );

        // will throw if the subscription doesn't set up in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);
        sink.publishSync(testChannelName, bsonEvent);
        latch.await();

        // check that the event is well formed as returned from the function.
        final Event event = evtRes.get();
        final BsonObject bson  = event.getBson();
        assert( inputUUID.equals( bson.getString("data")) );
        final long evtNum = event.getEventNumber();
        assertEquals( START_EVT_NUMBER, evtNum );
        final Instant evtTime = event.getInstant();
        assertNotNull(evtTime);
        final Long evtHash = event.getCrc32();
        assertNotNull(evtHash);
        final String evtStr = event.asString();
        assertTrue(evtStr.contains("EventNumber : "+START_EVT_NUMBER));

        sub.close();
        source.close();
        sink.close();
    }



}
