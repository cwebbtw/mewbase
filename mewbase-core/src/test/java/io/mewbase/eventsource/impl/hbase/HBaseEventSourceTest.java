package io.mewbase.eventsource.impl.hbase;


import io.mewbase.MewbaseTestBase;
import io.mewbase.bson.BsonObject;
import io.mewbase.eventsource.*;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
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


    @Test
    public void testSubscribeFromEventNumber() throws Exception {

        // use test local config
        final EventSink sink = new HBaseEventSink();
        final EventSource source = new HBaseEventSource();

        final String testChannelName = "TestFromNumberChannel"+UUID.randomUUID();
        final int START_EVENT_NUMBER = 0;
        final long MID_EVENT_NUMBER = 64;
        final int END_EVENT_NUMBER = 127;

        final int eventsToTest = 64;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        final EventSinkUtils utils =  new EventSinkUtils(sink);
        final List<Long> nums = new LinkedList<>();

        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, (long)END_EVENT_NUMBER);

        final CompletableFuture<Subscription> subFut = source.subscribeFromEventNumber(testChannelName, MID_EVENT_NUMBER, event -> {
            nums.add(event.getBson().getLong("num"));
            latch.countDown();
        });

        // will throw if the subscription doesnt set up in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        latch.await();
        // Todo check that the nums contains expected.

        sub.close();
        source.close();
        sink.close();
    }


    @Test
    public void testSubscribeFromInstant() throws Exception {

        final EventSink sink = new HBaseEventSink();
        final EventSource source = new HBaseEventSource();

        final String testChannelName = "TestFromInstantChannel"+UUID.randomUUID();;
        final long START_EVENT_NUMBER = 0;
        final long END_EVENT_NUMBER = 64;
        final long RESTART_EVENT_NUMBER = 65;
        final long REEND_EVENT_NUMBER = 128;

        final int expectedEvents = (int) (REEND_EVENT_NUMBER - RESTART_EVENT_NUMBER + 1);

        final CountDownLatch latch = new CountDownLatch(expectedEvents);
        final List<Long> nums = new LinkedList<>();

        final EventSinkUtils utils =  new EventSinkUtils(sink);

        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, END_EVENT_NUMBER);

        Thread.sleep(100); // give the events time to rest in the event source

        Instant then = Instant.now();

        Thread.sleep(10); // some room the other side of the time window

        utils.sendNumberedEvents(testChannelName,RESTART_EVENT_NUMBER,REEND_EVENT_NUMBER);

        final CompletableFuture<Subscription> subFut = source.subscribeFromInstant(testChannelName, then, event -> {
            nums.add(event.getBson().getLong("num"));
            latch.countDown();
        });

        // will throw if the subscription doesnt set up in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        latch.await();
        // Done - Checked that the correct number of event have occurred.


        sub.close();
        source.close();
        sink.close();
    }


}
