package io.mewbase.eventsource;

import com.typesafe.config.Config;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.vertx.ext.unit.junit.Repeat;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.concurrent.CountDownLatch;


/**
 * Created by Nige on 7/9/2017.
 */
@RunWith(VertxUnitRunner.class)
public class EventSourceTest extends MewbaseTestBase {

    @Test
    public void testConnectToEventSource() throws Exception {
        EventSource es = EventSource.instance(createConfig());
        es.close();
        assert (true);
    }


    @Test
    public void testSingleEvent() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestSingleEventChannel";
        final String inputUUID = randomString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        final CountDownLatch latch = new CountDownLatch(1);
        Subscription subs = source.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        long evtNum = event.getEventNumber();
                        Instant evtTime = event.getInstant();
                        Long evtHash = event.getCrc32();
                        latch.countDown();
                        }
                    );

        sink.publishSync(testChannelName, bsonEvent);
        latch.await();

        source.close();
        sink.close();
    }


    @Test
    public void testManyEventsInOrder() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestMultiEventChannel";
        final int START_EVENT_NUMBER = 0;
        final int END_EVENT_NUMBER = 127;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        source.subscribe(testChannelName, event -> {
                BsonObject bson =  event.getBson();
                long thisEventNum = END_EVENT_NUMBER - latch.getCount();
                assert(bson.getLong("num") == thisEventNum);
                latch.countDown();
        });

        EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, (long)END_EVENT_NUMBER);

        latch.await();

        source.close();
        sink.close();
    }


    @Test
    public void testMostRecent() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestMostRecentChannel";

        final int START_EVENT_NUMBER = 0;
        final long MID_EVENT_NUMBER = 64;
        final int END_EVENT_NUMBER = 127;

        final int eventsToTest = 63;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        final EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, (long)MID_EVENT_NUMBER);

        source.subscribeFromMostRecent(testChannelName, event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = MID_EVENT_NUMBER + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        utils.sendNumberedEvents(testChannelName,(long)MID_EVENT_NUMBER+1, (long)END_EVENT_NUMBER);

        latch.await();

        source.close();
        sink.close();
    }


    @Test
    public void testSubscribeFromEventNumber() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestFromNumberChannel";
        final int START_EVENT_NUMBER = 0;
        final long MID_EVENT_NUMBER = 64;
        final int END_EVENT_NUMBER = 127;

        final int eventsToTest = 64;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        final EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, (long)END_EVENT_NUMBER);

        source.subscribeFromEventNumber(testChannelName, MID_EVENT_NUMBER, event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = MID_EVENT_NUMBER + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        latch.await();

        source.close();
        sink.close();
    }

   @Test
    public void testSubscribeFromInstant() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestFromInstantChannel";
        final int START_EVENT_NUMBER = 0;
        final long MID_EVENT_NUMBER = 64;
        final int END_EVENT_NUMBER = 128;

        final int eventsToTest = 63;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);
        final EventSinkUtils utils =  new EventSinkUtils(sink);

        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, (long)MID_EVENT_NUMBER);

        Thread.sleep(10); // give the events time to rest in the event source

        Instant then = Instant.now();

        Thread.sleep(10); // some room the other side of the time window

        utils.sendNumberedEvents(testChannelName,(long)MID_EVENT_NUMBER+1, (long)END_EVENT_NUMBER);

        source.subscribeFromInstant(testChannelName, then, event -> {
            BsonObject bson  = event.getBson();
            long thisEventNum = MID_EVENT_NUMBER + 1 + (eventsToTest - latch.getCount());
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        latch.await();

        source.close();
        sink.close();
    }


    @Test
    public void testSubscribeAll() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestAllChannel";
        final int START_EVENT_NUMBER = 0;
        final long MID_EVENT_NUMBER = 64;
        final int END_EVENT_NUMBER = 128;

        final int eventsToTest = END_EVENT_NUMBER - START_EVENT_NUMBER;
        final CountDownLatch latch = new CountDownLatch(eventsToTest);

        final EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, MID_EVENT_NUMBER);

        source.subscribeAll(testChannelName,  event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = START_EVENT_NUMBER + eventsToTest - latch.getCount();
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        utils.sendNumberedEvents(testChannelName, MID_EVENT_NUMBER+1, (long)END_EVENT_NUMBER );

        latch.await();

        source.close();
        sink.close();
    }


    long events = 0;
    final long increment = 1000;
    // @Test
    // @Repeat(10)
    public void testManyEvents() throws Exception {
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        events += increment;

        final String testChannelName = "TestManyEvents";
        final EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)0, events);

        long start = Instant.now().toEpochMilli();
        source.subscribe(testChannelName,  event -> {
            BsonObject bson = event.getBson();
        });
        long end = Instant.now().toEpochMilli();
        System.out.println("Events:" + events + " Time " + (end-start) );
    }
}