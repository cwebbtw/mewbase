package io.mewbase.eventsource;

import com.typesafe.config.Config;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


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
        final String inputUUID = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        final CountDownLatch latch = new CountDownLatch(1);
        final CompletableFuture<Subscription> subFut = source.subscribe(testChannelName, event -> {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        long evtNum = event.getEventNumber();
                        Instant evtTime = event.getInstant();
                        Long evtHash = event.getCrc32();
                        latch.countDown();
                        }
                    );

        // will throw if the subscription doesnt set uop in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        sink.publishSync(testChannelName, bsonEvent);

        latch.await();

        sub.close();
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
        final long START_EVENT_NUMBER = 0;
        final long END_EVENT_NUMBER = 128;

        final int expectedEvents = (int) (END_EVENT_NUMBER + 1);

        final CountDownLatch latch = new CountDownLatch(expectedEvents);
        final List<Long> nums = new LinkedList<>();

        final CompletableFuture<Subscription> subFut =  source.subscribe(testChannelName, event -> {
            nums.add(event.getBson().getLong("num"));
            latch.countDown();
        });

        // will throw if the subscription doesnt set uop in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, (long)END_EVENT_NUMBER);

        latch.await();
        // TODO test numbers in order.

        sub.close();
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

        final long START_EVENT_NUMBER = 0;
        final long END_EVENT_NUMBER = 64;
        final long RESTART_EVENT_NUMBER = 65;
        final long REEND_EVENT_NUMBER = 128;

        final int expectedEvents = 65;

        final CountDownLatch latch = new CountDownLatch(expectedEvents);
        final List<Long> nums = new LinkedList<>();

        final EventSinkUtils utils =  new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,(long)START_EVENT_NUMBER, END_EVENT_NUMBER);

        final CompletableFuture<Subscription> subFut =  source.subscribeFromMostRecent(testChannelName, event -> {
            nums.add(event.getBson().getLong("num"));
            latch.countDown();
        });

        // will throw if the subscription doesnt set up in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);
        Thread.sleep(1000);
        utils.sendNumberedEvents(testChannelName, RESTART_EVENT_NUMBER, REEND_EVENT_NUMBER);

        // count the number of events
        latch.await();
        // Todo - Check the array
        System.out.println(nums);
        sub.close();
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

       // will throw if the subscription doesnt set uop in the given time
       final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        latch.await();
        // Todo - Check the numbers in order

        source.close();
        sink.close();
    }


    @Test
    public void testSubscribeAll() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestAllChannel"+UUID.randomUUID();
        final long START_EVENT_NUMBER = 0;
        final long END_EVENT_NUMBER = 64;
        final long RESTART_EVENT_NUMBER = 65;
        final long REEND_EVENT_NUMBER = 128;

        final int expectedEvents = (int) (REEND_EVENT_NUMBER  + 1);
        final CountDownLatch latch = new CountDownLatch(expectedEvents);
        final List<Long> nums = new LinkedList<>();

        final EventSinkUtils utils = new EventSinkUtils(sink);
        utils.sendNumberedEvents(testChannelName,START_EVENT_NUMBER, END_EVENT_NUMBER);

        final CompletableFuture<Subscription> subFut = source.subscribeAll(testChannelName,  event -> {
            nums.add(event.getBson().getLong("num"));
            latch.countDown();
        });

        // will throw if the subscription doesnt set uop in the given time
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        utils.sendNumberedEvents(testChannelName, RESTART_EVENT_NUMBER, REEND_EVENT_NUMBER );

        latch.await();
        // Todo

        sub.close();
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