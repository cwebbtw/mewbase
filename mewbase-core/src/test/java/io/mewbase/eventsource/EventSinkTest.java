package io.mewbase.eventsource;

import com.typesafe.config.Config;
import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import org.junit.Test;


import java.util.List;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Created by Nige on 7/9/2017.
 */
//@RunWith(VertxUnitRunner.class)
public class EventSinkTest extends MewbaseTestBase {

    @Test
    public void testConnectToEventSink() throws Exception {
        EventSink es = EventSink.instance();
        es.close();
        assert(true);
    }


    @Test
    public void testPublishSingleEvent() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        // make the channel unique so that the event number is always zero below.
        final String testChannelName = "SingleEventSink" + UUID.randomUUID();
        final String inputUUID = UUID.randomUUID().toString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        // check the event arrived
        final CountDownLatch latch = new CountDownLatch(1);

        final CompletableFuture<Subscription> subFut = source.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        latch.countDown();
                        }
                    );
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);


        long eventNumber = sink.publishSync(testChannelName,bsonEvent);
        assertEquals(0, eventNumber);
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

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestManyOrderedEventsChannel" + UUID.randomUUID();
        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 128;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        final CompletableFuture<Subscription> subFut = source.subscribe(testChannelName, event -> {
            BsonObject bson = event.getBson();
            long thisEventNum = END_EVENT_NUMBER - latch.getCount();
            assert (bson.getLong("num") == thisEventNum);
            latch.countDown();
        });

        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        LongStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).sequential().forEach(l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            sink.publishSync(testChannelName,bsonEvent);
        } );

        latch.await();

        sub.close();
        source.close();
        sink.close();
    }


    @Test
    public void testManyAsyncEvents() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestManyAsyncEventChannel" + UUID.randomUUID();
        final int START_EVENT_NUMBER = 0;
        final int END_EVENT_NUMBER = 127;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER + 1;

        final List<Integer> eventNums = IntStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).boxed().collect(Collectors.toList());

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        final CompletableFuture<Subscription> subFut = source.subscribe(testChannelName, event -> {
            BsonObject bson  = event.getBson();
            eventNums.remove( bson.getInteger("num") );
            latch.countDown();
        });

        // make sure the subscription is set up.
        final Subscription sub = subFut.get(SUBSCRIPTION_SETUP_MAX_TIMEOUT, TimeUnit.SECONDS);

        List<CompletableFuture<Long>> futs = IntStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).mapToObj(i -> {
            final BsonObject bsonEvent = new BsonObject().put("num", i);
            return sink.publishAsync(testChannelName,bsonEvent);
        } ).collect(Collectors.toList());

        // Ensure all of the futures complete successfully
        assertEquals(futs.size(),TOTAL_EVENTS);
        CompletableFuture.allOf((futs.toArray(new CompletableFuture[futs.size()]))).handle(
                (good, bad) -> {
                    if (bad != null) fail("One or more publishAsync calls failed");
                    return good;
                }).join();

        latch.await();
        assert(eventNums.isEmpty() );

        source.close();
        sink.close();
    }

}