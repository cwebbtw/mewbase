package io.mewbase.eventsource;

import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;

import io.mewbase.eventsource.impl.nats.NatsEventSink;
import io.mewbase.eventsource.impl.nats.NatsEventSource;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;


import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.LongStream;


import static org.junit.Assert.fail;

/**
 * Created by Nige on 7/9/2017.
 */
@RunWith(VertxUnitRunner.class)
public class EventSinkTest extends MewbaseTestBase {


    @Test
    public void testConnectToEventSink() throws Exception {
        EventSink es = EventSink.instance();
        es.close();
        assert(true);
    }


    @Test
    public void testPublishSingleEvent() throws Exception {

        EventSink eSink = EventSink.instance();

        final String testChannelName = "singleEventSink";
        final String inputUUID = randomString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        // check the event arrived
        final CountDownLatch latch = new CountDownLatch(1);
        EventSource eSource = new NatsEventSource();
        Subscription subs = eSource.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        latch.countDown();
                        }
                    );

        eSink.publish(testChannelName,bsonEvent);

        latch.await();

        eSource.close();
        eSink.close();
    }


    @Test
    public void testManyEventsInOrder() throws Exception {

        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestMultiEventChannel";
        EventSink eSink = EventSink.instance();

        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 128;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        EventSource eSource = new NatsEventSource();
        eSource.subscribe(testChannelName, event -> {
                BsonObject bson  = event.getBson();
                long thisEventNum = END_EVENT_NUMBER - latch.getCount();
                assert(bson.getLong("num") == thisEventNum);
                latch.countDown();
        });

        LongStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).forEach(l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            eSink.publish(testChannelName,bsonEvent);
        } );

        latch.await();
        eSource.close();
        eSink.close();
    }


    @Test
    public void testManyAsyncEvents() throws Exception {

        final String testChannelName = "TestManyAsyncEventChannel";
        EventSink eSink = EventSink.instance();

        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 128;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        EventSource eSource = new NatsEventSource();
        eSource.subscribe(testChannelName, event -> {
            BsonObject bson  = event.getBson();
            long thisEventNum = END_EVENT_NUMBER - latch.getCount();
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });


        List<CompletableFuture<BsonObject>> futs = LongStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).mapToObj(l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            return eSink.publishAsync(testChannelName,bsonEvent);
        } ).collect(Collectors.toList());

        // Ensure all of the futures complete successfully
        CompletableFuture.allOf((futs.toArray(new CompletableFuture[futs.size()]))).handle(
                (good, bad) -> {
                    if (bad != null) fail("One or more publishAsync calls failed");
                    return good;
                }).join();

        latch.await();
        eSource.close();
        eSink.close();
    }

}