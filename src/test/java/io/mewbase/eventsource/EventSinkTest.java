package io.mewbase.eventsource;

import com.typesafe.config.Config;
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

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "singleEventSink";
        final String inputUUID = randomString();
        final BsonObject bsonEvent = new BsonObject().put("data", inputUUID);

        // check the event arrived
        final CountDownLatch latch = new CountDownLatch(1);

        Subscription subs = source.subscribe(testChannelName,  event ->  {
                        BsonObject bson  = event.getBson();
                        assert(inputUUID.equals(bson.getString("data")));
                        latch.countDown();
                        }
                    );

        sink.publish(testChannelName,bsonEvent);

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


        // Test local event producer to inject events in the event source.
        final String testChannelName = "TestMultiEventChannel";
        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 128;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        source.subscribe(testChannelName, event -> {
                BsonObject bson  = event.getBson();
                long thisEventNum = END_EVENT_NUMBER - latch.getCount();
                assert(bson.getLong("num") == thisEventNum);
                latch.countDown();
        });

        LongStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).forEach(l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            sink.publish(testChannelName,bsonEvent);
        } );

        latch.await();
        source.close();
        sink.close();
    }


    @Test
    public void testManyAsyncEvents() throws Exception {

        // use test local config
        final Config testConfig = createConfig();
        final EventSink sink = EventSink.instance(testConfig);
        final EventSource source = EventSource.instance(testConfig);

        final String testChannelName = "TestManyAsyncEventChannel";
        final int START_EVENT_NUMBER = 1;
        final int END_EVENT_NUMBER = 128;

        final int TOTAL_EVENTS = END_EVENT_NUMBER - START_EVENT_NUMBER;

        final CountDownLatch latch = new CountDownLatch(TOTAL_EVENTS);

        source.subscribe(testChannelName, event -> {
            BsonObject bson  = event.getBson();
            long thisEventNum = END_EVENT_NUMBER - latch.getCount();
            assert(bson.getLong("num") == thisEventNum);
            latch.countDown();
        });


        List<CompletableFuture<BsonObject>> futs = LongStream.rangeClosed(START_EVENT_NUMBER,END_EVENT_NUMBER).mapToObj(l -> {
            final BsonObject bsonEvent = new BsonObject().put("num", l);
            return sink.publishAsync(testChannelName,bsonEvent);
        } ).collect(Collectors.toList());

        // Ensure all of the futures complete successfully
        CompletableFuture.allOf((futs.toArray(new CompletableFuture[futs.size()]))).handle(
                (good, bad) -> {
                    if (bad != null) fail("One or more publishAsync calls failed");
                    return good;
                }).join();

        latch.await();
        source.close();
        sink.close();
    }

}