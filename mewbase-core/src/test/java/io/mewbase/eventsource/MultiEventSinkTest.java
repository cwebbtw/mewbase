package io.mewbase.eventsource;


import io.mewbase.MewbaseTestBase;
import io.mewbase.bson.BsonObject;
import org.junit.Test;


import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;


import static org.junit.Assert.assertEquals;


/**
 * Created by Nige on 26/2/2018.
 */

public class MultiEventSinkTest extends MewbaseTestBase {

    class StubEventSink implements EventSink {

        AtomicLong publishSync = new AtomicLong(0);
        AtomicLong publishAsync = new AtomicLong(0);
        AtomicLong close = new AtomicLong(0);

        @Override
        public Long publishSync(String channelName, BsonObject event) {
            return publishSync.getAndIncrement();
        }

        @Override
        public CompletableFuture<Long> publishAsync(String channelName, BsonObject event) {
            return CompletableFuture.completedFuture( publishAsync.getAndIncrement() );
        }

        @Override
        public void close() {
            close.getAndIncrement();
        }
    }


    @Test
    public void testCreateAndCloseMulti()  {
        final EventSink es1 = new StubEventSink();
        final EventSink es2 = new StubEventSink();
        final MultiEventSink mes = MultiEventSink.instance(es1,es2);
        mes.close();
        assertEquals(1L, ((StubEventSink)es1).close.get() );
        assertEquals(1L, ((StubEventSink)es2).close.get() );
    }



    @Test
    public void testPublishSingleEventMultiply()  {

        final EventSink es1 = new StubEventSink();
        final EventSink es2 = new StubEventSink();
        final MultiEventSink mes = MultiEventSink.instance(es1,es2);
        mes.close();
        assertEquals(1L, ((StubEventSink)es1).close.get() );
        assertEquals(1L, ((StubEventSink)es2).close.get() );

        final String channelName = "StubEventChannel";
        final BsonObject event = new BsonObject().put("data", UUID.randomUUID().toString());
        final Stream<Long> evtNums = mes.publishSync(channelName, event);

        long size = evtNums.map( (Long l) -> {
            assertEquals(0L, (long)l );
            return 1;
        }  ).count();
        assertEquals(2L, size);

        mes.close();
    }


    @Test
    public void testPublishMultiEventMultiply()  {

        final long numEventSinks = 7;
        final EventSink[] eventSinks = new EventSink[(int)numEventSinks];
        for (int i = 0; i < numEventSinks; i++) eventSinks[i] = EventSink.instance();

        final MultiEventSink mes = MultiEventSink.instance(eventSinks);
        final String channelName = "StubEventChannel";

        final long numEventsToPublish = 16;
        LongStream.range(0, numEventsToPublish)
                .mapToObj( l -> {
                    final BsonObject event = new BsonObject().put("data", UUID.randomUUID().toString());
                    final CompletableFuture<Stream<Long>> evtNums = (CompletableFuture)mes.publishAsync(channelName, event);
                    evtNums.thenAccept( (Stream<Long> sl ) ->
                            assertEquals( sl.map( (Long p) -> {
                                        assertEquals((long) p, l);
                                        return 1;
                                    } ).count(),  numEventSinks ) );
                    return null;
                } );
        mes.close();
    }



}