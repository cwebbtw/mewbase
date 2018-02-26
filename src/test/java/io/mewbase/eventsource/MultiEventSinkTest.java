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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
            return publishSync.incrementAndGet();
        }

        @Override
        public CompletableFuture<Long> publishAsync(String channelName, BsonObject event) {
            return CompletableFuture.completedFuture( publishAsync.incrementAndGet() );
        }

        @Override
        public void close() {
            close.incrementAndGet();
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
    public void testPublishSingleEventMultiply() throws Exception {

        final EventSink es1 = new StubEventSink();
        final EventSink es2 = new StubEventSink();
        final MultiEventSink mes = MultiEventSink.instance(es1,es2);
        mes.close();
        assertEquals(1L, ((StubEventSink)es1).close.get() );
        assertEquals(1L, ((StubEventSink)es2).close.get() );

        final String channelName = "StubEventChannel";
        final BsonObject event = new BsonObject().put("data", UUID.randomUUID().toString());
        final Stream<Long> evtNums = mes.publishSync(channelName, event);

        //long size = evtNums.map( (Long l) -> assertEquals(0L, (long)l ) ).count();
        //assertEquals(2L, size);

        mes.close();
    }





}