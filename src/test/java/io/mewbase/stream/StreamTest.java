package io.mewbase.stream;

import io.mewbase.MewbaseTestBase;

import io.mewbase.bson.BsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.*;


/**
 * Created by Nige on 8/1/18.
 */
@RunWith(VertxUnitRunner.class)
public class StreamTest extends MewbaseTestBase {

    final static String key = "key";

    @Test
    public void testBasicDeDupe()  {

        DeDuper dd = new DeDuper(2);

        final BsonObject event1 = new BsonObject().put(key, "event1");
        final BsonObject event2 = new BsonObject().put(key, "event2");
        final BsonObject event3 = new BsonObject().put(key, "event3");

        // empty passes through
        assertEquals(dd.dedupe(event1), Optional.of(event1));
        // dupe is ignored
        assertEquals(dd.dedupe(event1), Optional.empty());
        // new one passes
        assertEquals(dd.dedupe(event2), Optional.of(event2));
        // new event filling window passes
        assertEquals(dd.dedupe(event3), Optional.of(event3));
        // check 2 and 3 are still in cache
        assertEquals(dd.dedupe(event2), Optional.empty());
        assertEquals(dd.dedupe(event3), Optional.empty());
        // and that 1 has gone and can again pass through
        assertEquals(dd.dedupe(event1), Optional.of(event1));
        assertEquals(dd.dedupe(event3), Optional.empty());
    }


    @Test
    public void testDeDupeStream()  {
        final int streamLength = 128;  // tested to 5m
        DeDuper dd = new DeDuper(streamLength);
        long count = IntStream.range(0, streamLength).mapToObj(i -> {
            final BsonObject event = new BsonObject().put(key, "event" + i);    // enforce unique
            final Optional<BsonObject> duped = dd.dedupe(new BsonObject().put(key, "event" + i));
            assertEquals(duped,Optional.of(event));
            return duped;
            }
        ).collect(Collectors.counting()); // force this to complete before testing the assertions.
        assertEquals(count, streamLength);

        // test something in the middle is memorized
        final BsonObject midEvent = new BsonObject().put(key, "event" + streamLength / 2);
        assertEquals(dd.dedupe(midEvent),Optional.empty());
    }

    @Test
    public void testFilterStream()  {

        final int streamLength = 128;
        final Predicate<BsonObject> filter = DeDuper.filter(streamLength);
        // set ensures unique
        Stream<BsonObject> events = IntStream.range(0, streamLength)
                    .mapToObj( i -> "" + i)
                    .flatMap( str -> {
                        final BsonObject event = new BsonObject().put(key, "event" + str);
                        final Stream<BsonObject> single = Stream.of(event);
                        final Stream<BsonObject> duplicate = Stream.of(event, event);
                        return str.contains("3") ? duplicate : single;
                        })
                    .filter(filter);

        // check remove dups and retained uniques.
        assertEquals(streamLength, events.count());

    }

}