package io.mewbase.stream;


import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

/**
 *
 * A de-duplicating class for events expressed as BsonObjects
 *
 * It is necessary to express a window of events over which the duplicates can be detected.
 *
 * E.g to de-duplicate 10K events use
 *
 *  DeDuper dd = new DeDuper(10000);
 *
 * * Important usage note *
 *
 * This uses long hashes of the events so it is possible although very improbable that non duplicate events will be
 * seen as duplicates (i.e. a hash collision). If is critical for your application that all non-duplicate events
 * are processed. We suggest using the "at least once" idempotent event pattern as opposed to de-duplication.
 *
 */

public class DeDuper {

    static final String nothing = "";

    final LinkedHashMap<BigInteger,String> map;
    

    public DeDuper(final int window) {
        // i.e. evict in Insertion order
        final Boolean evictInAccessOrder = false;
        map = new LinkedHashMap<BigInteger,String>(window, 0.75f, evictInAccessOrder) {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                return size() > window;
            }
        };
    }

    /**
     *  If this Event is not a copy of any in the window then return the event
     *  else return an Empty option.
     */
    public  Optional<BsonObject> dedupe(BsonObject event) {
        synchronized(map) {
            if ( map.get(hash(event)) != null )
                return Optional.empty();
            else {
                map.put(hash(event),nothing);    // just store the hash risking duplicates from colliding events.
            }
        }
        return Optional.of(event);
    }


    /**
     * Create a filter that can be used in a stream/flow to deduplicate on the fly
     * @param window
     * @return a filter
     */
    final static Predicate<BsonObject> filter(final int window) {
        DeDuper dd = new DeDuper(window);
        Predicate<BsonObject> filter = bson -> {
            Optional<BsonObject> empty = Optional.empty();
            return dd.dedupe(bson) != empty;
        };
        return filter;
    }

    /**
     * Make an MD5 hash of this event - chosen for least collision reasons
     * @param event
     * @return
     */
    private BigInteger hash(BsonObject event) {
        try { // poss "SHA-512","MD5"
            final MessageDigest md = MessageDigest.getInstance("MD5");
            return new BigInteger(1,md.digest(BsonCodec.bsonObjectToBsonBytes(event)));
        }
        catch(Exception exp) {
            throw new RuntimeException("Could not hash event");
        }
    }

}
