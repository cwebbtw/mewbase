package io.mewbase.stream;


import io.mewbase.bson.BsonObject;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;


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
    public Optional<BsonObject> dedupe(BsonObject event) {
        if ( map.get(hash(event)) != null )
            return Optional.empty();
        else {
            map.put(hash(event),nothing);    // just store the hash risking duplicates from
        }
        return Optional.of(event);
    }

    
    private static BigInteger hash(BsonObject event) {
        try { // poss "SHA-512","MD5"
            final MessageDigest md = MessageDigest.getInstance("MD5");
            return new BigInteger(1,md.digest(event.encode().getBytes()));
        }
        catch(Exception exp) {
            throw new RuntimeException("Could not hash event");
        }
    }

}
