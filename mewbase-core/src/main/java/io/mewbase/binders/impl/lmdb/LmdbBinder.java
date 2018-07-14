package io.mewbase.binders.impl.lmdb;

import io.mewbase.binders.KeyVal;
import io.mewbase.binders.impl.StreamableBinder;
import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;
import io.mewbase.binders.Binder;

import io.vertx.core.buffer.Buffer;

import org.lmdbjava.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import java.util.concurrent.*;

import java.util.function.Predicate;
import java.util.stream.Stream;


import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.DbiFlags.MDB_CREATE;


/**
 * Created by Tim on 29/12/16.
 */
@Deprecated
public class LmdbBinder extends StreamableBinder implements Binder {

    private final static Logger log = LoggerFactory.getLogger(LmdbBinder.class);

    private final String name;

    // In LMDB all transactional ops have thread affinity so they must be executed on the same thread.
    private final ExecutorService stexec = Executors.newSingleThreadExecutor();

    private final Env<ByteBuffer> env;
    private Dbi<ByteBuffer> dbi;

    public LmdbBinder(String name, Env<ByteBuffer> env) {
        this.env = env;
        this.name = name;

        // create the db if it doesnt exists
        this.dbi =  env.openDbi(name,MDB_CREATE);
        log.trace("Opened Binder named " + name);
    }


    @Override
    public String getName() {
        return name;
    }


    @Override
    public CompletableFuture<BsonObject> get(final String id) {

        CompletableFuture<BsonObject> fut = CompletableFuture.supplyAsync( () -> {
            // in order to do a read we have to do it under a txn so use
            // try with resource to get the auto close magic.
            try (Txn<ByteBuffer> txn = env.txnRead()) {
                ByteBuffer key = makeKeyBuffer(id);
                final ByteBuffer found = dbi.get(txn, key);
                if (found != null) {
                    BsonObject doc = BsonCodec.bsonBytesToBsonObject(txn.val().array());
                    return doc;
                } else {
                    return null;
                }
            }
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Boolean> put(final String id, final BsonObject doc) {
        CompletableFuture<Boolean> fut = CompletableFuture.supplyAsync( () -> {
            ByteBuffer key = makeKeyBuffer(id);
            byte[] valBytes = BsonCodec.bsonObjectToBsonBytes(doc);
            final ByteBuffer val = allocateDirect(valBytes.length);
            val.put(valBytes).flip();
            synchronized (this) {
                dbi.put(key, val);
            }
            return true;
        }, stexec);
        streamFunc.ifPresent( func -> func.accept(id,doc));
        return fut;
    }


    @Override
    public CompletableFuture<Boolean> delete(final String id) {
        CompletableFuture<Boolean> fut = CompletableFuture.supplyAsync( () -> {
            ByteBuffer key = makeKeyBuffer(id);
            boolean deleted = dbi.delete(key);
            return deleted;
        }, stexec);
        return fut;
    }

    @Override
    public Long countDocuments() {
        return getDocuments().count();
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments() {
        return getDocuments( kv -> true);
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments( Predicate<KeyVal<String, BsonObject>> filter) {
        CompletableFuture<Set<KeyVal<String, BsonObject>>> fut = CompletableFuture.supplyAsync( () -> {

            Set<KeyVal<String, BsonObject>> resultSet = new HashSet<>();

            try (final Txn<ByteBuffer> txn = env.txnRead()) {
                final CursorIterator<ByteBuffer> cursorItr = dbi.iterate(txn, KeyRange.all());
                final Iterator<CursorIterator.KeyVal<ByteBuffer>> itr = cursorItr.iterable().iterator();
                boolean hasNext = itr.hasNext();
                txn.reset();

                while (hasNext) {
                    txn.renew();
                    final CursorIterator.KeyVal<ByteBuffer> rawKV = itr.next();
                    final String id = new String(rawKV.key().array());
                    final BsonObject doc = BsonCodec.bsonBytesToBsonObject(rawKV.val().array());
                    hasNext = itr.hasNext(); // iterator makes reference to the txn
                    txn.reset(); // got data so release the txn

                    KeyVal<String,BsonObject> entry = KeyVal.create(id, doc);
                    if (filter.test(entry)) resultSet.add(entry);
                }
            }
            return resultSet;
        }, stexec);
        return fut.join().stream();
    }

    
    public Void close() {
        env.sync(true);
        dbi.close();
        stexec.shutdown();
        return null;
    }


    private ByteBuffer makeKeyBuffer(String id) {
        final ByteBuffer key = allocateDirect(env.getMaxKeySize());
        // between jdk 8 and 9  flip has been moved hence apparently redundant cast
        ((java.nio.Buffer)key.put(id.getBytes(StandardCharsets.UTF_8))).flip();
        return key;
    }


}