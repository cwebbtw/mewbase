package io.mewbase.binders.impl.lmdb;
import io.mewbase.binders.KeyVal;

import io.mewbase.bson.BsonObject;
import io.mewbase.binders.Binder;

import io.vertx.core.buffer.Buffer;

import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.Env;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import java.util.concurrent.*;

import java.util.function.Predicate;
import java.util.stream.Stream;


import static java.nio.ByteBuffer.allocateDirect;
import static org.lmdbjava.CursorIterator.IteratorType.FORWARD;
import static org.lmdbjava.DbiFlags.MDB_CREATE;


/**
 * Created by Tim on 29/12/16.
 */
@Deprecated
public class LmdbBinder implements Binder {

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

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            // in order to do a read we have to do it under a txn so use
            // try with resource to get the auto close magic.
            try (Txn<ByteBuffer> txn = env.txnRead()) {
                ByteBuffer key = makeKeyBuffer(id);
                final ByteBuffer found = dbi.get(txn, key);
                if (found != null) {
                    // copy to local Vert.x buffer from the LMDB mem managed array
                    Buffer buffer = Buffer.buffer(txn.val().remaining());
                    buffer.setBytes( 0 , txn.val() );
                    BsonObject doc = new BsonObject(buffer);
                    return doc;
                } else {
                    return null;
                }
            }
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Void> put(final String id, final BsonObject doc) {
        CompletableFuture fut = CompletableFuture.runAsync( () -> {
            ByteBuffer key = makeKeyBuffer(id);
            byte[] valBytes = doc.encode().getBytes();
            final ByteBuffer val = allocateDirect(valBytes.length);
            val.put(valBytes).flip();
            synchronized (this) {
                dbi.put(key, val);
            }
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Boolean> delete(final String id) {
        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            ByteBuffer key = makeKeyBuffer(id);
            boolean deleted = dbi.delete(key);
            return deleted;
        }, stexec);
        return fut;
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments() {
        return getDocuments( new HashSet(), document -> true);
    }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments(Set<String> keySet, Predicate<BsonObject> filter) {
        CompletableFuture<Set<KeyVal<String, BsonObject>>> fut = CompletableFuture.supplyAsync( () -> {

            Set<KeyVal<String, BsonObject>> resultSet = new HashSet<>();

            try (final Txn<ByteBuffer> txn = env.txnRead()) {
                final CursorIterator<ByteBuffer> cursorItr = dbi.iterate(txn, FORWARD);
                final Iterator<CursorIterator.KeyVal<ByteBuffer>> itr = cursorItr.iterable().iterator();
                boolean hasNext = itr.hasNext();
                txn.reset();

                while (hasNext) {
                    txn.renew();
                    final CursorIterator.KeyVal<ByteBuffer> rawKV = itr.next();
                    // Copy bytes from LMDB managed memory to vert.x buffers
                    final Buffer keyBuffer = Buffer.buffer(rawKV.key().remaining());
                    keyBuffer.setBytes(0, rawKV.key());
                    final Buffer valueBuffer = Buffer.buffer(rawKV.val().remaining());
                    valueBuffer.setBytes(0, rawKV.val());
                    hasNext = itr.hasNext(); // iterator makes reference to the txn
                    txn.reset(); // got data so release the txn
                    final String id = new String(keyBuffer.getBytes());
                    if (keySet.isEmpty() || keySet.contains(id)) {
                        final BsonObject doc = new BsonObject(valueBuffer);
                        if (filter.test(doc)) {
                            KeyVal<String,BsonObject> entry = KeyVal.create(id, doc);
                            resultSet.add(entry);
                        }
                    }
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