package io.mewbase.binders.impl.lmdb;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;

import org.lmdbjava.Env;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import java.util.List;
import java.util.Optional;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import java.util.stream.Stream;


import static org.lmdbjava.EnvFlags.MDB_NOTLS;

@Deprecated
public class LmdbBinderStore implements BinderStore {

    private final static Logger logger = LoggerFactory.getLogger(LmdbBinderStore.class);

    private final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();

    private final String docsDir;

    private final int maxDBs;
    private final long maxDBSize;

    private Env<ByteBuffer> env;

    @Deprecated
    public LmdbBinderStore() { this(ConfigFactory.load()); }

    @Deprecated
    public LmdbBinderStore(Config cfg) {

        this.docsDir = cfg.getString("mewbase.binders.lmdb.store.basedir");
        this.maxDBs = cfg.getInt("mewbase.binders.lmdb.store.max.binders");
        this.maxDBSize = cfg.getLong("mewbase.binders.lmdb.store.max.binder.size");

        logger.info("Starting LMDB binder store with docs dir: " + docsDir);
        File fDocsDir = new File(docsDir);
        createIfDoesntExists(fDocsDir);
        this.env = Env.<ByteBuffer>create()
                .setMapSize(maxDBSize)
                .setMaxDbs(maxDBs)
                .setMaxReaders(1024)
                .open(fDocsDir, Integer.MAX_VALUE, MDB_NOTLS);

        // get the names all the current binders and open them
        List<byte[]> names = env.getDbiNames();
        names.stream().map( name -> open(new String(name)));
    }


    @Override
    public Binder open(String name) {
        return binders.computeIfAbsent(name, k -> new LmdbBinder(k,env)) ;
    }


    @Override
    public Optional<Binder> get(String name) {
        return Optional.ofNullable(binders.get(name));
    }

    @Override
    public Stream<Binder> binders() {
        return binders.values().stream();
    }

    @Override
    public Stream<String> binderNames() {
        return binders.keySet().stream();
    }


    // TODO Delete Binder
    @Override
    public Boolean delete(String name) {
        return null;
    }


    @Override
    public void close() {
        try {
            binders().forEach(binder -> ((LmdbBinder)binder).close());
        } catch (Exception e) {
            logger.error("Failed to close all binders.", e);
        } finally {
            env.close();
        }
        logger.info("Closed LMDB binder store at " + docsDir);
    }


    private void createIfDoesntExists(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create dir " + dir);
            }
        }
    }



}
