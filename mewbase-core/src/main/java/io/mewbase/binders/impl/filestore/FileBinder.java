package io.mewbase.binders.impl.filestore;


import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.binders.impl.StreamableBinder;
import io.mewbase.bson.BsonObject;
import io.micrometer.core.instrument.Counter;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Created by Nige on 15/11/17.
 */
public class FileBinder extends StreamableBinder implements Binder {

    private final static Logger log = LoggerFactory.getLogger(FileBinder.class);

    private final String name;

    private final File binderDir;

    private final Counter putCounter;
    private final Counter getCounter;
    private final Counter delCounter;
    private final AtomicLong docsGuage =  new AtomicLong(0);

    private final ExecutorService stexec = Executors.newSingleThreadExecutor();


    public FileBinder(String name, File binderDir) {
        this.name = name;
        this.binderDir = binderDir;
        createIfDoesntExists(this.binderDir);

        // counters and logging
        // List<Tag> nameAsIterable = Arrays.asList(new );
        List<Tag> tag = Arrays.asList(Tag.of("name", name));
        putCounter = Metrics.counter("mewbase.binder.file.put", tag);
        getCounter = Metrics.counter( "mewbase.binder.file.get", tag);
        delCounter = Metrics.counter( "mewbase.binder.file.delete", tag);
        // gauges need to wrapped and registered
        Metrics.gauge("mewbase.binder.file.docs", tag, docsGuage);
        log.info("Opened Binder named " + name);
    }


    @Override
    public String getName() {
        return name;
    }


    @Override
    public CompletableFuture<BsonObject> get(final String id) {
        final File file = new File(binderDir,id);

        CompletableFuture fut = new CompletableFuture();
        stexec.submit( () -> {
            BsonObject doc = null;
            if (file.exists()) {
                try {
                    byte[] buffer = Files.readAllBytes(file.toPath());
                    doc = new BsonObject(buffer);
                    log.debug("Read Document " + id + " : " + doc);
                    getCounter.increment();
                } catch (Exception exp) {
                    log.error("Error getting document with key : " + id);
                    fut.completeExceptionally(exp);
                }
            }
            fut.complete(doc);

        });
        return fut;
    }


    @Override
    public CompletableFuture<Void> put(final String id, final BsonObject doc) {
        final File file = new File(binderDir, id);
        final byte[] valBytes = doc.encode().getBytes();

        CompletableFuture fut = new CompletableFuture();
        stexec.submit( () -> {
            try {
                Files.write(file.toPath(), valBytes); // implies CREATE, TRUNCATE_EXISTING, WRITE;
                log.debug("Written Document " + id + " : " + doc);
                putCounter.increment();
            } catch (Exception exp) {
                log.error("Error writing document key : " + id + " value : " + doc);
                fut.completeExceptionally(exp);
            }
            fut.complete(null);
        });
        streamFunc.ifPresent( func -> func.accept(id,doc));
        return fut;
    }


    @Override
    public CompletableFuture<Boolean> delete(final String id) {
        final File file = new File(binderDir, id);

        CompletableFuture fut = new CompletableFuture();
        stexec.submit(  () -> {
            try {
                fut.complete( Files.deleteIfExists(file.toPath()) );
                log.debug("Deleted " + file.toPath());
                delCounter.increment();
                fut.complete(true);
            } catch (Exception exp) {
                log.error("Error deleting document " + id );
                fut.completeExceptionally(exp);
            }
        });
        return fut;
    }


    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments() { return getDocuments( kv -> true); }

    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments( Predicate<KeyVal<String, BsonObject>> filter) {

        CompletableFuture<Set<KeyVal<String, BsonObject>>> fut = CompletableFuture.supplyAsync( () -> {

            Set<KeyVal<String, BsonObject>> resultSet = new HashSet<>();
            long documentCounter = 0;
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(binderDir.toPath())) {
                for (Path entry: stream) {
                    documentCounter += 1;
                    String key = entry.getFileName().toString();
                    byte[] buffer = Files.readAllBytes(entry);
                    final BsonObject doc = new BsonObject(buffer);
                    KeyVal<String,BsonObject> kv = KeyVal.create(key, doc);
                    if (filter.test(kv)) resultSet.add(kv);
                }
            } catch (Exception ex) {
                log.error("File based Binder failed to start", ex);
            }
            docsGuage.set(documentCounter);
            return resultSet;
        }, stexec);

        return fut.join().stream();
    }

    public static void createIfDoesntExists(File dir) {
        if (!dir.exists()) {
            if (!dir.mkdirs()) {
                throw new RuntimeException("Failed to create dir " + dir);
            }
        }
    }


}