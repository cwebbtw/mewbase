package io.mewbase.binders.impl.filestore;


import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.bson.BsonObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import java.nio.file.*;
import java.util.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Created by Nige on 15/11/17.
 */
public class FileBinder implements Binder {

    private final static Logger log = LoggerFactory.getLogger(FileBinder.class);

    private final String name;

    private final File binderDir;

    private final ExecutorService stexec = Executors.newSingleThreadExecutor();


    public FileBinder(String name, File binderDir) {
        this.name = name;
        this.binderDir = binderDir;
        createIfDoesntExists(this.binderDir);
        log.trace("Opened Binder named " + name);
    }


    @Override
    public String getName() {
        return name;
    }


    @Override
    public CompletableFuture<BsonObject> get(final String id) {
        File file = new File(binderDir,id);

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            if (file.exists()) {
                BsonObject doc = null;
                try {
                    byte[] buffer = Files.readAllBytes(file.toPath());
                    doc = new BsonObject(buffer);
                } catch (Exception exp) {
                    log.error("Error getting document with key : "+ id);
                }
                return doc;
            } else {
                return null;
            }
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Void> put(final String id, final BsonObject doc) {
        final File file = new File(binderDir, id);
        final byte[] valBytes = doc.encode().getBytes();

        CompletableFuture fut = CompletableFuture.runAsync( () -> {
            try {
                Files.write(file.toPath(), valBytes); // implies CREATE, TRUNCATE_EXISTING, WRITE;
            } catch (Exception exp) {
                log.error("Error writing document key : " + id + " value : " + doc);
            }
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Boolean> delete(final String id) {
        final File file = new File(binderDir, id);

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            try {
                return Files.deleteIfExists(file.toPath());
            } catch (Exception exp) {
                log.error("Error deleting document " + id );
            }
            return false;
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

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(binderDir.toPath())) {
                for (Path entry: stream) {
                    String key = entry.getFileName().toString();
                    if (keySet.isEmpty() || keySet.contains(key)) {
                        byte[] buffer = Files.readAllBytes(entry);
                        final BsonObject doc = new BsonObject(buffer);
                        if (filter.test(doc)) {
                            KeyVal<String,BsonObject> kv = KeyVal.create(key, doc);
                            resultSet.add(kv);
                        }
                    }
                }
            } catch (Exception ex) {
                log.error("File based Binder failed to start", ex);
            }
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