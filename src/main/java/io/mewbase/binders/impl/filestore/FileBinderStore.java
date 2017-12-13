package io.mewbase.binders.impl.filestore;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.*;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;



public class FileBinderStore implements BinderStore {

    private final static Logger logger = LoggerFactory.getLogger(FileBinderStore.class);

    protected final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();

    protected final File bindersDir;


    public FileBinderStore() { this(ConfigFactory.load() ); }

    public FileBinderStore(Config cfg) {

        bindersDir = Paths.get(cfg.getString("mewbase.binders.files.store")).toFile();

        logger.info("Starting file based binder store with docs dir: " + bindersDir.toString());

        FileBinder.createIfDoesntExists(bindersDir);

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(bindersDir.toPath())) {
            for (Path entry: stream) {
                open(entry.getFileName().toString());
            }
        } catch (Exception ex) {
            logger.error("File based Binder failed to start", ex);
        }
    }


    @Override
    public Binder open(String name) {
        return binders.computeIfAbsent(name, k -> new FileBinder(k, new File(bindersDir,name)));
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


    @Override
    public Boolean delete(String name) {
        return null;
    }


}
