package io.mewbase.binders.impl.postgres;


import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.filestore.FileBinder;
import io.mewbase.server.MewbaseOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;


public class PostgresBinderStore implements BinderStore {

    private final static Logger logger = LoggerFactory.getLogger(PostgresBinderStore.class);

    protected final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();

    Connection connection;


    public PostgresBinderStore() { this(new MewbaseOptions()); }

    public PostgresBinderStore(MewbaseOptions mewbaseOptions) {

        try {
            Class.forName("org.postgresql.Driver");
            final String uri = "jdbc:postgresql://127.0.0.1:5432/mewbase";
            connection = DriverManager.getConnection(uri, "mewbase", "mewbase");
            logger.info("Started postgress binder store with  " + uri);
        } catch (Exception exp) {
            logger.error("Postgres binder failed to start", exp);
        }


        // list all dbs <=>  binders
        // then  open(binder name);
    }


    @Override
    public Binder open(String name) {
        return binders.computeIfAbsent(name, key -> new PostgresBinder(connection, key));
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
