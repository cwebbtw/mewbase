package io.mewbase.binders.impl.postgres;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.Binder;
import io.mewbase.binders.BinderStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public class PostgresBinderStore implements BinderStore {

    public static final String MEWBASE_BINDER_DATA_TABLE_NAME = "mewbase_binder_data";
    public static final String MEWBASE_BINDER_META_TABLE_NAME = "mewbase_binder_meta";

    private final static Logger logger = LoggerFactory.getLogger(PostgresBinderStore.class);

    protected final ConcurrentMap<String, Binder> binders = new ConcurrentHashMap<>();

    private Connection connection;

    public PostgresBinderStore() throws Exception {
        this(ConfigFactory.load(), DriverManager::getConnection);
    }

    public PostgresBinderStore(Config cfg, ConnectionBuilder connectionBuilder) throws Exception {
        Class.forName("org.postgresql.Driver");
        final String uri = cfg.getString("mewbase.binders.postgres.store.url");
        final String username = cfg.getString("mewbase.binders.postgres.store.username");
        final String password = cfg.getString("mewbase.binders.postgres.store.password");

        connection = connectionBuilder.build(uri, username, password);
        logger.info("Started postgress binder store with  " + uri);

        listAllTables().forEach(this::open);
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


    private Stream<String> listAllTables() {
        Set<String> names = new HashSet<>();
        try {
            final String sql = "SELECT binder_name FROM " + MEWBASE_BINDER_META_TABLE_NAME;
            try (final Statement stmt = connection.createStatement()) {
                try (ResultSet dbrs = stmt.executeQuery(sql)) {
                    while (dbrs.next()) {
                        names.add(dbrs.getString(1));
                    }
                }
            }
        } catch (Exception exp) {
            logger.error("Failed to find current binders list in postgres",exp);
        }
        return names.stream();
    }

    @Override
    public void close() throws Exception {
        connection.close();
    }
}
