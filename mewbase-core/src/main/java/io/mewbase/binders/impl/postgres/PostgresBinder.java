package io.mewbase.binders.impl.postgres;


import com.google.common.base.Throwables;
import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.binders.impl.StreamableBinder;
import io.mewbase.bson.BsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.*;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;


/**
 * Created by Nige on 4/12/17.
 */
public class PostgresBinder extends StreamableBinder implements Binder {

    private final static Logger log = LoggerFactory.getLogger(PostgresBinder.class);

    private final String name;

    private final ExecutorService stexec = Executors.newSingleThreadExecutor();

    private final Connection connection;

    public PostgresBinder(Connection connection, String name) {
        this.connection = connection;
        this.name = name;
        try {
            log.info("Opened Binder named " + name);
            addToBinderMeta();
        } catch (Exception exp) {
            log.error("Failed to open binder " + name, exp);
            throw Throwables.propagate(exp);
        }
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public CompletableFuture<BsonObject> get(final String id) {

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            BsonObject doc = null;
            try {
                final Statement stmt = connection.createStatement();
                final String sql = "SELECT data FROM " + PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME + " WHERE key = ? AND binder_name = ?";
                try (final PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, id);
                    statement.setString(2, name);

                    try (final ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            byte[] buffer = resultSet.getBytes("data");
                            doc = new BsonObject(buffer);
                        }
                    }
                }
            } catch (Exception exp) {
                    log.error("Error getting document with key : " + id);
                    throw new CompletionException(exp);
            }
            return doc;
        }, stexec);
        return fut;
    }

    private void addToBinderMeta() throws SQLException {
        final String sql =
                "INSERT INTO " + PostgresBinderStore.MEWBASE_BINDER_META_TABLE_NAME + " VALUES (?) ON CONFLICT (binder_name) DO NOTHING";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            statement.execute();
        }
    }

    @Override
    public CompletableFuture<Void> put(final String id, final BsonObject doc) {
        final byte[] valBytes = doc.encode().getBytes();

        CompletableFuture fut = CompletableFuture.runAsync( () -> {
            try {
                final String sql = "INSERT INTO "+ PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME +" VALUES( ?, ?, ? )" +
                        " ON CONFLICT (binder_name, key) DO UPDATE SET data = ? ;";
                try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setString(1, name);
                    stmt.setString(2, id);
                    stmt.setBytes(3, valBytes);
                    stmt.setBytes(4, valBytes);
                    stmt.executeUpdate();
                }
            } catch (Exception exp) {
                log.error("Error writing document key : " + id + " value : " + doc);
                throw new CompletionException(exp);
            }
        }, stexec);
        streamFunc.ifPresent( func -> func.accept(id,doc));
        return fut;
    }


    @Override
    @Deprecated
    public CompletableFuture<Boolean> delete(final String id) {

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            try {
                final String sql = "DELETE FROM " + PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME + " WHERE key = ? AND binder_name = ?";
                try (final PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, id);
                    statement.setString(2, name);
                    statement.executeUpdate();
                    return true;
                }
            } catch (Exception exp) {
                log.error("Error deleting document " + id );
                throw new CompletionException(exp);
            }
        }, stexec);
        return fut;
    }


    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments() {
        return getDocuments( kv -> true);
    }


    @Override
    public Stream<KeyVal<String, BsonObject>> getDocuments( Predicate<KeyVal<String, BsonObject>> filter) {
        CompletableFuture<Set<KeyVal<String, BsonObject>>> fut = CompletableFuture.supplyAsync( () -> {
            Set<KeyVal<String, BsonObject>> resultSet = new HashSet<>();

            try {
                final String sql = "SELECT key, data FROM " + PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME + " WHERE binder_name = ?";
                try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    preparedStatement.setString(1, name);

                    try (final ResultSet dbrs = preparedStatement.executeQuery()) {
                        while(dbrs.next()) {
                            final String key = dbrs.getString("key");
                            byte[] bytes = dbrs.getBytes("data");
                            final BsonObject doc = new BsonObject(bytes);
                            KeyVal<String,BsonObject> kv = KeyVal.create(key, doc);
                            if (filter.test(kv)) resultSet.add(kv);
                        }
                    }
                }
            } catch (Exception ex) {
                log.error("Postgres Binder failed to get documents", ex);
            }
            return resultSet;
        }, stexec);
        return fut.join().stream();
    }

}