package io.mewbase.binders.impl.postgres;


import com.google.common.base.Throwables;
import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
import io.mewbase.binders.impl.StreamableBinder;
import io.mewbase.bson.BsonCodec;
import io.mewbase.bson.BsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.math.BigInteger;
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
    private final long id;

    private final ExecutorService stexec = Executors.newSingleThreadExecutor();

    private final Connection connection;

    public PostgresBinder(Connection connection, String name) {
        this.connection = connection;
        this.name = name;
        try {
            log.info("Opened Binder named " + name);
            addToBinderStore();
            id = queryBinderId();
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
    public CompletableFuture<BsonObject> get(final String key) {

        CompletableFuture<BsonObject> fut = CompletableFuture.supplyAsync( () -> {
            BsonObject doc = null;
            try {
                final Statement stmt = connection.createStatement();
                final String sql = "SELECT data FROM " + PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME + " WHERE key = ? AND binder_id = ?";
                try (final PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, key);
                    statement.setLong(2, id);

                    try (final ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            byte[] buffer = resultSet.getBytes("data");
                            doc = BsonCodec.bsonBytesToBsonObject(buffer);
                        }
                    }
                }
            } catch (Exception exp) {
                    log.error("Error getting document with key : " + key);
                    throw new CompletionException(exp);
            }
            return doc;
        }, stexec);
        return fut;
    }

    private void addToBinderStore() throws SQLException {
        final String sql =
                "INSERT INTO " + PostgresBinderStore.MEWBASE_BINDER_TABLE_NAME + "(name) VALUES (?) ON CONFLICT DO NOTHING";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            statement.execute();
        }
    }

    private long queryBinderId() throws SQLException {
        final String sql =
                "SELECT id FROM " + PostgresBinderStore.MEWBASE_BINDER_TABLE_NAME + " WHERE name = ?";
        try (final PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (final ResultSet rs = statement.executeQuery()) {
                if (rs.next())
                    return rs.getLong(1);
                else
                    throw new IllegalStateException("Binder with name " + name + " not found");
            }
        }
    }

    @Override
    public CompletableFuture<Boolean> put(final String key, final BsonObject doc) {
        final byte[] valBytes = BsonCodec.bsonObjectToBsonBytes(doc);

        CompletableFuture<Boolean> fut = CompletableFuture.supplyAsync( () -> {
            try {
                final String sql = "INSERT INTO "+ PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME +"(binder_id, key, data)  VALUES( ?, ?, ? )" +
                        " ON CONFLICT (binder_id, key) DO UPDATE SET data = ? ;";
                try (final PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setLong(1, id);
                    stmt.setString(2, key);
                    stmt.setBytes(3, valBytes);
                    stmt.setBytes(4, valBytes);
                    stmt.executeUpdate();
                }
            } catch (Exception exp) {
                log.error("Error writing document key : " + key + " value : " + doc);
                throw new CompletionException(exp);
            }
            return true;
        }, stexec);
        streamFunc.ifPresent( func -> func.accept(key,doc));
        return fut;
    }


    @Override
    @Deprecated
    public CompletableFuture<Boolean> delete(final String key) {

        CompletableFuture<Boolean> fut = CompletableFuture.supplyAsync( () -> {
            try {
                final String sql = "DELETE FROM " + PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME + " WHERE key = ? AND binder_id = ?";
                try (final PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, key);
                    statement.setLong(2, id);
                    statement.executeUpdate();
                    return true;
                }
            } catch (Exception exp) {
                log.error("Error deleting document " + key );
                throw new CompletionException(exp);
            }
        }, stexec);
        return fut;
    }

    @Override
    public Long countDocuments() {
        return null;
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
                final String sql = "SELECT key, data FROM " + PostgresBinderStore.MEWBASE_BINDER_DATA_TABLE_NAME + " WHERE binder_id = ?";
                try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    preparedStatement.setLong(1, id);

                    try (final ResultSet dbrs = preparedStatement.executeQuery()) {
                        while(dbrs.next()) {
                            final String key = dbrs.getString("key");
                            byte[] bytes = dbrs.getBytes("data");
                            final BsonObject doc = BsonCodec.bsonBytesToBsonObject(bytes);
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