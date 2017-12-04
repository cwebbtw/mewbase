package io.mewbase.binders.impl.postgres;


import io.mewbase.binders.Binder;
import io.mewbase.binders.KeyVal;
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
public class PostgresBinder implements Binder {

    private final static Logger log = LoggerFactory.getLogger(PostgresBinder.class);

    private final String name;

    private final ExecutorService stexec = Executors.newSingleThreadExecutor();

    private final Connection connection;


    public PostgresBinder(Connection connection, String name) {
        this.connection = connection;
        this.name = name;
        try {
            createIfDoesntExists(name);
            log.trace("Opened Binder named " + name);
        } catch (Exception exp) {
            log.error("Failed to open binder " + name, exp);
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
                final ResultSet resultSet = stmt.executeQuery("SELECT data FROM "+name+" WHERE key = '"+id+"';");
                if (resultSet.next()) {
                    byte[] buffer = resultSet.getBytes("data");
                    doc = new BsonObject(buffer);
                    resultSet.close();
                }
            } catch (Exception exp) {
                    log.error("Error getting document with key : " + id);
                    throw new CompletionException(exp);
            }
            return doc;
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Void> put(final String id, final BsonObject doc) {

        final byte[] valBytes = doc.encode().getBytes();

        CompletableFuture fut = CompletableFuture.runAsync( () -> {
            try {
                final String sql = "INSERT INTO "+name+" VALUES( ?, ? )" +
                        " ON CONFLICT (key) DO UPDATE SET data = ? ;";
                final PreparedStatement stmt = connection.prepareStatement(sql);
                stmt.setString(1,id);
                stmt.setBytes(2,valBytes);
                stmt.setBytes(3,valBytes);
                stmt.executeUpdate();
                stmt.close();
            } catch (Exception exp) {
                log.error("Error writing document key : " + id + " value : " + doc);
                throw new CompletionException(exp);
            }
        }, stexec);
        return fut;
    }


    @Override
    public CompletableFuture<Boolean> delete(final String id) {

        CompletableFuture fut = CompletableFuture.supplyAsync( () -> {
            try {
                final Statement stmt = connection.createStatement();
                stmt.executeUpdate("DELETE FROM "+name+" WHERE key = '"+id+"';");
                stmt.close();
                return true;
            } catch (Exception exp) {
                log.error("Error deleting document " + id );
                throw new CompletionException(exp);
            }
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

            try {
                final Statement stmt = connection.createStatement();
                final ResultSet dbrs = stmt.executeQuery("SELECT key, data from "+name+";");
                while(dbrs.next()) {
                    final String key = dbrs.getString("key");
                    if (keySet.isEmpty() || keySet.contains(key)) {
                        byte[] bytes = dbrs.getBytes("data");
                        final BsonObject doc = new BsonObject(bytes);
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


    public void createIfDoesntExists(final String name) throws SQLException {
        final String sql = "CREATE TABLE IF NOT EXISTS "+name+" (key TEXT, data bytea, PRIMARY KEY ( key ))";
        connection.createStatement().executeUpdate(sql);
    }

}