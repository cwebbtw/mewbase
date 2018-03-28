package io.mewbase.binder.session;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.mewbase.binders.BinderStore;
import io.mewbase.binders.impl.postgres.PostgresBinderStore;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.function.Supplier;

public class PostgresTestBinderStoreSessionSupplier implements Supplier<TestBinderStoreSession> {

    private final String uri;
    private final String username;
    private final String password;

    private final Config config;

    private String binderTableDDL() {
        final String referenceDDLLocation = "./postgres-schema.sql";
        final ClassLoader classLoader = this.getClass().getClassLoader();
        try {
            try (final InputStream inputStream = classLoader.getResourceAsStream(referenceDDLLocation)) {
                final StringWriter stringWriter = new StringWriter();
                IOUtils.copy(inputStream, stringWriter, Charset.defaultCharset());
                return stringWriter.toString();
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    public PostgresTestBinderStoreSessionSupplier(String uri, String username, String password) {
        this.uri = uri;
        this.username = username;
        this.password = password;
        final Properties properties = new Properties();
        properties.setProperty("mewbase.binders.postgres.store.url", "foo");
        properties.setProperty("mewbase.binders.postgres.store.username", "bar");
        properties.setProperty("mewbase.binders.postgres.store.password", "baz");
        config = ConfigFactory.load(ConfigFactory.parseProperties(properties));
    }

    @Override
    public TestBinderStoreSession get() {
        final Connection connection;
        try {
            connection = DriverManager.getConnection(uri, username, password);
            try (final Statement statement = connection.createStatement()) {
                /*
                Ensure that all proceeding queries are run against the pg_temp schema
                pg_temp is isolated to this connection
                 */
                statement.executeUpdate("SET search_path TO pg_temp");
                statement.executeUpdate(binderTableDDL());
            }
        }
        catch (SQLException e) {
            throw Throwables.propagate(e);
        }

        return new TestBinderStoreSession() {
            @Override
            public void close() throws Exception {
                connection.close();
            }

            @Override
            public BinderStore get() {
                try {
                    return new PostgresBinderStore(config, (uri, username, password) -> connection);
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
