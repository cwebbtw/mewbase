package io.mewbase.binders.impl.postgres;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionBuilder {
    Connection build(String uri, String username, String password) throws SQLException;
}