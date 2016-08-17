package fr.mediametrie.internet.streaming.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface that defines a {@link Connection} supplier
 */
@FunctionalInterface
public interface ConnectionSupplier {

    /**
     * Return a new connection
     *
     * @param url      database server urk
     * @param user     username
     * @param password password
     * @return a new connection
     * @throws SQLException
     */
    Connection getConnection(String url, String user, String password) throws SQLException;
}
