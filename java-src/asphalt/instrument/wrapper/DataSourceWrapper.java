package asphalt.instrument.wrapper;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import asphalt.instrument.JdbcEventFactory;
import asphalt.instrument.JdbcEventListener;

public class DataSourceWrapper<JdbcStatementCreation, SQLExecution> implements DataSource {

    private final DataSource ds;
    private final JdbcEventFactory<JdbcStatementCreation, SQLExecution> eventFactory;
    private final JdbcEventListener<JdbcStatementCreation> stmtCreationListener;
    private final JdbcEventListener<SQLExecution> sqlExecutionListener;

    public DataSourceWrapper(DataSource ds, final JdbcEventFactory<JdbcStatementCreation, SQLExecution> eventFactory,
            final JdbcEventListener<JdbcStatementCreation> stmtCreationListener,
            final JdbcEventListener<SQLExecution> sqlExecutionListener) {
        this.ds = ds;
        this.eventFactory = eventFactory;
        this.stmtCreationListener = stmtCreationListener;
        this.sqlExecutionListener = sqlExecutionListener;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return ds.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        ds.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        ds.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return ds.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ds.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ds.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ds.isWrapperFor(iface);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new ConnectionWrapper<>(ds.getConnection(), eventFactory, stmtCreationListener, sqlExecutionListener);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new ConnectionWrapper<>(ds.getConnection(username, password), eventFactory,
                stmtCreationListener, sqlExecutionListener);
    }

}
