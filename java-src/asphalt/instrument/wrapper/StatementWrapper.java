package asphalt.instrument.wrapper;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import asphalt.instrument.IStopWatch;
import asphalt.instrument.JdbcEventFactory;
import asphalt.instrument.JdbcEventListener;
import asphalt.instrument.StopWatchNanos;

public class StatementWrapper<SQLExecution> implements Statement {

    private final Connection conn;
    private final Statement stmt;
    private final JdbcEventFactory<?, SQLExecution> eventFactory;
    private final JdbcEventListener<SQLExecution> sqlExecutionListener;

    public StatementWrapper(final Connection conn, final Statement stmt,
            final JdbcEventFactory<?, SQLExecution> eventFactory,
            final JdbcEventListener<SQLExecution> sqlExecutionListener) {
        this.conn = conn;
        this.stmt = stmt;
        this.eventFactory = eventFactory;
        this.sqlExecutionListener = sqlExecutionListener;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return stmt.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return stmt.isWrapperFor(iface);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("Expected valid SQL, but found NULL");
        }
        final SQLExecution event = eventFactory.sqlQueryExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final ResultSet rs = stmt.executeQuery(sql);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return rs;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("Expected valid SQL, but found NULL");
        }
        final SQLExecution event = eventFactory.sqlUpdateExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final int updateCount = stmt.executeUpdate(sql);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return updateCount;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public void close() throws SQLException {
        stmt.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return stmt.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        stmt.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return stmt.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        stmt.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        stmt.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return stmt.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        stmt.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        stmt.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        stmt.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        stmt.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("Expected valid SQL, but found NULL");
        }
        final SQLExecution event = eventFactory.sqlExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final boolean result = stmt.execute(sql);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return result;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return stmt.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return stmt.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return stmt.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        stmt.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return stmt.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        stmt.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return stmt.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return stmt.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return stmt.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        stmt.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        stmt.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return stmt.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return stmt.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return stmt.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        final SQLExecution event = eventFactory.sqlUpdateExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final int updateCount = stmt.executeUpdate(sql, autoGeneratedKeys);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return updateCount;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        final SQLExecution event = eventFactory.sqlUpdateExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final int updateCount = stmt.executeUpdate(sql, columnIndexes);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return updateCount;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        final SQLExecution event = eventFactory.sqlUpdateExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final int updateCount = stmt.executeUpdate(sql, columnNames);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return updateCount;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        final SQLExecution event = eventFactory.sqlExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final boolean result = stmt.execute(sql, autoGeneratedKeys);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return result;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        final SQLExecution event = eventFactory.sqlExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final boolean result = stmt.execute(sql, columnIndexes);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return result;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        final SQLExecution event = eventFactory.sqlExecutionEventForStatement(sql);
        final String id = sqlExecutionListener.before(event);
        final IStopWatch timer = new StopWatchNanos();
        try {
            final boolean result = stmt.execute(sql, columnNames);
            sqlExecutionListener.onSuccess(id, timer.elapsed(), event);
            return result;
        } catch (SQLException|RuntimeException e) {
            sqlExecutionListener.onError(id, timer.elapsed(), event, e);
            throw e;
        } finally {
            sqlExecutionListener.lastly(id, timer.elapsed(), event);
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return stmt.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return stmt.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        stmt.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return stmt.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        stmt.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return stmt.isCloseOnCompletion();
    }

}
