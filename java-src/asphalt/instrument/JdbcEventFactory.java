package asphalt.instrument;

public interface JdbcEventFactory<JdbcStatementCreation, SQLExecution> {

    public JdbcStatementCreation jdbcStatementCreationEvent();
    public JdbcStatementCreation jdbcPreparedStatementCreationEvent(String sql);
    public JdbcStatementCreation jdbcCallableStatementCreationEvent(String sql);

    public SQLExecution sqlExecutionEventForStatement(String sql);
    public SQLExecution sqlQueryExecutionEventForStatement(String sql);
    public SQLExecution sqlUpdateExecutionEventForStatement(String sql);

    public SQLExecution sqlExecutionEventForPreparedStatement(String sql);
    public SQLExecution sqlQueryExecutionEventForPreparedStatement(String sql);
    public SQLExecution sqlUpdateExecutionEventForPreparedStatement(String sql);

}
