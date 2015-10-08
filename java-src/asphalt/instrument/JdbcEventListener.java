package asphalt.instrument;

public interface JdbcEventListener<E> {

    public static final JdbcEventListener<?> NOP = new JdbcEventListener<Object>() {
        @Override
        public String before(Object event) { return null; }
        @Override
        public void onSuccess(String id, long nanos, Object event) {}
        @Override
        public void onError(String id, long nanos, Object event, Exception e) {}
        @Override
        public void lastly(String id, long nanos, Object event) {}
    };

    /**
     * Triggered before a JDBC event takes place.
     * @param event the JDBC event
     * @return correlation ID
     */
    public String before(E event);

    /**
     * Triggered after a JDBC event takes place successfully.
     * @param id correlation ID
     * @param nanos nanoseconds elapsed since start
     * @param event the JDBC event
     */
    public void onSuccess(String id, long nanos, E event);

    /**
     * Triggered after a JDBC event results in error.
     * @param id correlation ID
     * @param nanos nanoseconds elapsed since start
     * @param event the JDBC event
     * @param e the exception
     */
    public void onError(String id, long nanos, E event, Exception e);

    /**
     * Triggered finally, after everything is over.
     * @param id correlation ID
     * @param nanos nanoseconds elapsed since start
     * @param event the JDBC event
     */
    public void lastly(String id, long nanos, E event);

}
