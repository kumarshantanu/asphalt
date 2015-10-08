package asphalt.instrument;

public class StopWatchNanos implements IStopWatch {

    private final long start;

    public StopWatchNanos() {
        start = System.nanoTime();
    }

    public StopWatchNanos(long init) {
        start = init;
    }

    @Override
    public long elapsed() {
        return System.nanoTime() - start;
    }

}
