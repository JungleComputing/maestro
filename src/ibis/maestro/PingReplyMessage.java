package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A message to tell the master the result of a ping message.
 * 
 * @author Kees van Reeuwijk.
 */
class PingReplyMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    
    /** The score of the worker in the benchmark. */
    private final double benchmarkScore;
    
    /** The time it took to run the benchmark.
     * This time is different from the benchmark score because it may take
     * several attempts to get a long enough benchmark run.
     * Also, the benchmark score is the time for one iteration, but is computed from
     * at least 20, and possibly more, iterations.
     */
    private long benchmarkTime;

    /** The number of work threads of this worker. */
    public final int workThreads;

    // FIXME: inline this.
    ReceivePortIdentifier getWorker() { return source; }
    double getBenchmarkScore() { return benchmarkScore; }
    long getBenchmarkTime() { return benchmarkTime; }

    PingReplyMessage( ReceivePortIdentifier worker, int workThreads, double benchmarkScore, long benchmarkTime ){
	super( worker );
	this.workThreads = workThreads;
        this.benchmarkScore = benchmarkScore;
        this.benchmarkTime = benchmarkTime;
    }

    /**
     * Returns a string representation of this ping reply message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "Ping reply message from worker " + source + " score=" + benchmarkScore + " time=" + Service.formatNanoseconds( benchmarkTime );
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TraceEvent.Type getMessageType()
    {
	return TraceEvent.Type.PING_REPLY;
    }
}
