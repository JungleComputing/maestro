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
    private final ReceivePortIdentifier worker;
    
    /** The score of the worker in the benchmark. */
    private final double benchmarkScore;
    
    /** The time it took to run the benchmark.
     * This time is different from the benchmark score because it may take
     * several attempts to get a long enough benchmark run.
     * Also, the benchmark score is the time for one iteration, but is computed from
     * at least 20, and possibly more, iterations.
     */
    private long benchmarkTime;

    ReceivePortIdentifier getWorker() { return worker; }
    double getBenchmarkScore() { return benchmarkScore; }
    long getBenchmarkTime() { return benchmarkTime; }

    PingReplyMessage( ReceivePortIdentifier worker, double benchmarkScore, long benchmarkTime ){
        this.worker = worker;
        this.benchmarkScore = benchmarkScore;
        this.benchmarkTime = benchmarkTime;
    }

}
