package ibis.maestro;

/**
 * A message to tell the master the result of a ping message.
 * 
 * @author Kees van Reeuwijk.
 */
class PingReplyMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    private final double benchmarkScore;

    double getBEnchmarkScore() { return benchmarkScore; }
    
    PingReplyMessage( double benchmarkScore ){
        this.benchmarkScore = benchmarkScore;
    }
}
