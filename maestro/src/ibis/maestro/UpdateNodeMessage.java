package ibis.maestro;


/**
 * A message from a worker to a master, telling it about its current
 * job completion times.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class UpdateNodeMessage extends Message {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /** For each type of task we know, the queue length on this worker. */
    final WorkerQueueInfo[] workerQueueInfo;

    final NodeIdentifier source;

    /**
     * Constructs a new work request message.
     * @param identifier The identifier to use.
     */
    UpdateNodeMessage( NodeIdentifier identifier, CompletionInfo[] completionInfo, WorkerQueueInfo[] workerQueueInfo )
    {
        source = identifier;
        this.completionInfo = completionInfo;
        this.workerQueueInfo = workerQueueInfo;
    }

    private String buildCompletionString()
    {
        StringBuilder b = new StringBuilder( "[" );
        boolean first = true;
        for( CompletionInfo i: completionInfo ) {
            if( i != null ) {
                if( first ) {
                    first = false;
                }
                else {
                    b.append( ',' );
                }
                b.append( i.toString() );
            }
        }
        b.append( ']' );
        return b.toString();
    }

    private String buildWorkerQueue()
    {
        StringBuilder b = new StringBuilder( "[" );
        boolean first = true;
        for( WorkerQueueInfo i: workerQueueInfo ) {
            if( i != null ) {
                if( first ) {
                    first = false;
                }
                else {
                    b.append( ',' );
                }
                b.append( i.toString() );
            }
        }
        b.append( ']' );
        return b.toString();
    }

    /**
     * Returns a string representation of update message. (Overrides method in superclass.)
     * @return The string representation.
     */
    @Override
    public String toString()
    {
        String completion = buildCompletionString();
        String workerQueue = buildWorkerQueue();
        return "Update " + completion + " " + workerQueue;
    }
}
