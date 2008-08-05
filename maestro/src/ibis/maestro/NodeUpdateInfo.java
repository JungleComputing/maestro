package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;

/**
 * A packet of node update info.
 *
 * @author Kees van Reeuwijk.
 */
public class NodeUpdateInfo implements Serializable
{
    private static final long serialVersionUID = 1L;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final CompletionInfo[] completionInfo;

    /** For each type of task we know, the queue length on this worker. */
    final WorkerQueueInfo[] workerQueueInfo;

    final IbisIdentifier source;

    final boolean masterHasWork;

    final long timestamp;

    NodeUpdateInfo( CompletionInfo[] completionInfo, WorkerQueueInfo[] workerQueueInfo,
        IbisIdentifier source, boolean masterHasWork )
    {
        this.completionInfo = completionInfo;
        this.workerQueueInfo = workerQueueInfo;
        this.source = source;
        this.masterHasWork = masterHasWork;
        this.timestamp = System.nanoTime();
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
