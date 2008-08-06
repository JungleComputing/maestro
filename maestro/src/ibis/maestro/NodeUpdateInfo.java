package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.Serializable;
import java.util.Arrays;

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

    NodeUpdateInfo getDeepCopy()
    {
        CompletionInfo completionInfoCopy[] = Arrays.copyOf( completionInfo, completionInfo.length );
        WorkerQueueInfo workerQueueInfoCopy[] = Arrays.copyOf( workerQueueInfo, workerQueueInfo.length );
        return new NodeUpdateInfo(
            completionInfoCopy,
            workerQueueInfoCopy,
            source,
            masterHasWork
        );
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
    
    private CompletionInfo searchCompletionInfoForType( TaskType type )
    {
        for( CompletionInfo ci: completionInfo )
        {
            if( ci != null ) {
                if( ci.type.index == type.index ) {
                    return ci;
                }
            }
        }
        return null;
    }
    
    private WorkerQueueInfo searchWorkerQueueInfo( TaskType type )
    {
        for( WorkerQueueInfo info: workerQueueInfo ) {
            if( info != null ) {
                if( info.type.index == type.index ) {
                    return info;
                }
            }
        }
        return null;
    }

    long estimateJobCompletion( LocalNodeInfo localNodeInfo, TaskType type )
    {
        WorkerQueueInfo queueInfo = searchWorkerQueueInfo( type );
        CompletionInfo typeCompletionInfo = searchCompletionInfoForType( type );
        
        if( queueInfo == null ) {
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "Node " + source + " does not provide queue info for type " + type );
            }
            return Long.MAX_VALUE;
        }
        if( localNodeInfo.suspect ){
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "Node " + source + " is suspect, no completion estimate" );
            }
            return Long.MAX_VALUE;
        }
        if( typeCompletionInfo != null && typeCompletionInfo.completionInterval == Long.MAX_VALUE  ) {
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "Node " + source + " has infinite completion time" );
            }
            return Long.MAX_VALUE;
        }
        long transmissionTime = localNodeInfo.getTransmissionTime( type );
        int allTasks = localNodeInfo.getCurrentTasks( type )+1;
        long total = transmissionTime + queueInfo.dequeueTime*allTasks + queueInfo.computeTime;
        if( typeCompletionInfo != null ) {
            total += typeCompletionInfo.completionInterval;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Estimated completion time for " + source + " is " + Service.formatNanoseconds( total ) );
        }
        return total;
    }
}
