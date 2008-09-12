package ibis.maestro;

import ibis.ipl.IbisIdentifier;

import java.io.PrintStream;
import java.io.Serializable;
import java.util.Arrays;

/**
 * A packet of node update info.
 *
 * @author Kees van Reeuwijk.
 */
class NodePerformanceInfo implements Serializable
{
    private static final long serialVersionUID = 1L;

    /** The node this information is for. */
    final IbisIdentifier source;

    /** For each type of task we know, the estimated time it will
     * take to complete the remaining tasks of this job.
     */
    final long[] completionInfo;

    /** For each type of task we know, the queue length on this worker. */
    WorkerQueueInfo[] workerQueueInfo;

    long timeStamp;

    final int numberOfProcessors;

    NodePerformanceInfo( long[] completionInfo, WorkerQueueInfo[] workerQueueInfo,
        IbisIdentifier source, int numberOfProcessors, long timeStamp )
    {
        this.completionInfo = completionInfo;
        this.workerQueueInfo = workerQueueInfo;
        this.source = source;
        this.numberOfProcessors = numberOfProcessors;
        this.timeStamp = timeStamp;
    }

    NodePerformanceInfo getDeepCopy()
    {
        long completionInfoCopy[] = Arrays.copyOf( completionInfo, completionInfo.length );
        WorkerQueueInfo workerQueueInfoCopy[] = Arrays.copyOf( workerQueueInfo, workerQueueInfo.length );
        return new NodePerformanceInfo(
            completionInfoCopy,
            workerQueueInfoCopy,
            source,
            numberOfProcessors,
            timeStamp
        );
    }

    private String buildCompletionString()
    {
        StringBuilder b = new StringBuilder( "[" );
        for( long i: completionInfo ) {
            b.append( Utils.formatNanoseconds( i ) );
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
        String workerQueue = Arrays.deepToString( workerQueueInfo );
        return "Update @" + timeStamp + " " + workerQueue + " " + completion;
    }
    
    long estimateJobCompletion( LocalNodeInfo localNodeInfo, TaskType type, boolean ignoreBusyProcessors )
    {
        if( localNodeInfo == null ) {
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "No local node info" );
            }
            return Long.MAX_VALUE;
        }
        WorkerQueueInfo queueInfo = workerQueueInfo[type.index];
        long completionInterval = completionInfo[type.index];
        long unpredictableOverhead = 0L;

        if( queueInfo == null ) {
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "Node " + source + " does not provide queue info for type " + type );
            }
            return Long.MAX_VALUE;
        }
        int currentTasks = localNodeInfo.getCurrentTasks( type );
        if( type.unpredictable ) {
            if( ignoreBusyProcessors && currentTasks>=numberOfProcessors ) {
        	// Don't submit jobs, there are no idle processors.
                if( Settings.traceRemainingJobTime ) {
                    Globals.log.reportError( "Node " + source + " has no idle processors" );
                }
        	return Long.MAX_VALUE;
            }
            // The compute time is just based on an initial estimate. Give nodes
            // already running tasks some penalty to encourage spreading the load
            // over nodes.
            unpredictableOverhead = (currentTasks*queueInfo.computeTime)/10;
        }
        else {
            int allowance = localNodeInfo.getAllowance( type );
            if( ignoreBusyProcessors && currentTasks>=allowance ) {
        	return Long.MAX_VALUE;
            }
        }
        if( localNodeInfo.suspect ){
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "Node " + source + " is suspect, no completion estimate" );
            }
            return Long.MAX_VALUE;
        }
        if( completionInterval == Long.MAX_VALUE  ) {
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportError( "Node " + source + " has infinite completion time" );
            }
            return Long.MAX_VALUE;
        }
        long transmissionTime = localNodeInfo.getTransmissionTime( type );
        long total = Utils.safeAdd( transmissionTime, queueInfo.dequeueTime, queueInfo.computeTime, completionInterval, unpredictableOverhead );
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Estimated completion time for " + source + " is " + Utils.formatNanoseconds( total ) );
        }
        return total;
    }

    /**
     * Given the index of a type, return the interval in nanoseconds it
     * will take from the moment a task of this type leaves the master queue
     * until the entire job it belongs to is completed on the node we have
     * this update info for.
     * @param ix The index of the type we're interested in.
     */
    long getCompletionOnWorker( int ix, int nextIx )
    {
       WorkerQueueInfo info = workerQueueInfo[ix];
       long nextCompletionInterval;

       if( info == null ) {
           // We don't support this type.
           return Long.MAX_VALUE;
       }
       if( nextIx>=0 ) {
           nextCompletionInterval = completionInfo[nextIx];
       }
       else {
           nextCompletionInterval = 0L;
       }

       return Utils.safeAdd( info.dequeueTime, info.computeTime, nextCompletionInterval );
    }

    void print( PrintStream s )
    {
        for( WorkerQueueInfo i: workerQueueInfo ) {
            if( i == null ) {
                s.print ( WorkerQueueInfo.emptyFormat() );
            }
            else {
                s.print( i.format() );
                s.print( ' ' );
            }
        }
        s.print( " | " );
        for( long t: completionInfo ) {
            s.printf( "%8s ", Utils.formatNanoseconds( t ) );
        }
        s.println( source );
    }
    
    static void printTopLabel( PrintStream s )
    {
        for( TaskType t: Globals.allTaskTypes ){
            s.print( WorkerQueueInfo.topLabelType( t ) );
            s.print( ' ' );
        }
        s.print( " | " );
        for( TaskType t: Globals.allTaskTypes ){
            s.printf( "%8s ", t );
        }
        s.println();
        for( @SuppressWarnings("unused") TaskType t: Globals.allTaskTypes ){
            s.print( WorkerQueueInfo.topLabel() );
            s.print( ' ' );
        }
        s.print( " | " );
        s.println();
    }

    void failTask( TaskType type )
    {
	WorkerQueueInfo info = workerQueueInfo[type.index];
	if( info != null ) {
	    info.failTask();
	}
    }

    void setComputeTime( TaskType type, long t )
    {
	WorkerQueueInfo info = workerQueueInfo[type.index];
	if( info != null ) {
	    info.setComputeTime( t );
	}	
    }

    void setQueueTimePerTask( TaskType type, long queueInterval, int queueLength )
    {
	WorkerQueueInfo info = workerQueueInfo[type.index];
	if( info != null ) {
	    info.setQueueTimePerTask( queueInterval, queueLength );
	}	
    }

    void setLocalQueueLength( TaskType type, int sz )
    {
	WorkerQueueInfo info = workerQueueInfo[type.index];
	if( info != null ) {
	    info.setQueueLength( sz );
	}	
    }
}
