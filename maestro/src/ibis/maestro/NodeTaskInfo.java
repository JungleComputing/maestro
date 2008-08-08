package ibis.maestro;

import java.io.PrintStream;

/**
 * Information the node has about a particular task type on a particular node.
 */
final class NodeTaskInfo {
    final WorkerQueueTaskInfo taskInfo;

    private final NodeInfo nodeInfo;

    private final TimeEstimate transmissionTimeEstimate;

    private final TimeEstimate roundtripTimeEstimate;

    /** How many instances of this task does this worker currently have? */
    private int outstandingTasks = 0;

    /** How many task instances has this worker executed until now? */
    private int executedTasks = 0;

    /** The maximal ever allowance given to this worker for this task. */
    private int maximalEverAllowance;

    /** How many outstanding instances of this task should this worker maximally have? */
    private int maximalAllowance;

    /**
     * Constructs a new information class for a particular task type
     * for a particular worker.
     * @param taskInfo The type of task we have administration for.
     * @param worker The worker we have administration for.
     * @param local True iff this is the local worker.
     * @param pingTime The ping time of this worker.
     */
    NodeTaskInfo( WorkerQueueTaskInfo taskInfo, NodeInfo worker, boolean local, long pingTime )
    {
        this.taskInfo = taskInfo;
        this.nodeInfo = worker;
        this.maximalAllowance = local?1:0;
        this.maximalEverAllowance = maximalAllowance;

        // Totally unfounded guesses, but we should learn soon enough what the real values are...
        this.transmissionTimeEstimate = new TimeEstimate( pingTime );
        this.roundtripTimeEstimate = new TimeEstimate( 2*pingTime );
        if( Settings.traceWorkerList || Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( "Created new WorkerTaskInfo " + toString() );
        }
    }

    /**
     * @return A string representation of this class instance.
     */
    @Override
    public String toString()
    {
        return "[taskInfo=" + taskInfo + " worker=" + nodeInfo + " transmissionTimeEstimate=" + transmissionTimeEstimate + " outstandingTasks=" + outstandingTasks + " maximalAllowance=" + maximalAllowance + "]";
    }

    /**
     * Registers the completion of a task.
     * @param transmissionTime The transmission time of this task.
     * @param roundtripTime The total roundtrip time of this task.
     */
    synchronized void registerTaskCompleted( long transmissionTime, long roundtripTime )
    {
        executedTasks++;
        outstandingTasks--;
        roundtripTimeEstimate.addSample(roundtripTime );
        transmissionTimeEstimate.addSample( transmissionTime );
        String label = "task=" + taskInfo + " worker=" + nodeInfo;
        if( Settings.traceNodeProgress || Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( label + ": roundTripTimeEstimate=" + roundtripTimeEstimate + " transimssionTimeEstimate=" + transmissionTimeEstimate );
        }
    }

    /** Register a new outstanding task. */
    synchronized void incrementOutstandingTasks()
    {
        outstandingTasks++;
    }

    /**
     * @return True iff this worker ever executed a task of this type.
     */
    private synchronized boolean didWork()
    {
        return (executedTasks != 0) || (outstandingTasks != 0);
    }

    /** Given a queue length on the worker, manipulate the allowance to
     * ensure the queue lengths stays within very reasonable limits.
     * @param queueLength The worker queue length.
     */
    private void controlAllowance( int queueLength )
    {
        if( maximalAllowance == outstandingTasks ) {
            // We can only regulate the allowance if we are
            // at our current maximal allowance.
            if( queueLength<1 ) {
                maximalAllowance++;
            }
            else if( queueLength>4 ) {
                // There are a lot of items in the queue; take a larger step.
                maximalAllowance -= 2;
            }
            else if( queueLength>1 ) {
                maximalAllowance--;
            }
            if( maximalAllowance<0 ) {
                // Yes, we are prepared to cut off a worker entirely.
                // However, if a worker reports it has room its queue
                // we will increase its allowance again.
                maximalAllowance = 0;
            }
            if( maximalAllowance>10 ){
                // We arbitrarily limit the maximal allowance to
                // 10 since larger than that doesn't seem useful.
                // FIXME: try to base the limit on something reasoned.
                maximalAllowance = 10;
            }
            if( maximalEverAllowance<maximalAllowance ) {
                maximalEverAllowance = maximalAllowance;
            }
        }
    }

    int getSubmissions()
    {
        return executedTasks;
    }

    synchronized long estimateRoundtripTime()
    {
        return roundtripTimeEstimate.getAverage();
    }

    synchronized void printStatistics( PrintStream s )
    {
        if( didWork() ) {
            s.println( "  " + taskInfo.type + ": executed " + executedTasks + " tasks; maximal allowance " + maximalEverAllowance + ", xmit time " + transmissionTimeEstimate );
        }
    }

    synchronized void updateRoundtripTimeEstimate( long t )
    {
        roundtripTimeEstimate.addSample( t );
    }

    synchronized void setWorkerQueueInfo( WorkerQueueInfo info )
    {
        controlAllowance( info.queueLength );        
    }

    synchronized int getCurrentTasks()
    {
        return outstandingTasks;
    }

    synchronized long getTransmissionTime()
    {
        return transmissionTimeEstimate.getAverage();
    }

    synchronized int getMaximalAllowance()
    {
        return maximalAllowance;
    }
}
