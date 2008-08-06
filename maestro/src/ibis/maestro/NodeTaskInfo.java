package ibis.maestro;

import java.io.PrintStream;

/**
 * Information the node has about a particular task type on a particular node.
 */
final class NodeTaskInfo {
    final TaskInfo taskInfo;

    final NodeInfo nodeInfo;

    private final TimeEstimate transmissionTimeEstimate;

    private final TimeEstimate roundtripTimeEstimate;

    private final TimeEstimate roundtripErrorEstimate;

    /** How many instances of this task does this worker currently have? */
    private int outstandingTasks = 0;

    /** How many task instances has this worker executed until now? */
    private int executedTasks = 0;

    /** The maximal ever allowance given to this worker for this task. */
    private int maximalEverAllowance;

    /** How many outstanding instances of this task should this worker maximally have? */
    private int maximalAllowance;

    /** How long in ns between dequeueings. */
    private long dequeueTime;

    /** How long in ns is a task estimated to compute. */
    private long computeTime;

    /** How long in ns it takes to complete the rest of the job this task belongs to. */
    private long remainingJobTime;

    private final boolean traceStats;

    private final long startTime = System.nanoTime();
    /**
     * Constructs a new information class for a particular task type
     * for a particular worker.
     * @param taskInfo The type of task we have administration for.
     * @param worker The worker we have administration for.
     * @param local True iff this is the local worker.
     * @param pingTime The ping time of this worker.
     */
    NodeTaskInfo( TaskInfo taskInfo, NodeInfo worker, boolean local, long pingTime )
    {
        this.taskInfo = taskInfo;
        this.nodeInfo = worker;
        this.maximalAllowance = local?1:0;
        this.maximalEverAllowance = maximalAllowance;

        this.traceStats = System.getProperty( "ibis.maestro.traceWorkerStatistics" ) != null;
        // Totally unfounded guesses, but we should learn soon enough what the real values are...
        this.transmissionTimeEstimate = new TimeEstimate( pingTime );
        this.roundtripTimeEstimate = new TimeEstimate( 2*pingTime );
        this.roundtripErrorEstimate = new TimeEstimate( 2*pingTime );
        this.computeTime = 2*pingTime;
        this.dequeueTime = 1*pingTime;
        this.remainingJobTime = taskInfo.type.remainingTasks*(computeTime+dequeueTime+pingTime);
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
        return "[taskInfo=" + taskInfo + " worker=" + nodeInfo + " transmissionTimeEstimate=" + transmissionTimeEstimate + " remainingJobTime=" + Service.formatNanoseconds(remainingJobTime) + ",outstandingTasks=" + outstandingTasks + ",maximalAllowance=" + maximalAllowance + "]";
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job.
     * @param tasks The number of tasks currently on the worker.
     * @return The completion time.
     */
    private synchronized long getAverageCompletionTime( int currentTasks )
    {
        /**
         * Don't give an estimate if we have to predict the future too far,
         * or of we just don't have the information.
         */
        if( remainingJobTime == Long.MAX_VALUE  ) {
            if( Settings.traceRemainingJobTime ) {
                Globals.log.reportProgress(
                    "getAverageCompletionTime(): type=" + taskInfo
                    + " worker=" + nodeInfo
                    + " infinite: "
                    + " isSuspect=" + nodeInfo.isSuspect()
                    + " remainingJobTime=" + Service.formatNanoseconds( remainingJobTime )
                );
            }
            return Long.MAX_VALUE;
        }
        long transmissionTime = transmissionTimeEstimate.getAverage();
        int allTasks = currentTasks+1;
        long total = transmissionTime + this.dequeueTime*allTasks + this.computeTime + remainingJobTime;
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress(
                "getAverageCompletionTime(): type=" + taskInfo
                + " worker=" + nodeInfo
                + " maximalAllowance=" + maximalAllowance
                + " currentTasks=" + currentTasks
                + " xmitTime=" + Service.formatNanoseconds( transmissionTime )
                + " dequeueTime=" + Service.formatNanoseconds( dequeueTime )
                + " computeTime=" + Service.formatNanoseconds( computeTime )
                + " remainingJobTime=" + Service.formatNanoseconds( remainingJobTime )
                + " total=" + Service.formatNanoseconds( total )
            );
        }
        return total;
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job.
     * @return The completion time.
     */
    long getAverageCompletionTime()
    {
        if( nodeInfo.isSuspect() ) {
            return Long.MAX_VALUE;
        }
        return getAverageCompletionTime( maximalAllowance-1 );
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job. Return
     * Long.MAX_VALUE if currently there are no task slots.
     * @return The completion time.
     */
    long estimateJobCompletion()
    {
        if( nodeInfo.isSuspect() ) {
            return Long.MAX_VALUE;
        }
        return getAverageCompletionTime( outstandingTasks );
    }

    /**
     * Registers the completion of a task.
     * @param transmissionTime The transmission time of this task.
     * @param roundtripTime The total roundtrip time of this task.
     * @param roundtripError The error in the previously predicted roundtrip time of this task.
     */
    synchronized void registerTaskCompleted( long transmissionTime, long roundtripTime, long roundtripError )
    {
        executedTasks++;
        outstandingTasks--;
        roundtripTimeEstimate.addSample(roundtripTime );
        roundtripErrorEstimate.addSample( roundtripError );
        transmissionTimeEstimate.addSample( transmissionTime );
        String label = "task=" + taskInfo + " worker=" + nodeInfo;
        if( Settings.traceNodeProgress || Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress( label + ": roundTripTimeEstimate=" + roundtripTimeEstimate + " roundTripErrorEstimate=" + roundtripErrorEstimate + " transimssionTimeEstimate=" + transmissionTimeEstimate );
        }
        if( traceStats ) {
            double now = 1e-9*(System.nanoTime()-startTime);
            System.out.println( "TRACE:newRoundtripTime " + label + " " + now + " " + 1e-9*roundtripTime );
            System.out.println( "TRACE:newTransmissionTime " + label + " " + now + " " + 1e-9*transmissionTime );
            System.out.println( "TRACE:roundtripTime " + label + " " + now + " " + 1e-9*roundtripTimeEstimate.getAverage() + " " + 1e-9*roundtripErrorEstimate.getAverage() );
            System.out.println( "TRACE:transmissionTime " + label + " " + now + " " + 1e-9*transmissionTimeEstimate.getAverage() );
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
    private boolean didWork()
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

    /**
     * @return True iff this worker is ready to handle this task.
     */
    synchronized boolean canProcessNow()
    {
        return outstandingTasks<maximalAllowance;
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
        if( true || didWork() ) {
            s.println( "  " + taskInfo.type + ": executed " + executedTasks + " tasks; maximal allowance " + maximalEverAllowance + ", xmit time " + transmissionTimeEstimate + " dequeueTime=" + Service.formatNanoseconds( dequeueTime )+ " computeTime=" + Service.formatNanoseconds( computeTime )+ ", remaining time " + Service.formatNanoseconds( remainingJobTime ) );
        }
    }

    synchronized void updateRoundtripTimeEstimate( long t )
    {
        roundtripTimeEstimate.addSample( t );
    }

    /** FIXME.
     * @param info
     */
    synchronized void setWorkerQueueInfo( WorkerQueueInfo info )
    {
        this.dequeueTime = info.dequeueTime;
        this.computeTime = info.computeTime;
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

    /** FIXME.
     * @return
     */
    synchronized long getPredictedDuration()
    {
        return roundtripTimeEstimate.getAverage();
    }
}
