package ibis.maestro;

/**
 * Information about a particular task type on a particular worker.
 */
final class WorkerTaskInfo {
    /** label of this worker/task combination in traces. */
    private final String label;

    private final TimeEstimate transmissionTimeEstimate;

    /** How many instances of this task does this worker currently have? */
    private int outstandingTasks = 0;

    /** How many task instances has this worker executed until now? */
    private int executedTasks = 0;

    /** The maximal ever allowance given to this worker for this task. */
    private int maximalEverAllowance;

    /** How many outstanding instances of this task should this worker maximally have? */
    private int maximalAllowance;

    /** How long in ns the job is estimated to dwell on the worker for queuing and execution. */
    private long workerDwellTime;

    /** How long in ns it takes to complete the rest of the job this task belongs to. */
    private long remainingJobTime;

    /**
     * @return A string representation of this class instance.
     */
    @Override
    public String toString()
    {
	return "[" + label + ": roundTripEstimate=" + transmissionTimeEstimate + ",remainingJobTime=" + Service.formatNanoseconds(remainingJobTime) + ",outstandingTasks=" + outstandingTasks + ",maximalAllowance=" + maximalAllowance + "]";
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job.
     * @return The completion time.
     */
    long getAverageCompletionTime()
    {
        if( maximalAllowance == 0 || remainingJobTime == Long.MAX_VALUE ) {
            return Long.MAX_VALUE;
        }
        long transmissionTime = transmissionTimeEstimate.getAverage();
        if( transmissionTime == Long.MAX_VALUE ) {
            return Long.MAX_VALUE;
        }
        return transmissionTime + workerDwellTime + remainingJobTime;
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job. Return
     * Long.MAX_VALUE if currently there are no task slots.
     * @return The completion time.
     */
    long estimateJobCompletion()
    {
        if( outstandingTasks >= maximalAllowance ){
            return Long.MAX_VALUE;
        }
        return getAverageCompletionTime();
    }

    /**
     * Constructs a new information class for a particular task type
     * for a particular worker.
     */
    WorkerTaskInfo( String label, int remainingTasks, boolean local )
    {
	this.label = label;
        this.maximalAllowance = local?2:0;
        this.maximalEverAllowance = maximalAllowance;

        // A totally unfounded guess, but we should learn soon enough what the real value is..
	long initialEstimate = local?0:10*Service.MILLISECOND_IN_NANOSECONDS;
	this.transmissionTimeEstimate = new TimeEstimate( initialEstimate );
	this.workerDwellTime = Service.MILLISECOND_IN_NANOSECONDS;
	this.remainingJobTime = 2*remainingTasks*initialEstimate;
	if( Settings.traceWorkerList ) {
	    Globals.log.reportProgress( "Created new WorkerTaskInfo " + toString() );
	}
    }

    /**
     * Registers the completion of a task.
     * @param transmissionTime The transmission time of this task.
     */
    void registerTaskCompleted( long transmissionTime )
    {
	executedTasks++;
        outstandingTasks--;
	transmissionTimeEstimate.addSample( transmissionTime );
	if( Settings.traceWorkerProgress || Settings.traceRemainingJobTime ) {
	    System.out.println( label + ": new transmission time estimate: " + transmissionTimeEstimate );
	}
    }

    void setCompletionTime( long remainingJobTime )
    {
	this.remainingJobTime = remainingJobTime;
    }

    void setDwellTime( long workerDwellTime )
    {
        this.workerDwellTime = workerDwellTime;
    }

    /** Register a new outstanding task. */
    void incrementOutstandingTasks()
    {
	outstandingTasks++;
    }

    String buildStatisticsString()
    {
	return " executed " + executedTasks + " tasks; maximal allowance " + maximalEverAllowance + ", estimated transmission time " + transmissionTimeEstimate + ", worker dwell time " + Service.formatNanoseconds( workerDwellTime )+ ", remaining job time " + Service.formatNanoseconds( remainingJobTime );
    }

    /**
     * @return True iff this worker ever executed a task of this type.
     */
    protected boolean didWork()
    {
        return (executedTasks != 0) || (outstandingTasks != 0);
    }

    /** Given a queue length on the worker, manipulate the allowance to
     * ensure the queue lengths stays within very reasonable limits.
     * @param queueLength The worker queue length.
     */
    protected void controlAllowance( int queueLength )
    {
	if( maximalAllowance>0 && maximalAllowance == outstandingTasks ) {
	    // We can only regulate the allowance if we
	    // at our current maximal allowance.
	    if( queueLength<1 ) {
		maximalAllowance++;
	    }
	    else if( queueLength>4 ) {
		// There are a lot of items in the queue; take a larger step.
		maximalAllowance -= 2;
	    }
	    else if( queueLength>2 ) {
		maximalAllowance--;
	    }
	    if( maximalAllowance<0 ) {
		// Yes, we are prepared to cut off a worker entirely.
		// However, if our work queue gets too large, we will
		// enable this worker again.
		maximalAllowance = 0;
	    }
	    if( maximalEverAllowance<maximalAllowance ) {
		maximalEverAllowance = maximalAllowance;
	    }
	}
    }

    protected boolean activate()
    {
        if( maximalAllowance>0 || remainingJobTime == Long.MAX_VALUE ) {
            return false;
        }
        maximalAllowance = 1;
	return true;
    }

    /**
     * @return True iff this worker is ready to handle this task, but isn't doing so yet.
     */
    boolean isIdleWorker()
    {
        if( maximalAllowance>0 || remainingJobTime == Long.MAX_VALUE ) {
            return false;
        }
        return true;
    }

    /** We now know this worker has the given ping time. Use this as the first estimate
     * for the transmission time if we don't have anything better.
     * @param pingTime The time in ns to ping this worker.
     */
    void setPingTime( long pingTime )
    {
        transmissionTimeEstimate.setInitialEstimate( pingTime );
    }
}
