package ibis.maestro;

/**
 * Information the master has about a particular task type on a particular worker.
 */
final class WorkerTaskInfo {
    /** label of this worker/task combination in traces. */
    private final String label;

    private final TimeEstimate transmissionTimeEstimate;

    private final TimeEstimate roundtripTimeEstimate;

    /** How many instances of this task does this worker currently have? */
    private int outstandingTasks = 0;

    /** How many instance of this task do we reserve for this worker? */
    private int reservations = 0;

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
	return "[" + label + ": transmissionTimeEstimate=" + transmissionTimeEstimate + ",remainingJobTime=" + Service.formatNanoseconds(remainingJobTime) + ",outstandingTasks=" + outstandingTasks + ",maximalAllowance=" + maximalAllowance + "]";
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job.
     * @param tasks The number of tasks currently on the worker.
     * @return The completion time.
     */
    long getAverageCompletionTime( int currentTasks, int futureTasks )
    {
        /**
         * Don't give an estimate if we have to predict the future too far,
         * or of we just don't have the information.
         */
        if( maximalAllowance == 0 || futureTasks>maximalAllowance || remainingJobTime == Long.MAX_VALUE  ) {
            return Long.MAX_VALUE;
        }
        long transmissionTime = transmissionTimeEstimate.getAverage();
        int allTasks = currentTasks+futureTasks;
        long total;
        if( transmissionTime == Long.MAX_VALUE ) {
            total = Long.MAX_VALUE;
        }
        else if( allTasks>maximalAllowance ){
            // FIXME: temporary disabled all reservation stuff.
            total = Long.MAX_VALUE;
        }
        else {
            total = futureTasks*transmissionTime + (allTasks*(workerDwellTime/maximalAllowance)) + remainingJobTime;
        }
        if( Settings.traceRemainingJobTime ) {
            Globals.log.reportProgress(
                "getAverageCompletionTime(): " + label
                + " maximalAllowance=" + maximalAllowance
                + " currentTasks=" + currentTasks
                + " futureTasks=" + futureTasks
                + " xmitTime=" + Service.formatNanoseconds( transmissionTime )
                + " workerDwellTime=" + Service.formatNanoseconds( workerDwellTime )
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
        return getAverageCompletionTime( maximalAllowance, 0 );
    }

    /**
     * Returns the estimated time this worker will take to transmit this task to this worker,
     * complete it, and all remaining tasks in the job. Return
     * Long.MAX_VALUE if currently there are no task slots.
     * @return The completion time.
     */
    long estimateJobCompletion()
    {
        return getAverageCompletionTime( outstandingTasks, reservations+1 );
    }

    /**
     * @param label The label of this worker/task combination.
     * @param remainingTasks How many tasks there are in this job after the current task.
     * @param local True iff this is the local worker.
     * @param pingTime The ping time of this worker.
     * Constructs a new information class for a particular task type
     * for a particular worker.
     */
    WorkerTaskInfo( String label, int remainingTasks, boolean local, long pingTime )
    {
	this.label = label;
        this.maximalAllowance = local?3:0;
        this.maximalEverAllowance = maximalAllowance;

        // Totally unfounded guesses, but we should learn soon enough what the real values are...
	this.transmissionTimeEstimate = new TimeEstimate( pingTime );
	this.roundtripTimeEstimate = new TimeEstimate( Long.MAX_VALUE/4 ); // Pessimistic guess, since it is used for deadlines.
	this.workerDwellTime = 2*pingTime;
	this.remainingJobTime = remainingTasks*(workerDwellTime+pingTime);
	if( Settings.traceWorkerList || Settings.traceRemainingJobTime ) {
	    Globals.log.reportProgress( "Created new WorkerTaskInfo " + toString() );
	}
    }

    /**
     * Registers the completion of a task.
     * @param transmissionTime The transmission time of this task.
     * @param roundTripTime The total roundtrip time of this task.
     */
    void registerTaskCompleted( long transmissionTime, long roundTripTime )
    {
	executedTasks++;
        outstandingTasks--;
        roundtripTimeEstimate.addSample(roundTripTime );
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
	return "executed " + executedTasks + " tasks; maximal allowance " + maximalEverAllowance + ", xmit time " + transmissionTimeEstimate + ", dwell time " + Service.formatNanoseconds( workerDwellTime )+ ", remaining time " + Service.formatNanoseconds( remainingJobTime );
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

    protected int getSubmissions()
    {
	return executedTasks;
    }

    
    protected void resetReservations()
    {
	reservations = 0;
    }

    /**
     * Reserve a slot with this worker if necessary, and return true; or return false
     * if reservations are not necessary.
     * @return True if we did a reservation.
     */
    protected boolean reserveIfNeeded() {
        if( (maximalAllowance>0) && (outstandingTasks>=maximalAllowance) ) {
            reservations++;
            return true;
        }
        return false;
    }

    protected long estimateRoundtripTime() {
	return roundtripTimeEstimate.getAverage();
    }
}
