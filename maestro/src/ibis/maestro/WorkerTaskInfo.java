package ibis.maestro;

/**
 * Information about a particular task type on a particular worker.
 */
final class WorkerTaskInfo {
    /** label of this worker/task combination in traces. */
    private final String label;

    private final TimeEstimate roundTripEstimate;

    /** How many instances of this task does this worker currently have? */
    private int outstandingTasks = 0;

    /** How many task instances has this worker executed until now? */
    private int executedTasks = 0;

    /** The maximal ever allowance given to this worker for this task. */
    private int maximalEverAllowance = 1;

    /** How many outstanding instances of this task should this worker maximally have? */
    private int maximalAllowance = 1;

    /** How long in ns it takes to complete the rest of the job this task belongs to. */
    private long remainingJobTime;

    /** If set, we are willing to allow an increased allowance if the worker
     * would request it.
     */
    private boolean mayIncreaseAllowance = false;

    /**
     * Returns the maximal round-trip interval for this worker and this task type, or
     * Long.MAX_VALUE if currently there are no task slots.
     * @param The number of reservations we already have.
     * @return The round-trip interval.
     */
    long estimateJobCompletion()
    {
        if( maximalAllowance == 0 ){
            System.err.println( "Internal error: zero allowance" );
        }
	if( remainingJobTime == Long.MAX_VALUE ) {
	    return Long.MAX_VALUE;
	}
        if( outstandingTasks >= maximalAllowance ){
            return Long.MAX_VALUE;
        }
	long roundTripTime = roundTripEstimate.getAverage();
	if( roundTripTime == Long.MAX_VALUE ) {
	    return Long.MAX_VALUE;
	}
	return roundTripTime + remainingJobTime;
    }

    long getAverageCompletionTime()
    {
        if( remainingJobTime == Long.MAX_VALUE ) {
            return Long.MAX_VALUE;
        }
        long average = roundTripEstimate.getAverage();
        if( average == Long.MAX_VALUE ) {
            return Long.MAX_VALUE;
        }
        return average + remainingJobTime;
    }

    /**
     * Constructs a new information class for a particular task type
     * for a particular worker.
     */
    WorkerTaskInfo( String label, boolean local )
    {
	this.label = label;
	long initialEstimate = local?0:1*Service.MILLISECOND_IN_NANOSECONDS;
	this.roundTripEstimate = new TimeEstimate( initialEstimate );
	this.remainingJobTime = 2*initialEstimate;
    }

    /**
     * Registers the completion of a task.
     * @param theRoundTripInterval The round-trip interval of this task.
     */
    void registerTaskCompleted( long theRoundTripInterval )
    {
	executedTasks++;
	roundTripEstimate.addSample( theRoundTripInterval );
	outstandingTasks--;
	if( Settings.traceWorkerProgress || Settings.traceRemainingJobTime ) {
	    System.out.println( label + ": new roundtrip time estimate: " + roundTripEstimate );
	}
    }

    void setCompletionInterval( long interval )
    {
	this.remainingJobTime = interval;
    }

    /** Register a new outstanding task. */
    void incrementOutstandingTasks()
    {
	outstandingTasks++;
	if( outstandingTasks >= maximalAllowance ) {
	    // Since we're now using the maximal allowance on this
	    // worker, we are willing to consider an increment.
	    mayIncreaseAllowance = true;
	}
    }

    /**
     * If allowed, increment the maximal number of outstanding tasks for
     * this worker and this type of work.
     *  @return True iff we really incremented the allowance.
     */
    protected boolean incrementAllowance()
    {
	if( !mayIncreaseAllowance ) {
	    return false;
	}
        maximalAllowance++;
        if( maximalEverAllowance<maximalAllowance ) {
            maximalEverAllowance = maximalAllowance;
        }
        mayIncreaseAllowance = false;
        return true;
    }
    
    String buildStatisticsString()
    {
	return " executed " + executedTasks + " tasks; maximal allowance " + maximalEverAllowance + ", estimated round-trip interval " + roundTripEstimate + ", remaining job time " + Service.formatNanoseconds( remainingJobTime );
    }

    /**
     * Limit the queue size on the worker.
     * @param queueLength The length of the current queue on the worker.
     * @param roundTripTime The time interval between the transmission of the task to the worker, and getting back the completion message.
     * @param workerDwellTime
     * @return True iff we really limited the allowance.
     */
    boolean limitAllowance( int queueLength, long roundTripTime, long workerDwellTime )
    {
	if( maximalAllowance<=1 ) {
	    // We must have an allowance of at least one.
	    return false;
	}
	if( queueLength>1 ) {
	//if( (maximalAllowance*workerDwellTime)>roundTripTime ) {
	    maximalAllowance--;
	    mayIncreaseAllowance = false;
	    return true;
	}
	return false;
    }

    /**
     * @return True iff this worker ever executed a task of this type.
     */
    public boolean didWork()
    {
        return (executedTasks != 0) || (outstandingTasks != 0);
    }

    /** Given a queue length on the worker, manipulate the allowance to
     * ensure the queue lengths stays within very reasonable limits.
     * @param queueLength The worker queue length.
     */
    protected void controlAllowance( int queueLength )
    {
        if( maximalAllowance == outstandingTasks && queueLength<1 ) {
            maximalAllowance++;
            if( maximalEverAllowance<maximalAllowance ) {
                maximalEverAllowance = maximalAllowance;
            }
        }
        else if( maximalAllowance == outstandingTasks && queueLength>2 && maximalAllowance>1 ) {
            maximalAllowance--;
        }
    }
}
