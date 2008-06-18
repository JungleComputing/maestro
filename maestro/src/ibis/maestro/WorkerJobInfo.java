package ibis.maestro;

/**
 * Information about a particular job type on a particular worker.
 */
final class WorkerJobInfo {
    /** label of this worker/job combination in traces. */
    private final String label;

    private final TimeEstimate roundTripEstimate;

    /** How many instances of this job does this worker currently have? */
    private int outstandingJobs = 0;

    /** How many job instances has this worker executed until now? */
    private int executedJobs = 0;

    /** The maximal ever allowance given to this worker for this job. */
    private int maximalEverAllowance = 1;

    /** How many outstanding instances of this job should this worker maximally have? */
    private int maximalAllowance = 1;

    /** How long in ns it takes to complete the rest of the task this job belongs to. */
    private long remainingTasksTime;

    /** If set, we are willing to allow an increased allowance if the worker
     * would request it.
     */
    private boolean mayIncreaseAllowance = false;

    /**
     * Returns the maximal round-trip interval for this worker and this job type, or
     * Long.MAX_VALUE if currently there are no job slots.
     * @param The number of reservations we already have.
     * @return The round-trip interval.
     */
    long estimateTaskCompletion()
    {
        if( maximalAllowance == 0 ){
            System.err.println( "Internal error: zero allowance" );
        }
	if( remainingTasksTime == Long.MAX_VALUE ) {
	    return Long.MAX_VALUE;
	}
        if( outstandingJobs >= maximalAllowance ){
            return Long.MAX_VALUE;
        }
	long roundTripTime = roundTripEstimate.getAverage();
	if( roundTripTime == Long.MAX_VALUE ) {
	    return Long.MAX_VALUE;
	}
	return roundTripTime + remainingTasksTime;
    }

    long getAverageCompletionTime()
    {
        if( remainingTasksTime == Long.MAX_VALUE ) {
            return Long.MAX_VALUE;
        }
        long average = roundTripEstimate.getAverage();
        if( average == Long.MAX_VALUE ) {
            return Long.MAX_VALUE;
        }
        return average + remainingTasksTime;
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     */
    WorkerJobInfo( String label, boolean local )
    {
	this.label = label;
	long initialEstimate = local?0:1*Service.MILLISECOND_IN_NANOSECONDS;
	this.roundTripEstimate = new TimeEstimate( initialEstimate );
	this.remainingTasksTime = 2*initialEstimate;
    }

    /**
     * Registers the completion of a job.
     * @param theRoundTripInterval The round-trip interval of this job.
     */
    void registerJobCompleted( long theRoundTripInterval )
    {
	executedJobs++;
	roundTripEstimate.addSample( theRoundTripInterval );
	outstandingJobs--;
	if( Settings.traceWorkerProgress || Settings.traceRemainingTaskTime ) {
	    System.out.println( label + ": new roundtrip time estimate: " + roundTripEstimate );
	}
    }

    void setCompletionInterval( long interval )
    {
	this.remainingTasksTime = interval;
    }

    /** Register a new outstanding job. */
    void incrementOutstandingJobs()
    {
	outstandingJobs++;
	if( outstandingJobs >= maximalAllowance ) {
	    // Since we're now using the maximal allowance on this
	    // worker, we are willing to consider an increment.
	    mayIncreaseAllowance = true;
	}
    }

    /**
     * If allowed, increment the maximal number of outstanding jobs for
     * this worker and this type of work.
     *  @return True iff we really incremented the allowance.
     */
    boolean incrementAllowance()
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

    /**
     * Try to reduce the allowance of this worker and job.
     * This may not succeed if the new allowance would
     * be too low.
     * @return True iff we reduced the allowance.
     */
    boolean decrementAllowance()
    {
	if( maximalAllowance<=1 ) {
	    return false;
	}
	maximalAllowance--;
	mayIncreaseAllowance = false;
	return true;
    }
    
    String buildStatisticsString()
    {
	return " executed " + executedJobs + " jobs; maximal allowance " + maximalEverAllowance + ", estimated round-trip interval " + roundTripEstimate + ", remaining tasks time " + Service.formatNanoseconds( remainingTasksTime );
    }

    /**
     * Limit the queue size on the worker.
     * @param roundTripTime
     * @param workerDwellTime
     * @return True iff we really limited the allowance.
     */
    public boolean limitAllowance( long roundTripTime, long workerDwellTime )
    {
	if( maximalAllowance<=1 ) {
	    // We must have an allowance of at least one.
	    return false;
	}
	if( ((maximalAllowance+1)*workerDwellTime)>roundTripTime ) {
	    
	    maximalAllowance--;
	    mayIncreaseAllowance = false;
	    return true;
	}
	return false;
    }

    /**
     * @return True iff this worker ever executed a job of this type.
     */
    public boolean didWork()
    {
        return (executedJobs != 0) || (outstandingJobs != 0);
    }
}
