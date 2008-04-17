/**
 * Information about a particular job type on a particular worker.
 */
package ibis.maestro;

import java.util.Hashtable;


class WorkerJobInfo {
    private final TimeEstimate roundTripEstimate = new TimeEstimate();

    /** How many instances of this job does this worker currently have? */
    private int outstandingJobs = 0;

    private int executedJobs = 0;

    private int maximalEverAllowance = 1;

    /** How many instance of this job should this worker maximally have? */
    private int maximalAllowance = 1;

    /** How long it takes to complete the rest of the task this job belongs to. */
    long remainingTaskTime = 0L;

    /** We have reached the maximal allowance of this worker, so we are
     * willing to consider an increased allowance.
     */
    private boolean mayIncreaseAllowance = false;

    private final Hashtable<TaskInstanceIdentifier,Long> workerJobInfoTable = new Hashtable<TaskInstanceIdentifier, Long>();

    /**
     * Returns the maximal round-trip interval for this worker and this job type, or
     * a very pessimistic estimate if currently there are no job slots.
     * @param The number of reservations we already have.
     * @return The round-trip interval.
     */
    long estimateRoundTripInterval()
    {
        if( maximalAllowance == 0 ){
            System.err.println( "Internal error: zero allowance" );
        }
        if( outstandingJobs>=maximalAllowance ){
            return Long.MAX_VALUE;
        }
	return roundTripEstimate.getEstimate();
    }

    /**
     * Returns the maximal round-trip interval for this worker and this job type, or
     * a very pessimistic estimate if currently there are no job slots.
     * @param The number of reservations we already have.
     * @return The round-trip interval.
     */
    long estimateTaskCompletion()
    {
	return remainingTaskTime + estimateRoundTripInterval();
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     */
    WorkerJobInfo()
    {
        // Nothing to do.
    }

    /**
     * Registers the completion of a job.
     * @param theRoundTripInterval The round-trip interval of this job.
     * @param taskCompletionTime The time it takes to complete the entire task this job belongs to.
     */
    void registerJobCompleted( long theRoundTripInterval, long taskCompletionTime )
    {
	executedJobs++;
	roundTripEstimate.addSample( theRoundTripInterval );
	this.remainingTaskTime = taskCompletionTime;
	outstandingJobs--;
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "New roundtrip time estimate: " + roundTripEstimate );
	}
    }

    /** Register a new outstanding job. */
    public void incrementOutstandingJobs()
    {
	outstandingJobs++;
	if( outstandingJobs == maximalAllowance ) {
	    // Since we're now using the maximal allowance on this
	    // worker, we are willing to consider an increment.
	    mayIncreaseAllowance = true;
	}
    }

    /**
     * Increment the maximal number of outstanding jobs for this worker and
     *  this type of work.
     *  @return True iff we really incremented the allowance.
     */
    public boolean incrementAllowance()
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
	return "executed " + executedJobs + " jobs; maximal ever allowance: " + maximalEverAllowance + " estimated round-trip interval: " + roundTripEstimate;
    }

    long getRemainingTaskTime( TaskInstanceIdentifier task )
    {
	Long val = workerJobInfoTable.get( task );
	if( val == null ) {
	    // We don't know anything about this task, be optimistic.
	    return 0L;
	}
	return val;
    }

    void setRemainingTaskTime( TaskInstanceIdentifier task, long val )
    {
	workerJobInfoTable.put( task, val );
    }
}
