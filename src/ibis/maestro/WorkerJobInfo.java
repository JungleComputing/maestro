/**
 * Information about a particular job type on a particular worker.
 */
package ibis.maestro;


class WorkerJobInfo {
    /** Estimated time in ns to complete a job, including communication. */
    private long roundTripInterval;

    /** How many instances of this job does this worker currently have? */
    private int outstandingJobs = 0;

    private int executedJobs = 0;
    
    private int maximalEverAllowance = 1;

    /** How many instance of this job should this worker maximally have? */
    private int maximalOutstandingJobs = 1;

    private void updateRoundTripTime( long newRoundTripTime )
    {
	roundTripInterval = (roundTripInterval+newRoundTripTime)/2;
    }

    /**
     * Returns the round-trip interval for this worker and this job type, or
     * a very large number if currently there are no job slots.
     * @return The round-trip interval.
     */
    long getRoundTripInterval()
    {
        if( outstandingJobs>=maximalOutstandingJobs ){
            return Long.MAX_VALUE;
        }
	return roundTripInterval;
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     * @param roundTripInterval The initial estimate for the send-to-receive
     *    time for this  job for this particular worker.
     */
    WorkerJobInfo( long roundTripInterval )
    {
	this.roundTripInterval = roundTripInterval;
    }

    /**
     * Registers the completion of a job.
     * @param theRoundTripInterval The round-trip interval of this job.
     */
    void registerJobCompleted( long theRoundTripInterval )
    {
	executedJobs++;
	updateRoundTripTime( theRoundTripInterval );
	outstandingJobs--;
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "New roundtrip time " + Service.formatNanoseconds( roundTripInterval )  );
	}
    }

    /** Register a new outstanding job. */
    public void incrementOutstandingJobs()
    {
	outstandingJobs++;
    }

    /**
     * Increment the maximal number of outstanding jobs for this worker and
     *  this type of work.
     */
    public void incrementAllowance()
    {
        maximalOutstandingJobs++;
        if( maximalEverAllowance<maximalOutstandingJobs ) {
            maximalEverAllowance = maximalOutstandingJobs;
        }
    }

    /** Since there are no jobs in our work queue, reduce the maximal number of
     * outstanding jobs.
     */
    void reduceAllowance()
    {
	int n = maximalOutstandingJobs-outstandingJobs;
	
	if( n<0 ) {
	    return;
	}
	maximalOutstandingJobs = Math.max( 1, maximalOutstandingJobs-n/2);
    }

    /**
     * Try to reduce the allowance of this worker and job.
     * This may not succeed if the new allowance would
     * be too low.
     * @return True iff we reduced the allowance.
     */
    boolean decrementAllowance()
    {
	if( maximalOutstandingJobs<=1 ) {
	    return false;
	}
	maximalOutstandingJobs--;
	return true;
    }
    
    String buildStatisticsString()
    {
	return "executed " + executedJobs + " jobs; maximal ever allowance: " + maximalEverAllowance;
    }
}
