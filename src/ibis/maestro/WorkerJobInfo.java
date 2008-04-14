/**
 * Information about a particular job type on a particular worker.
 */
package ibis.maestro;


class WorkerJobInfo {
    /** Estimated minimal time in ns to complete a job, including communication. */
    private long minimalRoundTripInterval;

    /** Estimated maximal time in ns to complete a job, including communication. */
    private long maximalRoundTripInterval;

    /** How many instances of this job does this worker currently have? */
    private int outstandingJobs = 0;

    private int executedJobs = 0;

    private int maximalEverAllowance = 1;

    /** How many instance of this job should this worker maximally have? */
    private int maximalAllowance = 1;

    /** We have reached the maximal allowance of this worker, so we are
     * willing to consider an increased allowance.
     */
    private boolean mayIncreaseAllowance = false;

    /**
     * Returns the round-trip interval for this worker and this job type, or
     * a very large number if currently there are no job slots.
     * @return The round-trip interval.
     */
    long getMinimalRoundTripInterval()
    {
        if( maximalAllowance == 0 ){
            System.err.println( "Zero allowance" );
        }
        if( outstandingJobs>=maximalAllowance ){
            return Long.MAX_VALUE;
        }
	return minimalRoundTripInterval;
    }
    
    boolean canNowExecute()
    {
	return outstandingJobs<maximalAllowance;
    }

    /**
     * Returns the maximal round-trip interval for this worker and this job type, or
     * a very pessimistic estimate if currently there are no job slots.
     * @param The number of reservations we already have.
     * @return The round-trip interval.
     */
    long estimateRoundTripInterval( int reservations )
    {
        if( maximalAllowance == 0 ){
            System.err.println( "Zero allowance" );
        }
        if( outstandingJobs>=maximalAllowance ){
            // This worker is fully booked for the moment, but estimate anyway how long
            // it would take by using a pessimistic round-trip time.

            // We have to be paranoid in this calculation, since maximalRoundTripInterval can be
            // Long.MAX_VALUE, so if we're not careful we can overflow a long.
            long roundTripPerJob = maximalRoundTripInterval/maximalAllowance;
            
            if( minimalRoundTripInterval/(1+reservations)+roundTripPerJob >= Long.MAX_VALUE/(2+2*reservations) ){
                // We're dangerously close to an overflow, so just give a rough approximation.
                return Long.MAX_VALUE;
            }
            return minimalRoundTripInterval+((1+reservations)*maximalRoundTripInterval)/maximalAllowance;
        }
	return minimalRoundTripInterval;
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     * @param minimalRoundTripInterval The initial estimate for the send-to-receive
     *    time for this  job for this particular worker.
     */
    WorkerJobInfo( long minimalRoundTripInterval, long maximalRoundTripInterval )
    {
	this.minimalRoundTripInterval = minimalRoundTripInterval;
	this.maximalRoundTripInterval = maximalRoundTripInterval;
    }

    /**
     * Registers the completion of a job.
     * @param theRoundTripInterval The round-trip interval of this job.
     */
    void registerJobCompleted( long theRoundTripInterval )
    {
	executedJobs++;
	minimalRoundTripInterval = (minimalRoundTripInterval+theRoundTripInterval)/2;
	maximalRoundTripInterval = (maximalRoundTripInterval+theRoundTripInterval)/2;
	outstandingJobs--;
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "New roundtrip time " + Service.formatNanoseconds( minimalRoundTripInterval ) + "..." + Service.formatNanoseconds( maximalRoundTripInterval ) );
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
	return "executed " + executedJobs + " jobs; maximal ever allowance: " + maximalEverAllowance;
    }
}
