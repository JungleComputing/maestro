/**
 * Information about a particular job type on a particular worker.
 */
package ibis.maestro;


class WorkerJobInfo {
    private final TimeEstimate roundTripEstimate = new TimeEstimate();

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
            System.err.println( "Internal error: zero allowance" );
        }
        if( outstandingJobs>=maximalAllowance ){
            // This worker is fully booked for the moment, but estimate anyway how long
            // it would take by using a pessimistic round-trip time.

            final long val = roundTripEstimate.getEstimate( 1+(outstandingJobs-maximalAllowance)+reservations  );
            return val;
        }
	return roundTripEstimate.getEstimate();
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
     */
    void registerJobCompleted( long theRoundTripInterval )
    {
	executedJobs++;
	roundTripEstimate.addSample(theRoundTripInterval);
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
}
