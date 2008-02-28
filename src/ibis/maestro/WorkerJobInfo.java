/**
 * Information about a particular job type on a particular worker.
 */
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

class WorkerJobInfo {
    /** Estimated time in ns to complete a job, including communication. */
    private long roundTripInterval;
    
    /** How many instances of this job does this worker currently have? */
    private int outstandingJobs = 0;

    /** How many instance of this job should this worker maximally have? */
    private int maximalOutstandingJobs = 1;

    private void updateRoundTripTime( long newRoundTripTime )
    {
	roundTripInterval = (roundTripInterval+newRoundTripTime)/2;
    }

    /**
     * Returns the round-trip interval for this worker and this job type, or
     * a very large number if currently there are no job slots.
     * @return The tround-trip interval.
     */
    synchronized long getRoundTripInterval() {
        if( outstandingJobs>=maximalOutstandingJobs ){
            return Long.MAX_VALUE;
        }
	return roundTripInterval;
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     * @param roundTripInterval The estimated send-to-receive time for this job for this particular worker.
     */
    public WorkerJobInfo( long roundTripInterval )
    {
	this.roundTripInterval = roundTripInterval;
    }

    void registerJobCompleted(WorkerInfo workerInfo, ReceivePortIdentifier master, long newRoundTripInterval )
    {
        synchronized( this ) {
	    updateRoundTripTime( newRoundTripInterval );
	    outstandingJobs--;
	}
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "New roundtrip time " + Service.formatNanoseconds( newRoundTripInterval )  );
	}
	if( Settings.writeTrace ) {
	    synchronized( this ) {
		Globals.tracer.traceWorkerSettings( master,
			workerInfo.port,
			roundTripInterval );
	    }
	}
    }

    /** Register a new outstanding job. */
    public synchronized void incrementOutstandingJobs()
    {
	outstandingJobs++;
    }

    /**
     * Increment the maximal number of outstanding jobs for this worker and this type of work.
     */
    public synchronized void incrementAllowance()
    {
        maximalOutstandingJobs++;
    }

    /** Since there are no jobs in our work queue, reduce the maximal number of
     * outstanding jobs.
     */
    synchronized void reduceAllowance()
    {
	int n = maximalOutstandingJobs-outstandingJobs;
	
	maximalOutstandingJobs -= n/2;
    }
}
