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
    private int maximalOutstandingJobs;

    private void updateRoundTripTime( long newRoundTripTime )
    {
	roundTripInterval = (roundTripInterval+newRoundTripTime)/2;
    }

    long getRoundTripTime() {
	return roundTripInterval;
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     * @param roundTripInterval The estimated send-to-receive time for this job for this particular worker.
     */
    public WorkerJobInfo( long roundTripInterval ) {
	this.roundTripInterval = roundTripInterval;
    }

    void registerJobCompleted(WorkerInfo workerInfo, ReceivePortIdentifier master, WorkerStatusMessage result, long newRoundTripInterval)
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
}