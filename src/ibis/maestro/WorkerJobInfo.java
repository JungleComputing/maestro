/**
 * Information about a particular job type on a particular worker.
 */
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

class WorkerJobInfo {
    /** Estimated time in ns to complete a job, including communication. */
    private long roundTripTime;

    /** Estimated time in ns to complete a job, excluding communication. */
    private long computeTime;

    private long latestSubmission = 0;

    /** Ideal time in ns between job submissions. */
    private long submissionInterval;

    private void updateRoundTripTime( long newRoundTripTime )
    {
	roundTripTime = (roundTripTime+newRoundTripTime)/2;
    }

    long getRoundTripTime() {
	return roundTripTime;
    }

    /**
     * Constructs a new information class for a particular job type
     * for a particular worker.
     * @param roundTripTime The estimated send-to-receive time for this job for this particular worker.
     * @param computeTime The estimated compute time on this job on this particular worker.
     * @param submissionInterval
     */
    public WorkerJobInfo(long roundTripTime, long computeTime,
	    long submissionInterval) {
	this.roundTripTime = roundTripTime;
	this.computeTime = computeTime;
	this.submissionInterval = submissionInterval;
    }

    /** Register a newly learned compute time.
     * 
     * @param newComputeTime The newly learned compute time.
     */
    private void updateComputeTime( long newComputeTime )
    {
	computeTime = (computeTime+newComputeTime)/2;
    }

    private void updateSubmissionInterval( int workThreads, WorkerStatusMessage result )
    {
	// We're aiming for a queue interval of half the compute time.
	long idealinterval = workThreads*computeTime/2;
	long step = (result.queueEmptyInterval+idealinterval-result.queueInterval)/2;
	submissionInterval += step;
	if( Settings.tracePrecompletionInterval ) {
	    System.out.println( "old submission interval=" + Service.formatNanoseconds(submissionInterval-step) + " queueEmptyInterval=" + Service.formatNanoseconds(result.queueEmptyInterval) + " queueInterval=" + Service.formatNanoseconds(result.queueInterval) + " ideal queueInterval=" + Service.formatNanoseconds(idealinterval) + " new submissionInterval=" + Service.formatNanoseconds(submissionInterval) );
	}
    }

    void update(WorkerInfo workerInfo, ReceivePortIdentifier master, WorkerStatusMessage result, long newRoundTripTime) {
	synchronized( this ) {
	    updateRoundTripTime( newRoundTripTime );
	    updateComputeTime( result.getComputeTime() );
	    updateSubmissionInterval( workerInfo.workThreads, result );
	}
	if( Settings.writeTrace ) {
	    synchronized( this ) {
		Globals.tracer.traceWorkerSettings( master,
			workerInfo.port,
			roundTripTime, computeTime, submissionInterval, result.queueInterval, result.queueEmptyInterval );
	    }
	}
    }

    /**
     * Returns the estimated compute time for this job on this worker.
     * @return The estimated compute time.
     */
    public long getComputeTime() {
	return computeTime;
    }

    /**
     * Returns the current submission interval for this job and worker.
     * @return The submission interval.
     */
    public long getSubmissionInterval() {
	return submissionInterval;
    }

    /**
     * Returns the most recent time ns that a job was submitted to this worker.
     * @return The most recent submission time.
     */
    public long getLatestSubmission()
    {
	return latestSubmission;
    }

    /**
     * Set the last submission time to the given value.
     * @param val The new latest submission time.
     */
    public void setLastSubmission( long val )
    {
	latestSubmission = val;
    }
}