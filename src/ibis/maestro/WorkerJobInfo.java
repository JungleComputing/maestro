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

    private long updateSubmissionInterval( int workThreads, WorkerStatusMessage result )
    {
        long oldSubmissionInterval = submissionInterval;
	// We're aiming for a queue interval of half the compute time.
	long idealQueueInterval = workThreads*computeTime/2;
	long step = (result.queueInterval-result.queueEmptyInterval)/2;
	submissionInterval += step;
        step += (result.queueInterval-idealQueueInterval)/2;
	if( Settings.traceSubmissionInterval ) {
	    System.out.println(
                "old submission interval=" + Service.formatNanoseconds(oldSubmissionInterval) +
                " queueEmptyInterval=" + Service.formatNanoseconds(result.queueEmptyInterval) +
                " queueInterval=" + Service.formatNanoseconds(result.queueInterval) +
                " ideal queueInterval=" + Service.formatNanoseconds(idealQueueInterval) +
                " new submissionInterval=" + Service.formatNanoseconds(submissionInterval)
            );
	}
	return step;
    }

    long update(WorkerInfo workerInfo, ReceivePortIdentifier master, WorkerStatusMessage result, long newRoundTripTime)
    {
        long step;

        synchronized( this ) {
	    updateRoundTripTime( newRoundTripTime );
	    updateComputeTime( result.getComputeTime() );
	    step = updateSubmissionInterval( workerInfo.workThreads, result );
	}
	if( Settings.traceWorkerProgress ) {
	    System.out.println( "New submission interval " + Service.formatNanoseconds( submissionInterval ) + " compute time " + Service.formatNanoseconds( computeTime ) + " roundtrip time " + Service.formatNanoseconds( roundTripTime ) );
	}
	if( Settings.writeTrace ) {
	    synchronized( this ) {
		Globals.tracer.traceWorkerSettings( master,
			workerInfo.port,
			roundTripTime, computeTime, submissionInterval, result.queueInterval, result.queueEmptyInterval );
	    }
	}
	return step;
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