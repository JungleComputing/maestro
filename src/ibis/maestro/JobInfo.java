/**
 * 
 */
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

class JobInfo {
    /** Estimated time in ns to complete a job, including communication. */
    private long roundTripTime;

    /** Estimated time in ns to complete a job, excluding communication. */
    private long computeTime;

    /** Estimated interval in ns, before one job completes, that we should submit a new job. */
    private long preCompletionInterval;

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
     * @param preCompletionInterval
     */
    public JobInfo(long roundTripTime, long computeTime,
	    long preCompletionInterval) {
	this.roundTripTime = roundTripTime;
	this.computeTime = computeTime;
	this.preCompletionInterval = preCompletionInterval;
    }

    /** Register a newly learned compute time.
     * 
     * @param newComputeTime The newly learned compute time.
     */
    private void updateComputeTime( long newComputeTime )
    {
	computeTime = (computeTime+newComputeTime)/2;
    }

    private void updatePrecompletionInterval( int workThreads, JobResultMessage result )
    {
	// We're aiming for a queue interval of half the compute time.
	long idealinterval = workThreads*computeTime/2;
	long step = (result.queueEmptyInterval+idealinterval-result.queueInterval)/2;
	preCompletionInterval += step;
	if( Settings.tracePrecompletionInterval ) {
	    System.out.println( "old PCI=" + Service.formatNanoseconds(preCompletionInterval-step) + " queueEmptyInterval=" + Service.formatNanoseconds(result.queueEmptyInterval) + " queueInterval=" + Service.formatNanoseconds(result.queueInterval) + " ideal queueInterval=" + Service.formatNanoseconds(idealinterval) + " new PCI=" + Service.formatNanoseconds(preCompletionInterval) );
	}
    }

    void update(WorkerInfo workerInfo, ReceivePortIdentifier master, JobResultMessage result, long newRoundTripTime) {
	synchronized( this ) {
	    updateRoundTripTime( newRoundTripTime );
	    updateComputeTime( result.getComputeTime() );
	    updatePrecompletionInterval( workerInfo.workThreads, result );
	}
	if( Settings.writeTrace ) {
	    synchronized( this ) {
		Globals.tracer.traceWorkerSettings( master,
			workerInfo.port,
			roundTripTime, computeTime, preCompletionInterval, result.queueInterval, result.queueEmptyInterval );
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
     * Returns the current precompletion interval for this job and worker.
     * @return The precompletion interval.
     */
    public long getPreCompletionInterval() {
	return preCompletionInterval;
    }

    /**
     * Returns the estimated transmission time of this job.
     * @return
     */
    public long getTransmissionTime() {
	return roundTripTime-computeTime;
    }
}