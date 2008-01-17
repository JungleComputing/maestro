/**
 * 
 */
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

class WorkerInfo {
    private final ReceivePortIdentifier port;
    private long roundTripTime;   // Estimated time to complete a job.
    private long jobStartTime;    // The time of the most recent job start.
    private long overhead;        // Estimated time used to send and receive a job.
    private long computeTime;

    WorkerInfo( ReceivePortIdentifier port ){
        this.port = port;
    }

    boolean hasId(ReceivePortIdentifier id) {
        return port.equals( id );
    }

    ReceivePortIdentifier getPort() {
        return port;
    }
    
    /** The start time of the most recent job.
     * @param t The start time.
     */
    public void registerJobStartTime( long t ) {
	jobStartTime = t;
    }

    /** Register job completion time, and also handle the
     * reported computation time.
     * @param t The time at which the completion message was received.
     * @param newComputeTime The compute time as reported by the worker.
     */
    public void registerJobCompletionTime( long t, long newComputeTime ) {
	long newRoundTripTime = jobStartTime-t; // The time to send the job, compute, and report the result.
	long newOverhead = newRoundTripTime-newComputeTime;
	
	roundTripTime = (roundTripTime+newRoundTripTime)/2;
	overhead = (overhead+newOverhead)/2;
	computeTime = (computeTime+newComputeTime)/2;
    }

    /** 
     * Given the current time, estimate how long this worker would take to complete
     * a job.
     * @param now The current time in ns.
     * @return The completion time of this worker in ns.
     */
    public long getCompletionTime(long now) {
	
	// We predict the worker will be busy until this moment...
	final long workerReadyTime = jobStartTime + (overhead/2) + computeTime; 
	final long arrivalTime = now+(overhead/2);
	
	// Now return the estimated completion time. The job can be started
	// at the estimated arrival time or the time the worker is finished, whichever
	// comes first, plus the compute time, plus the time to send the result back.
	return Math.max( workerReadyTime, arrivalTime ) + computeTime + (overhead/2);
    }
}