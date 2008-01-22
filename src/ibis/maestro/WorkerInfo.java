/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

class WorkerInfo {
    private final ReceivePortIdentifier port;
    
    /** The time in seconds to do one iteration of a standard benchmark on this worker. */
    private final double benchmarkScore;
    private long roundTripTime;   // Estimated time to complete a job.
    private long computeTime;
    private long jobStartTime;    // The time of the most recent job start.

    WorkerInfo( ReceivePortIdentifier port, double benchmarkScore, long roundTripTime, long computeTime ){
        this.port = port;
        this.benchmarkScore = benchmarkScore;
        this.roundTripTime = roundTripTime;
        this.computeTime = computeTime;
    }

    boolean hasId(ReceivePortIdentifier id )
    {
        return port.equals( id );
    }

    ReceivePortIdentifier getPort() {
        return port;
    }
    
    /** The start time of the most recent job.
     * @param t The start time.
     */
    void registerJobStartTime( long t )
    {
	jobStartTime = t;
    }

    /** Register job completion time, and also handle the
     * reported computation time.
     * @param t The time at which the completion message was received.
     * @param newComputeTime The compute time as reported by the worker.
     */
    public void registerJobCompletionTime( long t, long newComputeTime )
    {
	long newRoundTripTime = jobStartTime-t; // The time to send the job, compute, and report the result.
	
	roundTripTime = (roundTripTime+newRoundTripTime)/2;
	computeTime = (computeTime+newComputeTime)/2;
    }

    /** 
     * Given the current time, estimate how long this worker would take to complete
     * a job.
     * @param now The current time in ns.
     * @return The completion time of this worker in ns.
     */
    public long getCompletionTime( long now )
    {
	// We predict the worker will be ready with its current job from us until...
        final long overhead = roundTripTime-computeTime;
        final long workerReadyTime = jobStartTime + roundTripTime - (overhead/2); 
	final long arrivalTime = now+(overhead/2);

        // Now return the estimated completion time. The job can be started
	// at the estimated arrival time or the time the worker is finished, whichever
	// comes first, plus the compute time, plus the time to send the result back.
        //
        // Note that in our estimates we totally ignore any other jobs the worker handles,
        // although they will show up in the overhead time.
        // A refinement would be to report the time a job spends in the queue.
	return Math.max( workerReadyTime, arrivalTime ) + computeTime + (overhead/2);
    }

    /**
     * Returns the estimated time span, in ms, until this worker should
     * be send its next job.
     * @param now The current time in nanoseconds.
     * @return The interval in ms to the next useful job submission.
     */
    public long getBusyInterval( long now )
    {
        // We predict the worker will be ready with its current job from us until...
        final long overhead = roundTripTime-computeTime;
        final long workerReadyTime = jobStartTime + roundTripTime - (overhead/2);
        
        return (workerReadyTime-now)/1000;
    }

    /** Returns the current estimated multiplier from benchmark score
     * to predicted job computation time in ns.
     * @return Estimated multiplier.
     */
    public double calculateMultiplier() {
        return computeTime/benchmarkScore;
    }

    /**
     * Given an ibis, return true iff this worker lives on that ibis.
     * @param ibis The ibis to compare to.
     * @return True iff this worker lives on the given ibis.
     */
    public boolean hasIbis(IbisIdentifier ibis) {
        return port.ibisIdentifier().equals(ibis);
    }
}