/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import java.util.LinkedList;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

class WorkerInfo {
    private final ReceivePortIdentifier port;
    
    /** The time in seconds to do one iteration of a standard benchmark on this worker. */
    private final double benchmarkScore;
    private final LinkedList<ActiveJob> activeJobs = new LinkedList<ActiveJob>();
    private long roundTripTime;   // Estimated time to complete a job.
    private long computeTime;
    private final long jobStartTime[];    // The time of the most recent job start of each thread.

    /** How many jobs do we have on this worker at the moment? */
    private int outstandingJobs;
    
    /** How many work threads does this worker have? */
    private final int workThreads;

    WorkerInfo( ReceivePortIdentifier port, int workThreads, double benchmarkScore, long roundTripTime, long computeTime ){
        this.port = port;
        this.workThreads = workThreads;
        this.benchmarkScore = benchmarkScore;
        this.roundTripTime = roundTripTime;
        this.computeTime = computeTime;
        this.jobStartTime = new long[workThreads];
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
	synchronized( this ) {
	    // Shift down all completion times.
	    // jobStartTime is a FIFO with the size of
	    // the number of work threads of this worker.
	    // Inherently, jobStartTime[0] will be the oldest
	    // start time, and other entries are increasingly
	    // recent. We blindly assume that a newly started
	    // job replaces a job on the work thread with the oldest
	    // start time.
	    int i = 0;
	    while( (i+1)<workThreads ) {
		jobStartTime[i] = jobStartTime[i+1];
		i++;
	    }
	    jobStartTime[i] = t;
	    outstandingJobs++;
	}
    }

    /** Register job completion time, and also handle the
     * reported computation time.
     * @param t The time at which the completion message was received.
     * @param newComputeTime The compute time as reported by the worker.
     */
    public void registerJobCompletionTime( long t, long newComputeTime )
    {
	long newRoundTripTime = jobStartTime[0]-t; // The time to send the job, compute, and report the result.
	
	synchronized( this ) {
	    roundTripTime = (roundTripTime+newRoundTripTime)/2;
	    computeTime = (computeTime+newComputeTime)/2;
	    outstandingJobs--;
	}
    }

    /** 
     * Given the current time, estimate how long this worker would take to complete
     * a job.
     * @param now The current time in ns.
     * @return The completion time of this worker in ns.
     */
    public long getCompletionTime( long now )
    {
	// FIXME: take the number of outstanding jobs into account.
	// We predict the worker will be ready with its current job from us until...
        final long overhead = roundTripTime-computeTime;
        final long workerReadyTime = jobStartTime[0] + roundTripTime - (overhead/2); 
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
	// FIXME: take the number of outstanding jobs into account.
	// FIXME: Share logic with getCompletionTime()
        // We predict the worker will be ready with its current job from us until...
        final long overhead = roundTripTime-computeTime;
        final long workerReadyTime = jobStartTime[0] + roundTripTime - (overhead/2);
        
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


    /**
     * Given a job identifier, returns the job queue entry with that id, or null.
     * @param id The job identifier to search for.
     * @return The JobQueueEntry of the job with this id, or null if there isn't one.
     */
    private ActiveJob searchQueueEntry( long id )
    {
	// Note that we blindly assume that there is only one entry with
	// the given id. Reasonable because we hand out the ids ourselves...
	for( ActiveJob e: activeJobs ) {
	    if( e.getId() == id ) {
		return e;
	    }
	}
	return null;
    }

    public void registerJobResult(JobResultMessage result, CompletionListener completionListener) {
        final long id = result.jobid;    // The identifier of the job, as handed out by us.

        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring reported result from job with unknown id " + id );
            return;
        }
        if( completionListener != null ) {
            completionListener.jobCompleted( e.getJob(), result.result );
        }
        long now = System.nanoTime();
        registerJobCompletionTime( now, result.getComputeTime() );
        synchronized( this ) {
            
            activeJobs.remove( e );
            activeJobs.notifyAll();
        }
        System.out.println( "Master: retired job " + e );		
    }

    /**
     * Returns true iff this worker is idle.
     * @return True iff this worker is idle.
     */
    public boolean isIdle() {
	return activeJobs.isEmpty();
    }

    /** Register the start of a new job.
     * 
     * @param job The job that was started.
     * @param id The id given to the job.
     */
    public void registerJobStart(Job job, long id)
    {
        long startTime = System.nanoTime();
        ActiveJob j = new ActiveJob( job, id, startTime );

        synchronized( activeJobs ) {
            activeJobs.add( j );
        }	
    }

    public void retractJob(long id) {
        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring job retraction for unknown id " + id );
            return;
        }
        activeJobs.remove( e );
        System.out.println( "Master: retired job " + e );		
	
    }
}