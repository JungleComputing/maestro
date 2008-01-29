/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import java.util.LinkedList;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

class WorkerInfo {
    /** The receive port of this worker. */
    private final ReceivePortIdentifier port;
    
    /** How many work threads does this worker have? */
    private final int workThreads;
    
    /** The time in seconds to do one iteration of a standard benchmark on this worker. */
    private final double benchmarkScore;
    
    /** The active jobs of this worker. */
    private final LinkedList<ActiveJob> activeJobs = new LinkedList<ActiveJob>();

    /** Estimated time to complete a job, including communication. */
    private long roundTripTime;
    
    /** Estimated time to complete a job, excluding communication. */
    private long computeTime;
    
    /** Estimated interval, before one job completes, that we should submit a new job. In ns. */
    private long preCompletionInterval;

    WorkerInfo( ReceivePortIdentifier port, int workThreads, double benchmarkScore, long roundTripTime, long computeTime ){
        this.port = port;
        this.workThreads = workThreads;
        this.benchmarkScore = benchmarkScore;
        this.roundTripTime = roundTripTime;
        this.computeTime = computeTime;
    }

    boolean hasId(ReceivePortIdentifier id )
    {
        return port.equals( id );
    }

    ReceivePortIdentifier getPort()
    {
        return port;
    }

    /** Given an array, return the index of the lowest value in
     * the index. We blindly assume that the array is at least 1
     * element long.
     * @param val The array of values.
     * @return The index of the lowest values.
     */
    private static int indexOfLowest( long val[] )
    {
        int ix = 0;
        
        for( int i=1; i<val.length; i++ ){
            if( val[i]<val[ix] ){
                ix = i;
            }
        }
        return ix;
    }

    /**
     * Given the current time, estimate the time interval before a new job should be submitted to this
     * worker. We pass the current time to this method instead of looking at the clock ourselves to
     * make the computations more predictable.
     * @param now The current time.
     * @return The estimated time in ns before a new job should be submitted to this worker.
     */
    public long estimateSubmissionTime( long now )
    {
        // Since we submit jobs before the previous one was completed, we may have more
        // outstanding jobs on a worker than it has workers. To make sure we still can
        // estimate the 
        long completionInterval[] = new long[workThreads];
        for( int ix=0; ix<workThreads; ix++ ){
            completionInterval[ix] = Long.MIN_VALUE;
        }
        long overhead = roundTripTime-computeTime;

        for( int i=0; i<activeJobs.size(); i++ ){
            ActiveJob j = activeJobs.get( i );
            
            // Overwrite the current earliest completion time with the completion
            // time of this job.
            int ix = indexOfLowest( completionInterval );
            long startTime = j.startTime;
            long completionTime = Math.max( now, startTime + roundTripTime )-overhead/2;
            completionInterval[ix] = completionTime;
        }
        if( activeJobs.size()<workThreads ){
            return 0L;
        }
        // Now return the smallest completion interval in the current list.
        int ix = indexOfLowest( completionInterval );
        return completionInterval[ix];
    }
    /** 
     * Given the current time, estimate how long this worker would take to complete
     * a job.
     * @param now The current time in ns.
     * @return The completion time of this worker in ns.
     */
    public long getCompletionTime( long now )
    {
        // FIXME: implement this correctly.
        return now + computeTime;
    }

    /**
     * Returns the estimated time span, in ms, until this worker should
     * be send its next job.
     * @param now The current time in nanoseconds.
     * @return The interval in ms to the next useful job submission.
     */
    public long getBusyInterval( long now )
    {
        // FIXME: implement this correctly.
        return computeTime;
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
	    if( e.id == id ) {
		return e;
	    }
	}
	return null;
    }

    /**
     * Register a job result for an outstanding job.
     * @param result The job result message that tells about this job.
     * @param completionListener A completion listener to be notified.
     */
    public void registerJobResult(JobResultMessage result, CompletionListener completionListener)
    {
        final long id = result.jobid;    // The identifier of the job, as handed out by us.

        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring reported result from job with unknown id " + id );
            return;
        }
        if( completionListener != null ) {
            completionListener.jobCompleted( e.job, result.result );
        }
        long now = System.nanoTime();
        long newRoundTripTime = e.startTime-now; // The time to send the job, compute, and report the result.
        long newComputeTime = result.getComputeTime();

        roundTripTime = (roundTripTime+newRoundTripTime)/2;
        computeTime = (computeTime+newComputeTime)/2;

        synchronized( activeJobs ){
            activeJobs.remove( e );
            // Adjust the precompletion interval to avoid both empty queues and full queues.
            // The /2 is a dampening factor.
            preCompletionInterval += (result.queueEmptyInterval-result.queueEmptyInterval)/2;
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
     * @param completionTime The estimated completion time of this job.
     */
    public void registerJobStart( Job job, long id, long completionTime )
    {
        long startTime = System.nanoTime();
        ActiveJob j = new ActiveJob( job, id, startTime, completionTime );

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