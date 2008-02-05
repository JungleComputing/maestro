/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.util.LinkedList;

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

    WorkerInfo( ReceivePortIdentifier port, int workThreads, double benchmarkScore, long roundTripTime, long computeTime, long preCompletionInterval ){
        this.port = port;
        this.workThreads = workThreads;
        this.benchmarkScore = benchmarkScore;
        this.roundTripTime = roundTripTime;
        this.computeTime = computeTime;
        this.preCompletionInterval = preCompletionInterval;
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

    private long computeSubmitTime(long now)
    {
        long completionTime[] = new long[workThreads];
    
        // For /n/ worker threads, we want the /n/th latest
        // completion time. We compute this by maintaining
        // an array of the /n/ latest completion times, and
        // always replacing the lowest value by a higher one
        // if appropriate.
        // Note that we may arrive at a completion
        // time that is in the past. That's ok, it just means
        // that the result is still in transition. However, if
        // we are past the estimated arrival time, the completion
        // time is also adjusted.
        //
        // Also note that if there are less than /n/ active jobs,
        // we in essence get a very early completion time.
        // That's ok.
        synchronized( activeJobs ) {
    
            for( ActiveJob j: activeJobs ) {
        	int ix = indexOfLowest( completionTime );
    
        	long t = j.getCompletionTime( now );
        	if( completionTime[ix]<t ) {
        	    completionTime[ix] = t;
        	}
            }
        }
        int ix = indexOfLowest( completionTime );
        // The time we should submit the job.
        long submitTime = Math.max( now, completionTime[ix]-preCompletionInterval );
        return submitTime;
    }

    /** 
     * Given the current time, estimate how long this worker would take to complete
     * a job, taking the current load of the worker into account.
     * @param now The current time in ns.
     * @return The completion time of this worker in ns.
     */
    public long getCompletionTime( long now )
    {
	long submitTime = computeSubmitTime(now);
	return submitTime+roundTripTime;
    }

    /**
     * Returns the estimated time span, in ms, until this worker should
     * be send its next job.
     * @param now The current time in nanoseconds.
     * @return The interval in ns to the next useful job submission.
     */
    public long getBusyInterval( long now )
    {
	long submitTime = computeSubmitTime(now);
	return submitTime-now;
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
     * @param master The master this info belongs to.
     * @param result The job result message that tells about this job.
     * @param completionListener A completion listener to be notified.
     */
    public void registerJobResult(ReceivePortIdentifier master, JobResultMessage result, CompletionListener completionListener)
    {
        final long id = result.jobId;    // The identifier of the job, as handed out by us.

        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring reported result from job with unknown id " + id );
            return;
        }
        if( completionListener != null ) {
            completionListener.jobCompleted( e.job, result.result );
        }
        long now = System.nanoTime();
        long newRoundTripTime = now-e.startTime; // The time to send the job, compute, and report the result.
        long newComputeTime = result.getComputeTime();

        roundTripTime = (roundTripTime+newRoundTripTime)/2;
        computeTime = (computeTime+newComputeTime)/2;

        long sPreCompletionInterval;
        long sComputeTime;
        long sRoundTripTime;

        synchronized( activeJobs ){
            activeJobs.remove( e );
            // Adjust the precompletion interval to avoid both empty queues and full queues.
            // The /2 is a dampening factor.
            preCompletionInterval += (result.queueInterval-result.queueEmptyInterval)/2;
            roundTripTime = (roundTripTime+newRoundTripTime)/2;
            computeTime = (computeTime+newComputeTime)/2;
            sPreCompletionInterval = preCompletionInterval;
            sRoundTripTime = roundTripTime;
            sComputeTime = computeTime;
        }
        if( Settings.traceNodes ) {
            Globals.tracer.traceWorkerSettings( master,
                port,
                sRoundTripTime, sComputeTime, sPreCompletionInterval, result.queueInterval, result.queueEmptyInterval );
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
    public void registerJobStart( Job job, long id )
    {
        long startTime = System.nanoTime();
        long arrivalTime = startTime+preCompletionInterval;
        long completionTime = arrivalTime+computeTime;
        ActiveJob j = new ActiveJob( job, id, startTime, completionTime, arrivalTime );

        synchronized( activeJobs ) {
            activeJobs.add( j );
        }	
    }

    /** Given a job id, retract it from the administration.
     * For some reason we could not send this job to the worker.
     * @param id The identifier of the job.
     */
    public void retractJob( long id )
    {
        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring job retraction for unknown id " + id );
            return;
        }
        activeJobs.remove( e );
        System.out.println( "Master: retired job " + e );		
	
    }
}