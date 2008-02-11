/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.util.Vector;

class WorkerInfo {
    /** The receive port of this worker. */
    private final ReceivePortIdentifier port;
    
    /** How many work threads does this worker have? */
    private final int workThreads;

    /** The time in seconds to do one iteration of a standard benchmark on this worker. */
    private final double benchmarkScore;

    /** The active jobs of this worker. */
    private final Vector<ActiveJob> activeJobs = new Vector<ActiveJob>();

    /** Estimated time to complete a job, including communication. */
    private long roundTripTime;
    
    /** Estimated time to complete a job, excluding communication. */
    private long computeTime;
    
    /** Estimated interval, before one job completes, that we should submit a new job. In ns. */
    private long preCompletionInterval;

    WorkerInfo( ReceivePortIdentifier port, int workThreads, double benchmarkScore, long roundTripTime, long computeTime, long preCompletionInterval )
    {
        this.port = port;
        this.workThreads = workThreads;
        this.benchmarkScore = benchmarkScore;
        this.roundTripTime = roundTripTime;
        this.computeTime = computeTime;
        this.preCompletionInterval = preCompletionInterval;
        if( Settings.tracePrecompletionInterval ) {
            System.out.println( "Initial PCI is " + preCompletionInterval );
        }
    }

    boolean hasId( ReceivePortIdentifier id )
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
     * Given a worker selector describing the best worker to submit a 
     * job to thus far, update it with information of this worker.
     * @param now The current time.
     * @param sel The worker selector.
     * @param sendSize The number of bytes in a job submission message.
     * @param receiveSize The number of bytes in a job result message.
     */
    public void setBestWorker( long now, WorkerSelector sel, long sendSize, long receiveSize )
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
        	long t = j.getCompletionTime( completionTime[ix], now, roundTripTime, computeTime );

                if( completionTime[ix]<t ) {
        	    completionTime[ix] = t;
        	}
            }
        }
        int ix = indexOfLowest( completionTime );
        // The time we should submit the job.
        // We aim to keep each job about half its runtime in the queue; a reasonable compromise
        // between buffering against idle time and not committing too much to a worker.
        long ourCompletionTime = completionTime[ix];
        long l = estimateResultTransmissionTime( sendSize, receiveSize );
	long ourResultTime = ourCompletionTime+l;
	long idealSubmissionTime = ourCompletionTime-preCompletionInterval;
	long submitTime = idealSubmissionTime;
        if( now>idealSubmissionTime ) {
            if( Settings.traceFastestWorker ) {
        	System.err.println( "You asked " + Service.formatNanoseconds(now-idealSubmissionTime)+ " too late for ideal submit time" );
            }
            long delta = now-idealSubmissionTime;
            submitTime = now;
            // Updating our estimate with this knowledge.§
            ourCompletionTime += delta;
            ourResultTime += delta;
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "setBestWorker(): submitTime-now=" + Service.formatNanoseconds(submitTime-now) + " ourCompletionTime-now=" + Service.formatNanoseconds(ourCompletionTime-now) + " ourResultTime-now=" + Service.formatNanoseconds(ourResultTime-now) );
        }
        if( sel.resultTime>ourResultTime ) {
            if( Settings.traceMasterProgress ) {
        	System.out.println( "Worker " + this + " is the best pick thus far" );
            }
            sel.bestWorker = this;
            sel.resultTime = ourResultTime;
            sel.startTime = submitTime;
        }
    }

    private long estimateResultTransmissionTime( long sendSize, long receiveSize ) {
	long transmissionTime = roundTripTime-computeTime;
	long res;

	if( sendSize<=0 || receiveSize<=0 ) {
	    res = transmissionTime/2;
	}
	else {
	    double fraction = ((double) receiveSize)/((double) sendSize+receiveSize);
	    res = (long) (transmissionTime*fraction);
	}
	return res;
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
    public void registerJobResult( ReceivePortIdentifier master, JobResultMessage result, CompletionListener completionListener)
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

        long sPreCompletionInterval;
        long sComputeTime;
        long sRoundTripTime;

        synchronized( activeJobs ){
            activeJobs.remove( e );
            // Adjust the precompletion interval to avoid both empty queues and full queues.
            // The /2 is a dampening factor.
            roundTripTime = (roundTripTime+newRoundTripTime)/2;
            computeTime = (computeTime+newComputeTime)/2;
            // We're aiming for a queue interval of half the compute time.
            long idealinterval = (workThreads*computeTime/2);
            long step = (result.queueEmptyInterval+idealinterval-result.queueInterval)/2;
            preCompletionInterval += step;
            if( Settings.tracePrecompletionInterval ) {
                System.out.println( "old PCI=" + Service.formatNanoseconds(preCompletionInterval-step) + " queueEmptyInterval=" + Service.formatNanoseconds(result.queueEmptyInterval) + " queueInterval=" + Service.formatNanoseconds(result.queueInterval) + " ideal queueInterval=" + Service.formatNanoseconds(idealinterval) + " new PCI=" + Service.formatNanoseconds(preCompletionInterval) );
            }
            sPreCompletionInterval = preCompletionInterval;
            sRoundTripTime = roundTripTime;
            sComputeTime = computeTime;
        }
        if( Settings.writeTrace ) {
            Globals.tracer.traceWorkerSettings( master,
                port,
                sRoundTripTime, sComputeTime, sPreCompletionInterval, result.queueInterval, result.queueEmptyInterval );
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "Master: retired job " + e );		
        }
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
        ActiveJob j = new ActiveJob( job, id, startTime );

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
