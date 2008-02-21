/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.util.ArrayList;
import java.util.Hashtable;

class WorkerInfo {
    /** The receive port of this worker. */
    final ReceivePortIdentifier port;
    
    /** How many work threads does this worker have? */
    final int workThreads;

    /** Which types of job does it allow? */
    final ArrayList<JobType> allowedTypes;

    /** The next moment we can submit a job to each thread of this worker. */
    private final long nextSubmissionTime[];

    /** The time in seconds to do one iteration of a standard benchmark on this worker. */
    private final double benchmarkScore;
    private final long benchmarkTransmissionTime;

    /** The active jobs of this worker. */
    private final ArrayList<ActiveJob> activeJobs = new ArrayList<ActiveJob>();

    private final Hashtable<JobType,WorkerJobInfo> workerJobInfoTable = new Hashtable<JobType, WorkerJobInfo>();
    
    /**
     * Returns a string representation of this worker info. (Overrides method in superclass.)
     * @return The worker info.
     */
    @Override
    public String toString()
    {
        return "Worker " + port;
    }

    WorkerInfo( ReceivePortIdentifier port, int workThreads, ArrayList<JobType> allowedTypes, double benchmarkScore, long benchmarkTransmissionTime )
    {
        this.port = port;
        this.workThreads = workThreads;
        this.allowedTypes = allowedTypes;
        this.benchmarkScore = benchmarkScore;
        this.benchmarkTransmissionTime = benchmarkTransmissionTime;
        this.nextSubmissionTime = new long[workThreads];
    }

    boolean hasId( ReceivePortIdentifier id )
    {
        return port.equals( id );
    }

    ReceivePortIdentifier getPort()
    {
        return port;
    }

    /** Returns the current estimated multiplier from benchmark score
     * to predicted job computation time in ns.
     * @param type The job type to calculate the multiplier for.
     * @return Estimated multiplier.
     */
    public double calculateMultiplier( JobType type )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( type );
	if( workerJobInfo == null ) {
	    return -1;
	}
        return workerJobInfo.getComputeTime()/benchmarkScore;
    }

    /**
     * Given an ibis, return true iff this worker lives on that ibis.
     * @param ibis The ibis to compare to.
     * @return True iff this worker lives on the given ibis.
     */
    public boolean hasIbis( IbisIdentifier ibis )
    {
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
    public void registerJobResult( ReceivePortIdentifier master, JobResultMessage result, CompletionListener completionListener )
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

        synchronized( activeJobs ){
            activeJobs.remove( e );
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "Master: retired job " + e );		
        }
    }

    /**
     * Register a job result for an outstanding job.
     * @param master The master this info belongs to.
     * @param result The job result message that tells about this job.
     */
    public void registerWorkerStatus( ReceivePortIdentifier master, WorkerStatusMessage result )
    {
        final long id = result.jobId;    // The identifier of the job, as handed out by us.

        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring reported result from job with unknown id " + id );
            return;
        }
        long now = System.nanoTime();
        long newRoundTripTime = now-e.startTime; // The time to send the job, compute, and report the result.

        synchronized( activeJobs ){
            activeJobs.remove( e );
        }
        long step = e.workerJobInfo.update( this, master, result, newRoundTripTime );
        for( int i=0; i<nextSubmissionTime.length; i++ ) {
            nextSubmissionTime[i] += step;
        }
        if( Settings.traceMasterProgress ){
            System.out.println( "Master: retired job " + e + "; added " + Service.formatNanoseconds( step ) + " to nextSubmissionTime" );
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
     * @param jobInfo Information about the job.
     * @param id The id given to the job.
     * @param startTime The time this job was started.
     */
    public void registerJobStart( Job job, JobInfo jobInfo, long id, long startTime )
    {
        WorkerJobInfo workerJobInfo = workerJobInfoTable.get( job.getType() );
        if( workerJobInfo == null ) {
            System.err.println( "No worker job info for job type " + job.getType() );
            return;
        }
        setNextSubmissionTime( startTime+workerJobInfo.getSubmissionInterval() );
        workerJobInfo.setLastSubmission( startTime );
        ActiveJob j = new ActiveJob( job, id, startTime, workerJobInfo, jobInfo );

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

    /**
     * Returns true iff this worker has information about the given job type.
     * @param type The job type we want to know about.
     * @return True iff this worker has information about the given type.
     */
    public boolean knowsJobType( JobType type )
    {
	return workerJobInfoTable.containsKey( type );
    }

    /**
     * @param type The job type to register for.
     * @param multiplier The estated multiplier for this kind
     *        of job relative to the benchmark.
     */
    private void registerJobType( JobType type, double multiplier )
    {
	long computeTime;
	if( multiplier<0 ) {
	    computeTime = benchmarkTransmissionTime;
	}
	else {
	    computeTime = (long) (multiplier*benchmarkScore);
	}
	// We start with a very pessimistic job submission interval to
	// avoid committing too much work to this worker before it has
	// given some feedback.
	WorkerJobInfo info = new WorkerJobInfo( computeTime+benchmarkTransmissionTime, computeTime, 3*(computeTime+benchmarkTransmissionTime) );
	workerJobInfoTable.put( type, info );
    }

    /**
     * Returns the index in the entries of nextSubmission time with the
     * lowest value.
     */
    private int indexOfEarliestNextSubmissionTime()
    {
	int ix=0;
	long min = nextSubmissionTime[0];
	
	for( int i=1; i<nextSubmissionTime.length; i++ ) {
	    if( nextSubmissionTime[i]<min ) {
		min = nextSubmissionTime[i];
		ix = i;
	    }
	}
	return ix;
    }

    /**
     * Returns the next time a job should be submitted to this worker.
     * @return The next submission time.
     */
    public long getNextSubmissionTime()
    {
	int ix = indexOfEarliestNextSubmissionTime();
	return nextSubmissionTime[ix];
    }
    
    void setNextSubmissionTime( long val )
    {
	int ix = indexOfEarliestNextSubmissionTime();
        nextSubmissionTime[ix] = val;
    }

    /** Given a job type, return the submission interval for this job type, or
     * -1 if the job type is not allowed on this worker.
     * @param jobType The job type for which we want to know the submission interval.
     * @param allWorkers The list of all workers. Used to estimate a job interval if not yet known.
     * @return The submission interval, or -1 if this type of job is not allowed.
     */
    public long getSubmissionInterval( JobType jobType, WorkerList allWorkers )
    {
        // FIXME: enable this again.
        //if( !allowedTypes.contains( jobType ) ) {
        //    return -1;
        //}
        WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
        if( workerJobInfo == null ) {
            double multiplier = allWorkers.estimateMultiplier( jobType );
            registerJobType( jobType, multiplier );
            workerJobInfo = workerJobInfoTable.get( jobType );           
        }
        return workerJobInfo.getSubmissionInterval();
    }
}
