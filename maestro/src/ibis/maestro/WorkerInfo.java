package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

/**
 * Information about a worker in the list of a master.
 */
final class WorkerInfo {
    /** Our identifier with this worker. */
    final MasterIdentifier identifierWithWorker;

    /** The active jobs of this worker. */
    private final ArrayList<ActiveJob> activeJobs = new ArrayList<ActiveJob>();

    private final Hashtable<JobType,WorkerJobInfo> workerJobInfoTable = new Hashtable<JobType, WorkerJobInfo>();

    /** Our local identifier of this worker. */
    final Master.WorkerIdentifier identifier;

    /** The receive port of the worker. */
    final ReceivePortIdentifier port;

    final boolean local;

    private boolean enabled = false;

    private boolean dead = false;

    /** We know that this many jobs have excessive queue times. We have already
     * reduced the allowance for this worker, don't try again for the moment.
     */
    private int knownDelayedJobs = 0;

    /**
     * Returns a string representation of this worker info. (Overrides method in superclass.)
     * @return The worker info.
     */
    @Override
    public String toString()
    {
	return "Worker " + identifier;
    }

    WorkerInfo( ReceivePortIdentifier port, WorkerIdentifier identifier, MasterIdentifier identifierForWorker, boolean local, ArrayList<JobType> supportedTypes )
    {
	this.port = port;
	this.identifier = identifier;
	this.identifierWithWorker = identifierForWorker;
	this.local = local;
	for( JobType t: supportedTypes ) {
	    registerJobType(t);
	}
    }

    /**
     * Given a job identifier, returns the job queue entry with that id, or null.
     * @param id The job identifier to search for.
     * @return The index of the ActiveJob with this id, or -1 if there isn't one.
     */
    private int searchActiveJob( long id )
    {
        // Note that we blindly assume that there is only one entry with
        // the given id. Reasonable because we hand out the ids ourselves,
        // and we never make mistakes...
        for( int ix=0; ix<activeJobs.size(); ix++ ) {
            ActiveJob e = activeJobs.get( ix );
	    if( e.id == id ) {
		return ix;
	    }
	}
	return -1;
    }

    /** The most recently returned job spent most of its time in the queue.
     * If we haven't done so recently, reduce the queue time of this worker
     * by reducing the number of allowed outstanding jobs.
     * @param workerJobInfo Information about the job that was delayed so long.
     */
    private void limitQueueTime( WorkerJobInfo workerJobInfo, long roundTripTime, long workerDwellTime )
    {
	if( knownDelayedJobs>0 ) {
	    // We've recently done a reduction, and there are still
	    // outstanding jobs from before that. It's not
	    // safe to do another reduction.
	    return;
	}
	if( !workerJobInfo.limitAllowance( roundTripTime, workerDwellTime ) ) {
	    // We cannot reduce the allowance.
	    return;
	}
	if( Settings.traceMasterProgress ) {
	    System.out.println( "Reduced allowance of job type " + workerJobInfo + " to reduce queue time" );
	}
	knownDelayedJobs = activeJobs.size();
    }

    private void registerCompletionInfo( CompletionInfo completionInfo )
    {
        if( completionInfo == null ) {
            return;
        }
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( completionInfo.type );

	if( workerJobInfo == null ) {
	    return;
	}
	if( completionInfo.completionInterval != Long.MAX_VALUE ) {
	    workerJobInfo.setCompletionInterval( completionInfo.completionInterval );
	}
    }

    void registerCompletionInfo( CompletionInfo[] completionInfo )
    {
        enabled = true;
	for( CompletionInfo i: completionInfo ) {
	    registerCompletionInfo( i );
	}
    }

    /**
     * Register a job result for an outstanding job.
     * @param result The job result message that tells about this job.
     */
    void registerWorkerStatus( JobCompletedMessage result )
    {
	final long id = result.jobId;    // The identifier of the job, as handed out by us.

	long now = System.nanoTime();
	int ix = searchActiveJob( id );
	if( ix<0 ) {
	    Globals.log.reportInternalError( "Master: ignoring reported result from job with unknown id " + id );
	    return;
	}
	ActiveJob job = activeJobs.remove( ix );
        long queueInterval = result.queueInterval;
	long newRoundTripInterval = (now-job.startTime); // The time interval to send the job, compute, and report the result.

	if( knownDelayedJobs>0 ) {
	    knownDelayedJobs--;
	}
	job.workerJobInfo.registerJobCompleted( newRoundTripInterval );
	registerCompletionInfo( result.completionInfo );
	limitQueueTime( job.workerJobInfo, newRoundTripInterval, queueInterval+result.computeInterval );
	if( Settings.traceMasterProgress ){
	    System.out.println( "Master: retired job " + job + "; roundTripTime=" + Service.formatNanoseconds( newRoundTripInterval ) );
	}
    }

    /**
     * Returns true iff this worker is idle.
     * @return True iff this worker is idle.
     */
    boolean isIdle()
    {
	return enabled && activeJobs.isEmpty();
    }

    /** Register the start of a new job.
     * 
     * @param job The job that was started.
     * @param id The id given to the job.
     */
    void registerJobStart( JobInstance job, long id )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( job.type );
	if( workerJobInfo == null ) {
	    System.err.println( "No worker job info for job type " + job.type );
	    return;
	}
	workerJobInfo.incrementOutstandingJobs();
	ActiveJob j = new ActiveJob( job, id, System.nanoTime(), workerJobInfo );

	activeJobs.add( j );
    }

    /** Given a job id, retract it from the administration.
     * For some reason we could not send this job to the worker.
     * @param id The identifier of the job.
     */
    void retractJob( long id )
    {
        int ix = searchActiveJob( id );
        if( ix<0 ) {
            Globals.log.reportInternalError( "Master: ignoring job retraction for unknown id " + id );
            return;
        }
        ActiveJob job = activeJobs.remove( ix );
	System.out.println( "Master: retracted job " + job );
    }

    /**
     * @param type The job type to register for.
     */
    private void registerJobType( JobType type )
    {
        if( Settings.traceTypeHandling ){
            System.out.println( "worker " + identifier + " (" + port + ") can handle " + type );
        }
	WorkerJobInfo info = new WorkerJobInfo( toString() + " job type " + type, local );
	workerJobInfoTable.put( type, info );
    }

    /** Given a job type, estimate the completion time of this job type,
     * or Long.MAX_VALUE if the job type is not allowed on this worker,
     * or the worker is currently using its entire allowance.
     * @param jobType The job type for which we want to know the round-trip interval.
     * @return The interval, or Long.MAX_VALUE if this type of job is not allowed.
     */
    long estimateTaskCompletion( JobType jobType )
    {
        if( !enabled ) {
            if( Settings.traceTypeHandling ){
                System.out.println( "estimateTaskCompletion(): worker " + identifier + " not yet enabled" );
            }
            return Long.MAX_VALUE;
        }
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
	if( workerJobInfo == null ) {
	    if( Settings.traceTypeHandling ){
	        System.out.println( "estimateTaskCompletion(): worker " + identifier + " does not support type " + jobType );
	    }
	    return Long.MAX_VALUE;
	}
	return workerJobInfo.estimateTaskCompletion();
    }

    long getAverageCompletionTime( JobType jobType )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );

	if( workerJobInfo == null ) {
	    if( Settings.traceTypeHandling ){
		Globals.log.reportProgress( "getAverageCompletionTime(): worker " + identifier + " does not support type " + jobType );
	    }
	    return Long.MAX_VALUE;
	}
        return workerJobInfo.getAverageCompletionTime();
    }

    /**
     * Tries to increment the maximal number of outstanding jobs for this worker.
     * @param jobType The job type for which we want to increase our allowance.
     * @return True iff we could increment the allowance of this type.
     */
    boolean incrementAllowance( JobType jobType )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
	if( workerJobInfo == null ) {
	    return false;
	}
	return workerJobInfo.incrementAllowance();
    }

    /**
     * Returns true iff this worker is dead.
     * @return Is this worker dead?
     */
    boolean isDead()
    {
	return dead;
    }

    /** Mark this worker as dead.
     * 
     */
    void setDead()
    {
	dead = true;
    }

    /**
     * Given a job type, return true iff this worker
     * supports the job type.
     * @param type The job type we're looking for.
     * @return True iff this worker supports the type.
     */
    boolean supportsType( JobType type )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( type );
	final boolean res = workerJobInfo != null;
        if( Settings.traceTypeHandling ){
            System.out.println( "Worker " + identifier + " supports type " + type + "? Answer: " + res );
        }
	return res;
    }

    /**
     * Given a print stream, print some statistics about this worker.
     * @param s The stream to print to.
     */
    void printStatistics( PrintStream s )
    {
	s.println( "Worker " + identifier );
	Enumeration<JobType> keys = workerJobInfoTable.keys();
	while( keys.hasMoreElements() ){
	    JobType jobType = keys.nextElement();
	    WorkerJobInfo info = workerJobInfoTable.get( jobType );
	    if( info.didWork() ) {
	        String stats = info.buildStatisticsString();
	        s.println( "  " + jobType.toString() + ": " + stats );
	    }
	}
    }
}
