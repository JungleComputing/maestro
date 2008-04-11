/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;
import ibis.maestro.Master.WorkerIdentifier;
import ibis.maestro.Worker.MasterIdentifier;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

class WorkerInfo {
    /** Our identifier with this worker. */
    final MasterIdentifier identifierWithWorker;

    /** The active jobs of this worker. */
    private final ArrayList<ActiveJob> activeJobs = new ArrayList<ActiveJob>();

    private final Hashtable<JobType,WorkerJobInfo> workerJobInfoTable = new Hashtable<JobType, WorkerJobInfo>();

    /** Our local identifier of this worker. */
    final Master.WorkerIdentifier identifier;

    /** The receive port of the worker. */
    final ReceivePortIdentifier port;

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
	return "Worker";
    }

    WorkerInfo( ReceivePortIdentifier port, WorkerIdentifier identifier, MasterIdentifier identifierForWorker )
    {
	this.port = port;
	this.identifier = identifier;
	this.identifierWithWorker = identifierForWorker;
    }

    /**
     * Given a job identifier, returns the job queue entry with that id, or null.
     * @param id The job identifier to search for.
     * @return The JobQueueEntry of the job with this id, or null if there isn't one.
     */
    private ActiveJob searchQueueEntry( long id )
    {
	// Note that we blindly assume that there is only one entry with
	// the given id. Reasonable because we hand out the ids ourselves,
	// and we never make mistakes...
	for( ActiveJob e: activeJobs ) {
	    if( e.id == id ) {
		return e;
	    }
	}
	return null;
    }

    /** The most recently returned job spent most of its time in the queue.
     * If we haven't done so recently, reduce the queue time of this worker
     * by reducing the number of allowed outstanding jobs.
     * @param workerJobInfo Information about the job that was delayed so long.
     */
    private void reduceLongQueueTime( WorkerJobInfo workerJobInfo )
    {
	if( knownDelayedJobs>0 ) {
	    // We've recently done a reduction, and there are still
	    // outstanding jobs from before that. It's not
	    // safe to do another reduction.
	    return;
	}
	if( !workerJobInfo.decrementAllowance() ) {
	    // We cannot reduce the allowance.
	    return;
	}
	if( Settings.traceMasterProgress ) {
	    System.out.println( "Reduced allowance of job type " + workerJobInfo + " to reduce queue time" );
	}
	knownDelayedJobs = activeJobs.size();
    }

    /**
     * Register a job result for an outstanding job.
     * @param master The master this info belongs to.
     * @param result The job result message that tells about this job.
     */
    void registerWorkerStatus( ReceivePortIdentifier master, WorkerStatusMessage result )
    {
	final long id = result.jobId;    // The identifier of the job, as handed out by us.

	long now = System.nanoTime();
	ActiveJob e = searchQueueEntry( id );
	if( e == null ) {
	    Globals.log.reportInternalError( "Master " + master + ": ignoring reported result from job with unknown id " + id );
	    return;
	}
	activeJobs.remove( e );
	long newRoundTripInterval = now-e.startTime; // The time to send the job, compute, and report the result.
	long queueInterval = result.queueInterval;

	if( knownDelayedJobs>0 ) {
	    knownDelayedJobs--;
	}
	e.workerJobInfo.registerJobCompleted( newRoundTripInterval );
	if( queueInterval>(2*newRoundTripInterval)/3 ) {
	    reduceLongQueueTime( e.workerJobInfo );
	}
	if( Settings.traceMasterProgress ){
	    System.out.println( "Master " + master + ": retired job " + e + "; roundTripTime=" + Service.formatNanoseconds( newRoundTripInterval ) );
	}
    }

    /**
     * Returns true iff this worker is idle.
     * @return True iff this worker is idle.
     */
    public boolean isIdle()
    {
	return activeJobs.isEmpty();
    }

    /** Register the start of a new job.
     * 
     * @param job The job that was started.
     * @param id The id given to the job.
     */
    public void registerJobStart( Job job, long id )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( job.getType() );
	if( workerJobInfo == null ) {
	    System.err.println( "No worker job info for job type " + job.getType() );
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
     */
    private void registerJobType( JobType type )
    {
	// We start with a very optimistic roundtrip time.
	// Since we only commit one job to the new worker, that's fairly safe.
        if( Settings.traceTypeHandling ){
            System.out.println( "worker " + identifier + " (" + port + ") can handle " + type );
        }
	WorkerJobInfo info = new WorkerJobInfo( 0 );
	workerJobInfoTable.put( type, info );
    }

    /** Given a job type, return the round-trip interval for this job type, or
     * Long.MAX_VALUE if the job type is not allowed on this worker.
     * @param jobType The job type for which we want to know the round-trip interval.
     * @return The interval, or Long.MAX_VALUE if this type of job is not allowed.
     */
    public long getRoundTripInterval( JobType jobType )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
	if( workerJobInfo == null ) {
	    if( Settings.traceTypeHandling ){
	        System.out.println( "Worker " + identifier + " does not support type " + jobType );
	    }
	    return Long.MAX_VALUE;
	}
	return workerJobInfo.getRoundTripInterval();
    }

    /**
     * Tries to increment the maximal number of outstanding jobs for this worker.
     * @param jobType The job type for which we want to increase our allowance.
     * @return True iff we could increment the allowance of this type.
     */
    public boolean incrementAllowance( JobType jobType )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
	if( workerJobInfo == null ) {
	    return false;
	}
	return workerJobInfo.incrementAllowance();
    }

    /** Registers that the worker can support the given type.
     * @param allowedType An allowed type of this worker.
     */
    public void registerAllowedType( JobType allowedType )
    {
	WorkerJobInfo workerJobInfo = workerJobInfoTable.get( allowedType );
	if( workerJobInfo == null ) {
	    // This is new information.
	    registerJobType( allowedType );
	}
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
    public void setDead()
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
	    String stats = info.buildStatisticsString();
	    s.println( "  " + jobType.toString() + ": " + stats );
	}
    }
}
