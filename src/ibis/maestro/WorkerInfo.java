/**
 * Information about a worker in the list of a master.
 */
package ibis.maestro;

import ibis.ipl.IbisIdentifier;
import ibis.ipl.ReceivePortIdentifier;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

class WorkerInfo {
    /** The receive port of this worker. */
    final ReceivePortIdentifier port;

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

    WorkerInfo( ReceivePortIdentifier port )
    {
        this.port = port;
    }

    boolean hasId( ReceivePortIdentifier id )
    {
        return port.equals( id );
    }

    ReceivePortIdentifier getPort()
    {
        return port;
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
     * @param result The job result message that tells about this job.
     */
    public void registerJobResult( JobResultMessage result )
    {
        final long id = result.jobId;    // The identifier of the job, as handed out by us.

        ActiveJob e = searchQueueEntry( id );
        if( e == null ) {
            Globals.log.reportInternalError( "ignoring reported result from job with unknown id " + id );
            return;
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
            Globals.log.reportInternalError( "Master " + master + ": ignoring reported result from job with unknown id " + id );
            return;
        }
        long now = System.nanoTime();
        long newRoundTripTime = now-e.startTime; // The time to send the job, compute, and report the result.

        synchronized( activeJobs ){
            activeJobs.remove( e );
        }
        e.workerJobInfo.registerJobCompleted( this, master, newRoundTripTime );
        if( Settings.traceMasterProgress ){
            System.out.println( "Master " + master + ": retired job " + e + "; roundTripTime=" + Service.formatNanoseconds( newRoundTripTime ) );
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
        workerJobInfo.incrementOutstandingJobs();
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
     */
    private void registerJobType( JobType type )
    {
	// We start with a very optimistic roundtrip time.
	// Since we only commit one job to the new worker, that's fairly safe.
	WorkerJobInfo info = new WorkerJobInfo( 0 );
	workerJobInfoTable.put( type, info );
    }

    /** Given a job type, return the round-trip interval for this job type, or
     * -1 if the job type is not allowed on this worker.
     * @param jobType The job type for which we want to know the round-trip interval.
     * @return The interval, or -1 if this type of job is not allowed.
     */
    public long getRoundTripInterval( JobType jobType )
    {
        WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
        if( workerJobInfo == null ) {
            System.err.println( "Cannot get round-trip interval for unregistered job type " + jobType );
            return -1;
        }
        return workerJobInfo.getRoundTripInterval();
    }

    /**
     *  Increments the maximal number of outstanding jobs for this worker.
     *  @param jobType The job type for which we want to increase our allowance.
     */
    public void incrementAllowance( JobType jobType ) {
        WorkerJobInfo workerJobInfo = workerJobInfoTable.get( jobType );
        if( workerJobInfo == null ) {
            System.err.println( "Cannot increment allowance for unregistered job type " + jobType );
            return;
        }
        workerJobInfo.incrementAllowance();
    }

    /** Registers that the worker can support the given types.
     * @param allowedTypes The list of allowed types that are allowed by
     *        this worker.
     */
    public void updateAllowedTypes( ArrayList<JobType> allowedTypes )
    {
        for( JobType t: allowedTypes ){
            WorkerJobInfo workerJobInfo = workerJobInfoTable.get( t );
            if( workerJobInfo == null ) {
                registerJobType( t );
            }
        }
    }

    /** The queue is empty; don't allow too many outstanding jobs.
     */
    public void reduceAllowances() {
	for (Enumeration<WorkerJobInfo> iterator = workerJobInfoTable.elements(); iterator.hasMoreElements();) {
	    WorkerJobInfo wji =  iterator.nextElement();
	    wji.reduceAllowance();
	    
	}
    }

    public void printStats( ReceivePortIdentifier master )
    {
	for (Enumeration<WorkerJobInfo> iterator = workerJobInfoTable.elements(); iterator.hasMoreElements();) {
	    WorkerJobInfo wji =  iterator.nextElement();
	    wji.printStats( master, port );	    
	}
    }
}
