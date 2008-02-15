package ibis.maestro;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class ActiveJob {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final Job job;
    final long id;
    final WorkerJobInfo workerJobInfo;
    final JobInfo jobInfo;

    /** The time this job was sent to the worker. */
    final long startTime;

    ActiveJob( Job job, long id, long startTime, WorkerJobInfo workerJobInfo, JobInfo jobInfo )
    {
        this.job = job;
        this.id = id;
        this.startTime = startTime;
        this.workerJobInfo = workerJobInfo;
        this.jobInfo = jobInfo;
    }

    /**
     * Returns a string representation of this job queue entry.
     * @return The string.
     */
    @Override
    public String toString() {
	return "(ActiveJob id=" + id + ", job=" + job + ", start time " + Service.formatNanoseconds( startTime ) + ')';
    }
    
    /**
     * Given the computation time of  a job, return the ideal queue time of the job.
     */
    private static long computeIdealQueueTime( long computeTime )
    {
	return computeTime/2;
    }

    /**
     * Returns the estimated this job will no longer need a work thread.
     * @param previousCompletionTime The completion time of the previous job for this work thread.
     * @param now The current time.
     * @param  
     * @param roundTripTime Average time from send to return of a job.
     * @param computeTime Average computation time of a job.
     * @return The estimated completion time on the worker.
     */
    public long getCompletionTime( long previousCompletionTime, long now )
    {
	long roundTripTime = workerJobInfo.getRoundTripTime();
	long computeTime = workerJobInfo.getComputeTime();
	long emptyArrivalTime = startTime+roundTripTime;
	long busyCompletionTime = previousCompletionTime+computeIdealQueueTime( computeTime )+computeTime;
	long arrivalTime = Math.max( emptyArrivalTime, now );
	long communicationTime = roundTripTime-computeTime;
	long res = Math.max( busyCompletionTime, arrivalTime-communicationTime/2 );
	if( Settings.traceFastestWorker ) {
	    System.out.println( "computeTime=" + Service.formatNanoseconds(computeTime ) + " roundTripTime=" + Service.formatNanoseconds(roundTripTime) + " emptyArrivalTime-now=" + Service.formatNanoseconds( emptyArrivalTime-now ) + " arrivalTime-now=" + Service.formatNanoseconds( arrivalTime-now ) + " busyCompletionTime-now=" + Service.formatNanoseconds( busyCompletionTime-now ) + " res-now=" + Service.formatNanoseconds(res-now) );
	}
	return res;
    }
 
 }