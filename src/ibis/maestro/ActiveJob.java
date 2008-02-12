package ibis.maestro;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class ActiveJob implements Comparable<ActiveJob> {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final Job job;
    final long id;
    final WorkerJobInfo jobInfo;
    
    /** The time this job was sent to the worker. */
    final long startTime;

    ActiveJob( Job job, long id, long startTime, WorkerJobInfo jobInfo )
    {
        this.job = job;
        this.id = id;
        this.startTime = startTime;
        this.jobInfo = jobInfo;
    }

    /**
     * Returns a comparison result for this queue entry compared
     * to the given other entry.
     * @param other The other queue entry to compare to.
     */
    @Override
    public int compareTo(ActiveJob other) {
        int res = this.job.compareTo( other.job );
        if( res == 0 ) {
            if( this.id<other.id ) {
                res = -1;
            }
            else {
                if( this.id>other.id ) {
                    res = 1;
                }
            }
        }
        return res;
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
	long roundTripTime = jobInfo.getRoundTripTime();
	long computeTime = jobInfo.getComputeTime();
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