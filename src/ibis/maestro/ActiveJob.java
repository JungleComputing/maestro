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
  
 }