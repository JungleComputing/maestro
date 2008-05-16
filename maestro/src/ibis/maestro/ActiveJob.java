package ibis.maestro;

/**
 * An entry in our job queue.
 * @author Kees van Reeuwijk
 *
 */
class ActiveJob {
    final JobInstance job;
    final long id;
    final WorkerJobInfo workerJobInfo;

    /** The time this job was sent to the worker. */
    final long startTime;

    ActiveJob( JobInstance job, long id, long startTime, WorkerJobInfo workerJobInfo )
    {
        this.job = job;
        this.id = id;
        this.workerJobInfo = workerJobInfo;
        this.startTime = startTime;
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