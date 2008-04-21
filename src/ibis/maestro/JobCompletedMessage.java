package ibis.maestro;


/**
 * A message from the worker to the master, telling the master that the
 * worker has completed the given job.
 * 
 * @author Kees van Reeuwijk
 *
 */
final class JobCompletedMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final long jobId;   // The identifier of the job
    final long queueInterval;
    final long taskCompletionInterval;   // The estimated time it will take to complete this job entirely.

    /**
     * Constructs a job completed message for the master of a job.
     * @param src The worker that handled the job (i.e. this worker)
     * @param jobId The identifier of the job, as handed out by the master.
     * @param queueInterval The time the job spent in the worker queue.
     * @param taskCompletionInterval The average time it takes on this
     *     worker to complete the remaining jobs of this task.
     */
    JobCompletedMessage( Master.WorkerIdentifier src, long jobId, long queueInterval, long taskCompletionInterval )
    {
	super( src );
        this.jobId = jobId;
        this.queueInterval = queueInterval;
        this.taskCompletionInterval = taskCompletionInterval;
    }
    
    /**
     * Returns a string representation of this status message.
     * @return The string representation.
     */
    @Override
    public String toString()
    {
	return "worker status message: jobId=" + jobId + " queueInterval=" + Service.formatNanoseconds(queueInterval) + " taskCompletionInterval=" + Service.formatNanoseconds( taskCompletionInterval );
    }
}
