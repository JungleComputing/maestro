package ibis.maestro;


/**
 * A message from the worker to the master, telling the master that the
 * worker has completed the given job.
 * 
 * @author Kees van Reeuwijk
 *
 */
class WorkerStatusMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final long jobId;   // The identifier of the job
    final long queueInterval;

    /**
     * Constructs a new status update message for the master of a job.
     * @param src The worker that handled the job (i.e. this worker)
     * @param jobId The identifier of the job, as handed out by the master.
     * @param queueInterval The time the job spent in the worker queue.
     */
    WorkerStatusMessage( Master.WorkerIdentifier src, long jobId, long queueInterval )
    {
	super( src );
        this.jobId = jobId;
        this.queueInterval = queueInterval;
    }
}
