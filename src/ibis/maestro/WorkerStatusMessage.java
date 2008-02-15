package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A job result as communicated from the worker to the master.
 * 
 * @author Kees van Reeuwijk
 *
 */
class WorkerStatusMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final long jobId;   // The identifier of the job
    final long queueEmptyInterval; // The most recent interval the queue was empty, in ns.
    final long computeInterval;  // The time it took the worker, from queue entry to job completion, in ns.
    final long queueInterval; // The time the job spent in the queue, in ns.

    long getComputeTime() { return computeInterval; }

    /**
     * Constructs a new status update message for the master of a job.
     * @param src The worker that handled the job (i.e. this worker)
     * @param jobId The identifier of the job, as handed out by the master.
     * @param computeTime The time it took to compute the job.
     * @param interval The time in ns the worker queue was empty before this job entered it.
     * @param queueInterval The time in ns the job stayed in the queue before it was started.
     */
    WorkerStatusMessage( ReceivePortIdentifier src, long jobId, long computeTime, long interval, long queueInterval )
    {
	super( src );
        this.jobId = jobId;
        this.computeInterval = computeTime;
        this.queueEmptyInterval = interval;
        this.queueInterval = queueInterval;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TransmissionEvent.Type getMessageType()
    {
	return TransmissionEvent.Type.JOB_RESULT;
    }
}
