package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A job result as communicated from the worker to the master.
 * 
 * @author Kees van Reeuwijk
 *
 */
class JobResultMessage extends WorkerMessage {
    /** Contractual obligation. */
    private static final long serialVersionUID = 1L;
    final JobReturn result;
    final long jobId;   // The identifier of the job
    final long queueEmptyInterval; // The most recent interval the queue was empty, in ns.
    final long computeInterval;  // The time it took the worker, from queue entry to job completion, in ns.
    final long queueInterval; // The time the job spent in the queue, in ns.
    final long resultMessageSize;

    long getComputeTime() { return computeInterval; }

    JobResultMessage( ReceivePortIdentifier src, JobReturn r, long jobId, long computeTime, long interval, long queueInterval, long resultMessageSize )
    {
	super( src );
        this.result = r;
        this.jobId = jobId;
        this.computeInterval = computeTime;
        this.queueEmptyInterval = interval;
        this.queueInterval = queueInterval;
        this.resultMessageSize = resultMessageSize;
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
