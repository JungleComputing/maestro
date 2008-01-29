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
    final long jobid;   // The identifier of the job
    final long queueEmptyInterval; // The most recent interval the queue was empty, in ns.
    final long computeInterval;  // The time it took the worker, from queue entry to job completion, in ns.
    final long queueInterval; // The time the job spent in the queue, in ns.

    long getComputeTime() { return computeInterval; }

    JobResultMessage( ReceivePortIdentifier src, JobReturn r, long jobid, long computeTime, long interval, long queueInterval )
    {
	super( src );
        this.result = r;
        this.jobid = jobid;
        this.computeInterval = computeTime;
        this.queueEmptyInterval = interval;
        this.queueInterval = queueInterval;
    }

    /**
     * Returns the event type of this message.
     */
    @Override
    protected TraceEvent.Type getMessageType()
    {
	return TraceEvent.Type.JOB_RESULT;
    }
}
