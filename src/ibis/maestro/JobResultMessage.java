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
    private final JobReturn result;
    long jobid;   // The identifier of the job
    private long computeTime;  // The time it took the worker, from queue entry to job completion.

    JobReturn getResult() { return result; }
    long getId() { return id; }
    long getComputeTime() { return computeTime; }

    JobResultMessage( ReceivePortIdentifier src, JobReturn r, long jobid, long computeTime ){
	super( src );
        this.result = r;
        this.jobid = jobid;
        this.computeTime = computeTime;
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
