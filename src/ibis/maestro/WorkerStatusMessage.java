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

    /**
     * Constructs a new status update message for the master of a job.
     * @param src The worker that handled the job (i.e. this worker)
     * @param jobId The identifier of the job, as handed out by the master.
     */
    WorkerStatusMessage( ReceivePortIdentifier src, long jobId )
    {
	super( src );
        this.jobId = jobId;
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
