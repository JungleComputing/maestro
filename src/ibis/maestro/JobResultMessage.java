package ibis.maestro;

import ibis.ipl.ReceivePortIdentifier;

/**
 * A job result as communicated from the worker to the master.
 * 
 * @author Kees van Reeuwijk
 *
 */
class JobResultMessage extends MasterMessage {
    /** */
    private static final long serialVersionUID = 5158947332427678656L;
    final JobProgressValue result;
    final long jobId;   // The identifier of the job

    JobResultMessage( ReceivePortIdentifier src, JobProgressValue r, long jobId )
    {
	super( src );
        this.result = r;
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
