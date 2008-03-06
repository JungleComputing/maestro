package ibis.maestro;


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

    JobResultMessage( JobProgressValue r, long jobId )
    {
	super( null );    // Source is meaningless for this operation
        this.result = r;
        this.jobId = jobId;
    }
}
