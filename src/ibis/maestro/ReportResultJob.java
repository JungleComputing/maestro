package ibis.maestro;

/**
 * A special subclass of Job that reports job results to the local maestro.
 * @author Kees van Reeuwijk
 */
public class ReportResultJob implements Job {
    private static final long serialVersionUID = 1900686553738202952L;

    /** FIXME: different types for different result types. */
    static final JobType jobType = new JobType( "ReportResultJob" );
    
    /** The result value we want to report. */
    final JobResultValue result;

    /**
     * The identifier of the entire run.
     */
    public final TaskIdentifier id;

    /** Constructs a new instance of a report result job.
     * 
     * @param id The identifier of the result.
     * @param result The result to report.
     */
    public ReportResultJob( TaskIdentifier id, JobResultValue result )
    {
	this.id = id;
	this.result = result;
    }

    /**
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return jobType;
    }

    /** Runs this job.
     * 
     * @param node The execution context.
     */
    @Override
    public void run( Node node, TaskIdentifier taskId ) {
        node.reportCompletion( taskId, result );
    }
}
