package ibis.maestro;

/**
 * 
 * @author Kees van Reeuwijk
 *
 * @param <T> The type of result to report.
 */
public abstract class ReportResultJob<T> implements Job {
    
    /** FIXME: different types for different result types. */
    private static final JobType jobType = new JobType( "ReportResultJob" );
    final T result;

    /** Constructs a new instance of a report result job.
     * 
     * @param result The result to report.
     */
    ReportResultJob( T result )
    {
	this.result = result;
    }

    /**
     * @return The type of this job.
     */
    @Override
    public JobType getType() {
	return jobType;
    }
}
