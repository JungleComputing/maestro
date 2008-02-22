package ibis.maestro;

/**
 * Information about a particular type of job. Contrary to WorkerJobInfo,
 * this information is independent of the particular worker.
 * @author Kees van Reeuwijk
 *
 */
public class JobInfo {

    /** The type of this job. */
    public final JobType type;

    /**
     * Constructs a new job info class instance for the job with the given type.
     * 
     * @param type The type of job this info class instance describes.
     */
    public JobInfo( JobType type )
    {
	this.type = type;
    }

}
