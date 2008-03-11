package ibis.maestro;

/**
 * The available methods for a running job.
 * @author Kees van Reeuwijk
 *
 */
public interface JobContext {

    /**
     * Submit a new job from this job.
     * @param submitter The job that submitted this job.
     * @param job The job to submit.
     */
    void submit( Job submitter, Job job );

    /** Report the completion of a job with the given id and result value.
     * 
     * @param id The identifier of the job.
     * @param result The result of the job.
     */
    void reportCompletion( long id, JobProgressValue result );
}
