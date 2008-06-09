package ibis.maestro;

/**
 * The interface of a work source.
 *
 * @author Kees van Reeuwijk.
 */
interface JobSource
{

    /** Gets a job from this work source.
     * @return The job.
     */
    RunJob getJob();

    /** Reports the result of executing a job.
     * @param job The executed job.
     * @param result The result of the job execution.
     */
    void reportJobCompletion( RunJob job, Object result );

}
