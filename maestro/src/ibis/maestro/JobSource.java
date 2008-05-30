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
    RunJobMessage getJob();

    /** Reports the result of executing a job.
     * @param job The executed job
     */
    void reportJobCompletion( RunJobMessage job );

}
