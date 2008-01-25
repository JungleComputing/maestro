package ibis.maestro;

/**
 * The interface of a work source.
 *
 * @author Kees van Reeuwijk.
 */
public interface WorkSource
{

    /** Gets a job from this work source.
     * @return The job.
     */
    RunJobMessage getJob();

    /** Reports the result of executing a job.
     * @param job The executed job
     * @param r The job result
     */
    void reportJobResult( RunJobMessage job, JobReturn r );

}
