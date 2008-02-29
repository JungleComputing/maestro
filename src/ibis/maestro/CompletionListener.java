package ibis.maestro;

/**
 * The interface of Maestro job completion listeners.
 * 
 * @author Kees van Reeuwijk
 *
 */
public interface CompletionListener {
    /**
     * Registers that a job is completed.
     * @param node The node we're running on.
     * @param id The id of the job that was completed.
     * @param result The result of the job.
     */
    void jobCompleted( Node node, long id, JobProgressValue result );
}
