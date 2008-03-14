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
     * @param id The id of the task that was completed.
     * @param result The result of the task.
     */
    void jobCompleted( Node node, TaskIdentifier id, JobResultValue result );
}
