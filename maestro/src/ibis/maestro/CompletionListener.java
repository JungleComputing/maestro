package ibis.maestro;

/**
 * The interface of Maestro job completion listeners.
 * 
 * @author Kees van Reeuwijk
 *
 */
public interface CompletionListener {
    /**
     * Registers that a task has completed, and handles the
     * final result of the task.
     * @param node The node we're running on.
     * @param id The identifier of the task that was completed.
     *      This identifier was provided by the user program at the moment
     *      the task instance was submitted.
     * @param result The result of the task.
     */
    void taskCompleted( Node node, Object id, Object result );
}
