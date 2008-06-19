package ibis.maestro;

/**
 * The interface of a work source.
 *
 * @author Kees van Reeuwijk.
 */
interface TaskSource
{

    /** Gets a task from this work source.
     * @return The task.
     */
    RunTask getTask();

    /** Reports the result of executing a task.
     * @param task The executed task.
     * @param result The result of the task execution.
     */
    void reportTaskCompletion( RunTask task, Object result );

}
