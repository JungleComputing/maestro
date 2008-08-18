package ibis.maestro;


/**
 * The super-interface of all variations of tasks.
 * @author Kees van Reeuwijk
 *
 */
public interface Task
{
    /** Returns the name of this task, suitable for human consumption.
     * @return The name of this task.
     */
    String getName();

    /**
     * Returns true iff this task can run in this context.
     * @return True iff this task can run.
     */
    abstract boolean isSupported();
}
