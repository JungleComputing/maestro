package ibis.maestro;


/**
 * The super-interface of all public task-like interfaces.
 * @author Kees van Reeuwijk
 *
 */
public interface Task
{
    /** Returns the name of this task, suitable for human consumption.
     * @return The name.
     */
    String getName();

    /**
     * Returns true iff this task can run in this context.
     * @return True iff this task can run.
     */
    abstract boolean isSupported();
}
