package ibis.maestro;

import java.io.Serializable;

/**
 * The interface of a task in the Maestro master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface Task extends Serializable {
    /**
     * Runs the task.

     * @param input The input value of this task run.
     * @param node The node this task is running on.
     * @return The result of the task run.
     */
    Object run( Object input, Node node );

    /**
     * Returns true iff this task can run in this context.
     * @return True iff this task can run.
     */
    abstract boolean isSupported();
}
