package ibis.maestro;

/**
 * The interface of an atomic (indivisible) task in the Maestro master/worker system.
 * @author Kees van Reeuwijk
 *
 */
public interface AtomicTask extends Task {
    /**
     * Runs the task.

     * @param input The input value of this task run.
     * @param node The node this task is running on.
     * @return The result of the task run.
     */
    Object run( Object input, Node node );
}
