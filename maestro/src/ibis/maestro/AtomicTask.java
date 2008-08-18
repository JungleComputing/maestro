package ibis.maestro;

/**
 * The interface of an atomic (indivisible) task in the Maestro dataflow system.
 * @author Kees van Reeuwijk
 *
 */
public interface AtomicTask extends Task
{
    /**
     * Runs the task.

     * @param input The input value of this task run.
     * @param node The node this task is running on.
     * @return The result of the task run.
     * @throws TaskFailedException Thrown if the node failed to perform this task. The administration will be updated to ensure no further tasks of this type are sent to the node
     */
    Object run( Object input, Node node ) throws TaskFailedException;
}
