package ibis.maestro;

/**
 * The interface of an atomic (indivisible) task in the Maestro dataflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface AtomicJob extends Job {
    /**
     * Runs the job.
     * 
     * @param input
     *            The input value of this task run.
     * @return The result of the task run.
     * @throws JobFailedException
     *             Thrown if the node failed to perform this task. The
     *             administration will be updated to ensure no further tasks of
     *             this type are sent to the node
     */
    Object run(Object input) throws JobFailedException;
}
