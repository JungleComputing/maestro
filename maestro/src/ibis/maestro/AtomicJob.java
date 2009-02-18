package ibis.maestro;

/**
 * The interface of an atomic (indivisible) job in the Maestro dataflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface AtomicJob extends Job {
    /**
     * The method to actually run the job.
     * 
     * @param input
     *            The input value of the job.
     * @return The result of the job.
     * @throws JobFailedException
     *             Thrown if the node failed to perform this job. The
     *             administration of all nodes will be updated to ensure no
     *             further jobs of this type are sent to the node, although it
     *             may take some time to propagate this information.
     */
    public Object run(Object input) throws JobFailedException;
}
