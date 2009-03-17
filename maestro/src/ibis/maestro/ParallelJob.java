package ibis.maestro;

/**
 * The interface of a parallel job in the Maestro workflow system.
 * 
 * @author Kees van Reeuwijk
 * 
 */
public interface ParallelJob extends Job {
    /**
     * Given an input, submits a number of jobs to the given handler.
     * 
     * @param input
     *            The input value of this map.
     * @param handler
     *            The handler.
     *            @return The Job instance that was created during the split.
     */
    public ParallelJobInstance split(Object input, ParallelJobHandler handler);
}
