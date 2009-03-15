package ibis.maestro;

import java.io.Serializable;

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
     */
    public void split(Object input, ParallelJobHandler handler);

    /**
     * Merges the result of one part of a split job into the final result.
     * The method should be prepared to handle duplicate results.
     * 
     * @param id
     *            The identifier of the result.
     * @param result
     *            The result.
     */
    public void merge(Serializable id, Object result);

    /**
     * Returns the final result of the parallel job. The system will only
     * invoke this method after all jobs submitted in the split method
     * have yielded a result, and the merge() method has been invoked
     * for each of these partial results.
     * 
     * @return The result.
     */
    public Object getResult();
}
