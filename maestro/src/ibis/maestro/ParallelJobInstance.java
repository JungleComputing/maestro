package ibis.maestro;

import java.io.Serializable;

public interface ParallelJobInstance {

    /**
     * Merges the result of one part of a split job into the final result.
     * The method should be prepared to handle duplicate results.
     * 
     * @param id
     *            The identifier of the result.
     * @param result
     *            The result.
     *            @return
     *            True iff we now have the result.
     */
    public void merge(Serializable id, Object result);

    /**
     * Returns true iff the result is ready.
     * @return True iff the result is ready.
     */
    public boolean resultIsReady();

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
