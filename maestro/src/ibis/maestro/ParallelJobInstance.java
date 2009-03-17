package ibis.maestro;

import java.io.Serializable;

public abstract class ParallelJobInstance {
    private final RunJobMessage message;
    private final double runMoment;

    public ParallelJobInstance(RunJobMessage message, double runMoment) {
        this.message = message;
        this.runMoment = runMoment;
    }

    void handleJobResult(Node node,Object result)
    {
        node.handleJobResult(message, result, runMoment);
    }

    /**
     * Given an input, submits a number of jobs to the given handler.
     * 
     * @param input
     *            The input value of this map.
     * @param handler
     *            The handler.
     * @return The Job instance that was created during the split.
     */
    public abstract void split(Object input, ParallelJobHandler handler);

    /**
     * Merges the result of one part of a split job into the final result. The
     * method should be prepared to handle duplicate results.
     * 
     * @param id
     *            The identifier of the result.
     * @param result
     *            The result.
     * @return True iff we now have the result.
     */
    public abstract void merge(Serializable id, Object result);

    /**
     * Returns true iff the result is ready.
     * 
     * @return True iff the result is ready.
     */
    public abstract boolean resultIsReady();

    /**
     * Returns the final result of the parallel job. The system will only invoke
     * this method after all jobs submitted in the split method have yielded a
     * result, and the merge() method has been invoked for each of these partial
     * results.
     * 
     * @return The result.
     */
    public abstract Object getResult();

}
